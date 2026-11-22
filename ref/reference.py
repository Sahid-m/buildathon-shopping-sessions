#!/usr/bin/env python3
"""Straightforward pure-Python reference implementation of rules.md.

Slow but simple; used for differential testing of the Rust pipeline.
Usage: reference.py [DATA_DIR OUTPUT_DIR]  (or env DATA_DIR / OUTPUT_DIR)
"""
import bisect
import datetime
import glob
import json
import os
import sys
from collections import defaultdict

import pyarrow as pa
import pyarrow.parquet as pq

DAY = 86_400_000
WEEK = 7 * DAY
GAP = 1_800_000
DIGITS = b"0123456789"


def read_lines(path):
    with open(path, "rb") as f:
        data = f.read()
    lines = data.split(b"\n")
    if lines and lines[-1] == b"":
        lines.pop()
    return lines[1:]  # skip header


def parse_line(line, n):
    """Return list of values (None for null) or None if malformed."""
    if line == b"":
        return None
    parts = line.split(b",")
    if len(parts) != n:
        return None
    out = []
    for p in parts:
        if b'"' in p:
            if len(p) < 2 or p[0:1] != b'"' or p[-1:] != b'"' or b'"' in p[1:-1]:
                return None
            p = p[1:-1]
        out.append(p if p != b"" else None)
    return out


def rows(folder, n):
    for path in sorted(glob.glob(os.path.join(folder, "*.csv"))):
        for line in read_lines(path):
            r = parse_line(line, n)
            if r is not None:
                yield r


def alldigits(b):
    return len(b) > 0 and all(c in DIGITS for c in b)


def is_leap(y):
    return y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)


MDAYS = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]


def days_from_civil(y, m, d):
    return (datetime.date(y, m, d) - datetime.date(1970, 1, 1)).days


def parse_ts(b):
    """Return epoch ms (int) or None."""
    if b is None:
        return None
    if alldigits(b):
        return int(b)
    if len(b) not in (20, 25):
        return None
    dt = b[:19]
    # YYYY-MM-DDTHH:MM:SS
    if not (dt[4:5] == b"-" and dt[7:8] == b"-" and dt[10:11] == b"T" and dt[13:14] == b":" and dt[16:17] == b":"):
        return None
    for s in (dt[0:4], dt[5:7], dt[8:10], dt[11:13], dt[14:16], dt[17:19]):
        if not alldigits(s):
            return None
    y, mo, d, h, mi, s = int(dt[0:4]), int(dt[5:7]), int(dt[8:10]), int(dt[11:13]), int(dt[14:16]), int(dt[17:19])
    if not (1 <= mo <= 12):
        return None
    md = MDAYS[mo - 1] + (1 if mo == 2 and is_leap(y) else 0)
    if not (1 <= d <= md) or h > 23 or mi > 59 or s > 59:
        return None
    if y < 1:
        return None
    z = b[19:]
    if z == b"Z":
        off = 0
    elif len(z) == 6 and z[0:1] in (b"+", b"-") and z[3:4] == b":" and alldigits(z[1:3]) and alldigits(z[4:6]):
        oh, om = int(z[1:3]), int(z[4:6])
        if oh > 23 or om > 59:
            return None
        off = (oh * 60 + om) * 60_000
        if z[0:1] == b"-":
            off = -off
    else:
        return None
    ms = (days_from_civil(y, mo, d) * 86400 + h * 3600 + mi * 60 + s) * 1000
    return ms - off


def main():
    data = sys.argv[1] if len(sys.argv) > 2 else os.environ.get("DATA_DIR", "/data")
    out = sys.argv[2] if len(sys.argv) > 2 else os.environ.get("OUTPUT_DIR", "/output")
    with open(os.path.join(data, "manifest.json")) as f:
        m = json.load(f)
    ws = parse_ts(m["window_start"].encode())
    we = parse_ts(m["window_end"].encode())

    products = {}
    for line in read_lines(os.path.join(data, "products.csv")):
        r = parse_line(line, 2)
        if r is not None and r[0] is not None:
            products[r[0]] = r[1]
    campaigns = {}
    for line in read_lines(os.path.join(data, "campaigns.csv")):
        r = parse_line(line, 2)
        if r is not None and r[0] is not None:
            campaigns[r[0]] = r[1]

    # links: union-find
    parent = {}

    def find(x):
        root = x
        while parent.get(root, root) != root:
            root = parent[root]
        while parent.get(x, x) != root:
            nxt = parent[x]
            parent[x] = root
            x = nxt
        return root

    for a, b in rows(os.path.join(data, "links"), 2):
        if a is None or b is None:
            continue
        ra, rb = find(a), find(b)
        if ra != rb:
            if ra < rb:
                parent[rb] = ra
            else:
                parent[ra] = rb
    # with min-root union the root is the smallest id in the set

    def person(u):
        return find(u)

    # events
    best = {}
    for r in rows(os.path.join(data, "events"), 7):
        eid = r[0]
        if eid is None:
            continue
        ing = parse_ts(r[3])
        if ing is None:
            ing = float("inf")  # should not happen
        cur = best.get(eid)
        if cur is None or ing < cur[0]:
            best[eid] = (ing, r)
    per_person = defaultdict(list)
    for eid, (ing, r) in best.items():
        _, uid, ts_s, _, etype, pid, price_s = r
        if uid is None or pid is None:
            continue
        ts = parse_ts(ts_s)
        if ts is None or not (ws <= ts < we):
            continue
        if etype not in (b"view", b"cart", b"purchase"):
            continue
        price = 0
        if etype == b"purchase":
            if price_s is None or not alldigits(price_s):
                continue
            price = int(price_s)
            if not (1 <= price <= 10_000_000):
                continue
        if ing - ts > DAY:
            continue
        cat = products.get(pid, b"unknown") if etype == b"purchase" else None
        per_person[person(uid)].append((ts, etype == b"purchase", price, cat))

    # clicks
    seen = set()
    clicks_pp = defaultdict(list)
    for cid, uid, camp, ts_s in rows(os.path.join(data, "clicks"), 4):
        if cid is None or cid in seen:
            continue
        seen.add(cid)
        if uid is None:
            continue
        ts = parse_ts(ts_s)
        if ts is None or not (ws - WEEK <= ts < we):
            continue
        if camp not in campaigns:
            continue
        clicks_pp[person(uid)].append((ts, cid, camp))
    for v in clicks_pp.values():
        v.sort()

    ud = {}
    cd = defaultdict(lambda: [0, 0, 0, 0])
    for p, evs in per_person.items():
        evs.sort(key=lambda e: e[0])
        sessions = []
        prev = None
        for e in evs:
            if prev is None or e[0] - prev > GAP:
                sessions.append([e[0], []])
            sessions[-1][1].append(e)
            prev = e[0]
        cl = clicks_pp.get(p, [])
        cts = [c[0] for c in cl]
        for start, sev in sessions:
            day = start // DAY
            key = (p, day)
            rec = ud.setdefault(key, [0, 0, 0, defaultdict(int)])
            rec[0] += 1
            rec[1] += len(sev)
            rev = 0
            purchased = False
            for ts, isp, price, cat in sev:
                if isp:
                    purchased = True
                    rev += price
                    rec[3][cat] += price
            rec[2] += rev
            lo = bisect.bisect_left(cts, start - WEEK)
            hi = bisect.bisect_right(cts, start)
            q = cl[lo:hi]
            if q:
                maxts = q[-1][0]
                chosen = min(c[1] for c in q if c[0] == maxts)
                camp = next(c[2] for c in q if c[0] == maxts and c[1] == chosen)
                for c in set(c[2] for c in q):
                    cd[(c, day)][3] += 1
            else:
                camp = b"organic"
            r = cd[(camp, day)]
            r[0] += 1
            r[1] += 1 if purchased else 0
            r[2] += rev

    def date(d):
        return datetime.date(1970, 1, 1) + datetime.timedelta(days=d)

    u_rows = {"user_id": [], "day": [], "sessions": [], "events": [], "revenue_cents": [], "top_category": []}
    for (p, day), (s, e, rv, cats) in ud.items():
        u_rows["user_id"].append(p.decode("utf-8", "surrogateescape"))
        u_rows["day"].append(date(day))
        u_rows["sessions"].append(s)
        u_rows["events"].append(e)
        u_rows["revenue_cents"].append(rv)
        if cats:
            top = min(cats.items(), key=lambda kv: (-kv[1], kv[0]))[0]
            u_rows["top_category"].append(top.decode())
        else:
            u_rows["top_category"].append(None)
    c_rows = {"campaign_id": [], "channel": [], "day": [], "sessions": [], "purchasing_sessions": [],
              "revenue_cents": [], "assisted_sessions": []}
    for (c, day), (s, ps, rv, a) in cd.items():
        c_rows["campaign_id"].append(c.decode())
        c_rows["channel"].append(b"organic".decode() if c == b"organic" else campaigns[c].decode())
        c_rows["day"].append(date(day))
        c_rows["sessions"].append(s)
        c_rows["purchasing_sessions"].append(ps)
        c_rows["revenue_cents"].append(rv)
        c_rows["assisted_sessions"].append(a)
    us = pa.schema([("user_id", pa.string()), ("day", pa.date32()), ("sessions", pa.int64()), ("events", pa.int64()),
                    ("revenue_cents", pa.int64()), ("top_category", pa.string())])
    cs = pa.schema([("campaign_id", pa.string()), ("channel", pa.string()), ("day", pa.date32()),
                    ("sessions", pa.int64()), ("purchasing_sessions", pa.int64()), ("revenue_cents", pa.int64()),
                    ("assisted_sessions", pa.int64())])
    os.makedirs(os.path.join(out, "user_daily"), exist_ok=True)
    os.makedirs(os.path.join(out, "campaign_daily"), exist_ok=True)
    pq.write_table(pa.table(u_rows, schema=us), os.path.join(out, "user_daily", "part-0.parquet"))
    pq.write_table(pa.table(c_rows, schema=cs), os.path.join(out, "campaign_daily", "part-0.parquet"))


if __name__ == "__main__":
    main()
