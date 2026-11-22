# Shopping Sessions: a native Rust pipeline

Entry for the Shopping Sessions track (by Databricks). It reads the messy click-stream CSVs in
`$DATA_DIR`, applies `rules.md`, and writes `$OUTPUT_DIR/user_daily/` and
`$OUTPUT_DIR/campaign_daily/` as Parquet.

## Approach

One static Rust binary, no runtime engine. All work is multi-threaded over `NCPU` threads.

1. **Small files.** `products.csv` and `campaigns.csv` go into hash maps. Category names are
   sorted byte-wise (with `unknown` added), so a category ID order is the tie-break order.
2. **Links.** Link files are memory-mapped and parsed in parallel. A union-find over the IDs
   keeps the byte-wise smallest ID as each root, which is the person's name.
3. **Events, pass A.** Every event file is memory-mapped and cut into chunks of whole lines.
   Each thread parses lines with the exact CSV dialect (quotes only around a whole field, `\r`
   is data, field count must match), parses `ts` (F1/F2/F3, strict calendar and offset
   checks), and computes the validity of the copy (user, window, type, product, price, late).
   Records are radix-partitioned by a key of `event_id`.
4. **Events, pass B.** Per partition, dedup on `event_id` keeps the copy with the smallest
   `ingest_ts`; the event survives only if that copy is valid. Survivors are mapped to their
   person and repartitioned by person.
5. **Clicks.** Parsed and validated in parallel, mapped to their person and partitioned by
   person. Copies are identical, so dedup happens after the sort in pass C.
6. **Pass C.** Per person partition: sort events by (person, ts) and clicks by
   (person, ts, click key); build sessions (gap > 30 min); a sliding 7-day window over the
   person's clicks gives the credited click (latest ts, then smallest `click_id` byte-wise)
   and the set of assisting campaigns (per-campaign counts plus an active list).
7. **Output.** A small hand-written Parquet writer (PLAIN, uncompressed, one row group,
   `day` as `INT32 (DATE)`).

String keys: IDs of up to 15 bytes are packed losslessly into a 128-bit key; longer IDs get
a 120-bit hash in a disjoint key space.

`ref/reference.py` is a plain Python implementation of the rules, used for differential
testing against the Rust binary on generated datasets.

## Run

```sh
docker buildx build --platform linux/amd64 --load -t shopping-sessions:latest .
python3 scripts/run_fixtures.py fixtures -- scripts/docker_run.sh   # 36 fixtures
scripts/docker_run.sh path/to/dev out && python3 scripts/compare.py out dev-answer
```

Without Docker: `cargo build --release && python3 scripts/run_fixtures.py fixtures -- target/release/sessions`.

## Libraries

- [memmap2](https://crates.io/crates/memmap2) (MIT/Apache-2.0) for memory-mapped input.
- Test scripts only: polars, pyarrow (Python).

## License

MIT, see `LICENSE`.
