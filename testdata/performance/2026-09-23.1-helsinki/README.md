# Live Helsinki tile baseline

Measured on 2026-10-08 with the pinned Linux arm64 service, DuckDB 1.5.5,
bundled checksum-verified extensions, read-only root, and a writable 2 GiB
cache tmpfs. The service used one worker, two DuckDB threads, 512 MiB worker
memory, a 300-second tile deadline, 1 GiB cache capacity, and 512 MiB scratch
capacity. The client deadline was 360 seconds.

Cell: `8a1126d33027fff`, resolution 10. Release: `2026-09-23.1`.
Catalog version:
`2026-09-23.1+sha256:b930a692b2bb6f9cd1b3d197dc1c0b581a1dc84edf301c7a4870cc77ddaa9239`.
Projection:
`sha256:8d45cd52fbfd6ea2ad9248f2fd99b601111241c4b80d77c6d1af0b9093918d57`.

Catalog-bbox selection reduced 16 assets to partition `00011`, with a full
object size of 632751442 bytes and 5088275 rows in 256 row groups.
The object filename is recorded in [baseline-profile.txt](baseline-profile.txt).

| Measurement | Initial cold request | Profiled cold request |
| --- | ---: | ---: |
| Client elapsed | 244723 ms | 244880 ms |
| Worker elapsed | 244420 ms | 244586 ms |
| Candidate query/materialization | 244299 ms | 244482 ms |
| COPY | 2 ms | 3 ms |
| Output validation | 7 ms | 2 ms |
| HTTP HEADs | Not profiled | 1 |
| HTTP GETs | Not profiled | 1030 |
| HTTP bytes received | Not profiled | 70.6 MiB |
| Validated output | 58 rows, 5859 bytes | 58 rows, 5859 bytes |

HTTP transfer is the rounded value reported by HTTPFS, not the full asset size
or an exact byte count. The executed scan projects only `geometry`, `id`,
`names`, and `basic_category`; `bbox` is read for filtering. COPY reads the
local candidate table. Both cold requests succeeded with HTTP 200. Their
SHA-256 digest is
`cb765e5b572d196352298e74e0c778032a1f34212ad338485150c2a7c00c4144`.

Repeating the initial request returned the identical file in 303 ms and did
not launch another worker. PyArrow independently read the GeoParquet 1.1
metadata and selected schema. Shapely decoded every WKB point, and Python H3
confirmed all 58 points belong to the requested cell; IDs are unique and
recorded in [ids.json](ids.json). These checks establish output validity;
they do not independently establish completeness against an unpruned source
scan.

An independent footer read found that simple bbox min/max overlap selects
only row group 242 (22944 rows). Its compressed chunks for the selected
columns and bbox total 1454266 bytes. All bbox chunks across the file total
71337796 bytes. The observed transfer and 1030 GETs suggest the conservative
cross-field bbox checks scan bbox chunks across the file; this is an inference,
not an instrumented count of skipped/read row groups. Successful completion
within 300 seconds does not establish acceptable first-request latency.

Repeat with a fresh cache and the pinned service image:

```sh
podman run --rm --name overture-live-helsinki --read-only --cap-drop=ALL \
  --security-opt=no-new-privileges \
  --tmpfs /var/cache/overture:rw,size=2g,mode=0777 -p 18080:8080 \
  -e OVERTURE_TILE_TIMEOUT=300s -e OVERTURE_WORKER_COUNT=1 \
  -e OVERTURE_WORKER_THREADS=2 -e OVERTURE_CACHE_MAX_BYTES=1073741824 \
  -e OVERTURE_SCRATCH_MAX_BYTES=536870912 overturetunkki/service:dev -v
```

In another terminal:

```sh
bin/overture-client --server-url=http://localhost:18080 --timeout=360s \
  tile 8a1126d33027fff --output=bin/helsinki-profiled.parquet
bin/overture-client --server-url=http://localhost:18080 --timeout=360s \
  tile 8a1126d33027fff --output=bin/helsinki-cache-hit.parquet
podman logs overture-live-helsinki
```

The CLI neither retries nor changes the requested release/cell automatically.
Future catalog releases change the version and invalidate this comparison.

## Intermediate branch measurement

The first disjoint-branch implementation, with negated validity checks for
fallback branches, completed in 223851 ms at the client. Materialization took
223467 ms, COPY 1 ms, and validation 1 ms. Its
[executed profile](branched-profile.txt) reports one HEAD, 775 GETs, and 95.1 MiB
received. The ordinary branch took 2.94 seconds and the wrapped branch 4.42
seconds, but invalid-latitude and invalid-longitude fallback scans still took
125.73 and 88.14 seconds. Their combined scan times do not equal wall time.

The output has the same 58 IDs, byte count, and SHA-256 digest as the baseline;
an independent reader confirmed H3 membership again. A repeated cache hit
took 280 ms and returned identical bytes.

## Final branch measurement

The final implementation uses explicit invalid-bound checks instead of negated
validity checks. Its [executed profile](optimized-profile.txt) records:

| Measurement | Final cold request |
| --- | ---: |
| Client elapsed | 185025 ms |
| Worker elapsed | 184743 ms |
| Candidate query/materialization | 184631 ms |
| COPY | 1 ms |
| Output validation | 1 ms |
| HTTP HEADs | 1 |
| HTTP GETs | 775 |
| HTTP bytes received | 95.1 MiB |
| Validated output | 58 rows, 5859 bytes |
| Repeated cache hit | 273 ms |

PyArrow/Shapely/Python H3 independently verified the selected schema, unique
IDs, and membership of every point. The final IDs, bytes, and SHA-256 digest
match the baseline; the cache hit is byte-for-byte identical and launches no
worker. Bbox branch equivalence tests compare against unpruned H3 membership
on Parquet fixtures, including boundaries, poles, pentagons, the antimeridian,
null fields, reversed latitude bounds, out-of-range values, NaNs, and infinities.
The generated union's row-limit test spans ordinary and both invalid-bbox
branches, checking zero capacity, rejection at the allowance plus one, and
complete admission without duplicate rows.

The ordinary and wrapped scan operators took 3.00 and 4.58 seconds; invalid
latitude and longitude scans still took 115.84 and 59.07 seconds. Ordinary
overlap filters can use row-group bounds, while fallback checks must retain
malformed rows and still read widely. The observed client latency is about
24% lower and GET count about 25% lower than the baseline, but transferred
bytes increase from 70.6 to 95.1 MiB. These single-run measurements depend on
network conditions and do not establish a latency guarantee or constant-time
rejection. The service satisfies the chosen-fields contract; expensive fallback
reads are still a cold-tile limitation.
