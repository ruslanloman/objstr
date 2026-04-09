# objstrd Benchmarks

Benchmark suites for the objstrd S3 daemon. Each subfolder targets a
different aspect of performance.

| Subfolder | Description | Details |
|-----------|-------------|---------|
| [catalog/](catalog/) | Catalog rebuild, load, and save times for images with hundreds of thousands of objects | [catalog/README.md](catalog/README.md) |
| [lance/](lance/) | LanceDB dataset operations across different storage backends, measuring S3 HTTP layer overhead | [lance/README.md](lance/README.md) |
| [listing/](listing/) | ListObjectsV2 listing speed comparison across raw, filesystem, and in-memory backends | [listing/README.md](listing/README.md) |
| [rclone/](rclone/) | Throughput benchmarks using `rclone test speed` covering small/large objects, multipart, and concurrency | [rclone/README.md](rclone/README.md) |
