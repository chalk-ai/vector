The HTTP-based sources (e.g. `http_server`, `opentelemetry`) now pre-size the request-body buffer from the `Content-Length` header, collecting each body in a single allocation instead of growing an unsized buffer. This removes a per-request reallocation/copy cost on the ingestion path and restores throughput on high-volume HTTP sources, while keeping the streaming decompressed-size cap.

authors: armleth
