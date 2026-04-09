# Example: S3/R2 Pass-Through Proxy

Use objstrd as an S3-compatible proxy in front of AWS S3 or Cloudflare R2.
The tree config puts a single remote S3 shard behind objstrd, so clients
talk to your local endpoint and traffic is forwarded to the cloud provider.

## AWS S3

**proxy.conf:**

```
cluster  s3-proxy
bucket   my-app-bucket

proxy  rf=1  listen=0.0.0.0:8000  endpoint=http://localhost:8000
  s3  endpoint=https://s3.us-east-1.amazonaws.com  bucket=my-app-bucket  region=us-east-1  access_key=AKIA...  secret_key=wJal...
```

## Cloudflare R2

```
cluster  r2-proxy
bucket   my-r2-bucket

proxy  rf=1  listen=0.0.0.0:8000  endpoint=http://localhost:8000
  s3  endpoint=https://ACCOUNT_ID.r2.cloudflarestorage.com  bucket=my-r2-bucket  access_key=...  secret_key=...  path_style
```

## Start It

```bash
objstrd --config proxy.conf --node proxy
```

Clients use `http://localhost:8000` as their S3 endpoint. objstrd handles
SigV4 auth locally (if configured with `--access-key`/`--secret-key`) and
forwards operations to the upstream provider.

## Use Cases

- Adding local auth in front of a public bucket
- Providing a stable local endpoint while switching providers
- Development/testing against a real backend without vendor SDK lock-in
- **Audit logging gateway (planned):** route all S3 traffic through objstrd
  to get a single audit log of every GET/PUT/DELETE across multiple upstream
  accounts. Would log operation, key, client IP, request headers, status
  code, and latency. Useful for compliance, debugging, and cost attribution
  when multiple teams or services share S3 buckets.
