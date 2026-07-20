Fixed the `tls.server_name` option so that it is used for certificate hostname verification in addition to SNI. Previously, on the OpenSSL path (used by HTTP-based sinks such as `datadog_logs`), the certificate was still verified against the connection URL host, causing a "hostname mismatch" verification failure when `server_name` differed from the endpoint host. The override is also scoped to the upstream destination only: it is no longer applied to the TLS connection to an HTTPS forward proxy, so proxy certificates continue to be verified against the proxy host.

authors: gwenaskell
