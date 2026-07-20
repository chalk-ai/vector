Fixed `tls.server_name` so it is no longer applied to the TLS connection to an HTTPS forward proxy. Previously the destination server name was used to verify the proxy's own certificate, causing HTTPS proxies with their own certificate to fail with a hostname mismatch. The override now applies only to the upstream destination, and proxy certificates are verified against the proxy host.

authors: gwenaskell
