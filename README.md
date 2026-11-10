# mcpg-plugin-http-ingress

Serve HTTP on the connections that an `http`-wire MCPG transport plugin
accepts, and hand each request to the gateway.

An `http`-wire transport (a tunnel, a relay, a socket the gateway does not
bind) owns its connections. For each one it calls `serve_connection` with the
byte stream and a `Connection`:

- `remote_addr`: the client's address; the gateway's per-IP rate limits key
  on it.
- `peer`: the caller identity the transport established, for example a
  tailnet peer's owner. It rides on every request of the connection.
- `refusal`: when set, every request gets this local answer and never
  reaches the gateway.
- `max_body`: the largest request body read; a larger one gets HTTP 413.

The crate reads HTTP/1.1 and HTTP/2, reads each request body whole, hands the
request to the gateway through the SDK's `HttpIngressServer`, and streams the
answer back frame by frame, so SSE works. When the client goes away, the
gateway stops its side of the response.

```rust
use mcpg_plugin_http_ingress::{Connection, serve_connection};

// In the transport's accept loop:
let connection = Connection::from_addr(remote_addr, 4 * 1024 * 1024);
tokio::spawn(serve_connection(stream, server, connection, "my-tunnel"));
```

## License

Apache-2.0 — see [LICENSE](LICENSE).
