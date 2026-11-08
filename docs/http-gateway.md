# JSON/HTTP Gateway

T4 serves the etcd v3 API in two shapes: gRPC, for `etcdctl`, the Go client and Kubernetes; and JSON over HTTP, for everything that cannot speak gRPC — browsers above all, and anything reaching for `curl`.

The HTTP surface is etcd's own. T4 registers the generated grpc-gateway handlers that ship in `go.etcd.io/etcd/api/v3/etcdserverpb/gw`, so the routes, the JSON shapes and the field names are etcd's by construction. A request crosses the same interceptors a gRPC request does, so authentication and RBAC apply identically.

```bash
t4 run --data-dir /var/lib/t4 --listen 0.0.0.0:3379

# Keys and values are base64, as on etcd. "/hello" -> L2hlbGxv, "world" -> d29ybGQ=
curl -X POST http://localhost:3379/v3/kv/put \
  -d '{"key":"L2hlbGxv","value":"d29ybGQ="}'

curl -X POST http://localhost:3379/v3/kv/range \
  -d '{"key":"L2hlbGxv"}'
# {"header":{...},"kvs":[{"key":"L2hlbGxv","create_revision":"2",
#  "mod_revision":"2","version":"1","value":"d29ybGQ="}],"count":"1"}
```

Turn it off with `--enable-grpc-gateway=false`.

## Endpoints

Every etcd v3 route is served under `/v3/`: `kv/range`, `kv/put`, `kv/deleterange`, `kv/txn`, `kv/compaction`, the `lease/`, `auth/`, `cluster/` and `maintenance/` families, and `watch`. The `/v3beta/` prefix etcd used before 3.4 is accepted and rewritten.

Keys, values and range ends are base64-encoded, and 64-bit numbers are JSON strings. Both are protobuf's JSON mapping rather than T4 choices, and both are what etcd does.

## Watch and lease keepalive need a websocket

`Watch` and `LeaseKeepAlive` are bidirectional streams, and plain HTTP has no framing for those. Open them over a websocket on the same path:

```javascript
const ws = new WebSocket("ws://localhost:3379/v3/watch")
ws.onopen = () => ws.send(JSON.stringify({create_request: {key: btoa("/hello")}}))
ws.onmessage = (m) => console.log(JSON.parse(m.data).result)
// {header: {...}, watch_id: "1", created: true}
// {header: {...}, watch_id: "1", events: [{kv: {key: "L2hlbGxv", value: "..."}}]}
```

### grpc-web will not work, on T4 or on etcd

A browser cannot watch an etcd-compatible server over grpc-web, whatever proxy sits in front. grpc-web has no bidirectional streaming: the client half-closes the request after its first message, the server's receive loop ends, and the watch is torn down before any event can reach it. The request returns `200` with `grpc-status: 0` and no events — a watch that looks established and silently delivers nothing.

Unary calls do work behind a grpc-web proxy such as Envoy's `grpc_web` filter. If that is all you need, that route is available and requires nothing from T4. For anything streaming, use the websocket above.

T4 answers a grpc-web content type with `501` and a pointer to this page, rather than letting it look like it succeeded.

## Authentication

Get a token, then send it in the `Authorization` header — the same convention etcd's JSON API uses:

```bash
TOKEN=$(curl -s -X POST http://localhost:3379/v3/auth/authenticate \
  -d '{"name":"root","password":"rootpw"}' | jq -r .token)

curl -X POST http://localhost:3379/v3/kv/range \
  -H "Authorization: $TOKEN" \
  -d '{"key":"L2hlbGxv"}'
```

Without a valid token an auth-enabled T4 refuses the request exactly as it refuses a gRPC one. Client certificates are not an identity on either protocol: T4 authenticates with tokens, and mTLS only decides who may open a connection.

## Browser access

Two settings gate browsers, and both are closed by default.

`--gateway-cors-origins` lists the origins allowed to make cross-origin calls, or `*` for all. It is **empty by default**, which sends no CORS headers and so blocks browsers while leaving `curl`, servers and Kubernetes unaffected. etcd defaults this to `*`; T4 does not, because the gateway is on by default and an open default would let any page a user visits call any T4 their browser can route to.

```bash
t4 run --gateway-cors-origins https://console.example.com
```

A websocket handshake carries no CORS preflight, so this list is applied to the handshake's `Origin` directly: a browser from an origin not on the list is refused with `403`, while clients that send no `Origin` are unaffected.

`--gateway-host-whitelist` lists the `Host` values accepted over plaintext, defaulting to `*`. Narrowing it guards against DNS rebinding ([CVE-2018-5702](https://nvd.nist.gov/vuln/detail/CVE-2018-5702)), where a name under an attacker's control is made to resolve to a T4 the browser can reach but the attacker cannot. Requests over TLS skip the check, since the certificate already settles which server was reached.

## Which port the gateway uses

On a plaintext `--listen`, the gateway shares the port with gRPC, as on etcd. That costs nothing: the first bytes of a connection identify the protocol, so the two are separated before either server sees them and gRPC keeps its own transport.

Under TLS, sharing is expensive. Nothing distinguishes the protocols per connection — gRPC and a browser both negotiate `h2` over ALPN — so the split has to happen per request, by content type, above HTTP. gRPC is then served through Go's HTTP/2 stack instead of its own, and **a request takes roughly twice as long**:

| Layout | gRPC Get, mean |
|---|---|
| `--enable-grpc-gateway=false` | 71 µs |
| gateway on its own port | 71 µs |
| gateway sharing `--listen` | 137 µs |

<sub>M1 Pro, 20000 sequential Gets over TLS on a local node.</sub>

So when client TLS is configured, T4 gives the gateway a port of its own without being asked: the port **below** `--listen`, on the same host. Below rather than above, because the peer port is one above by convention — `3379` and `3380`, as in etcd's `2379` and `2380`.

```bash
t4 run --listen 0.0.0.0:3379 --client-tls-cert server.crt --client-tls-key server.key
# etcd gRPC server listening       listen_addr=0.0.0.0:3379
# etcd JSON/HTTP gateway listening gateway_addr=0.0.0.0:3378 derived=true
```

`--listen` then serves gRPC exactly as it does with no gateway at all, and the gateway answers JSON and websockets on its own address under the same certificate. **Expose that second port** — in a Kubernetes Service, a firewall rule or a load balancer — or JSON clients cannot reach it.

Name the address yourself to put it somewhere else:

```bash
t4 run --gateway-addr 0.0.0.0:2379
```

An address you name is required to work: if T4 cannot listen on it, startup fails. A derived address is not, since nobody asked for it — if the port below `--listen` is taken, T4 logs a warning, falls back to sharing `--listen`, and keeps serving. An upgrade therefore cannot fail to boot over a port collision, but it can quietly land on the slower arrangement, so the warning is worth watching for.

## Configuration

| Flag | Default | Environment variable |
|---|---|---|
| `--enable-grpc-gateway` | `true` | `T4_ENABLE_GRPC_GATEWAY` |
| `--gateway-addr` | shares `--listen` on plaintext; the port below it under TLS | `T4_GATEWAY_ADDR` |
| `--gateway-cors-origins` | *(none)* | `T4_GATEWAY_CORS_ORIGINS` |
| `--gateway-host-whitelist` | `*` | `T4_GATEWAY_HOST_WHITELIST` |
