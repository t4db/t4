# Kubernetes apiserver storage tests

This harness runs the upstream `k8s.io/apiserver/pkg/storage/etcd3` tests,
and the watch cache tests in `k8s.io/apiserver/pkg/storage/cacher`,
against T4's etcd-compatible gRPC server. It builds the upstream tests in a
temporary work directory, rewrites only the embedded etcd test harness import to
`github.com/t4db/t4-apiserver-testserver`, and runs the compiled test binaries
against a real `t4 run` process.

Run it explicitly:

```sh
make test-apiserver
```

The runner uses the Kubernetes apiserver version selected by `T4_APISERVER_VERSION`.
When unset, it defaults to `v0.35.4`.
