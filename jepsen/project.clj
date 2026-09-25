(defproject jepsen.t4 "0.1.0-SNAPSHOT"
  :description "Jepsen linearizability tests for T4"
  :license     {:name "Apache-2.0"}
  :main        jepsen.t4.core
  :source-paths ["src"]

  :dependencies
  [[org.clojure/clojure  "1.11.3"]
   [jepsen               "0.3.5"]
   ;; jetcd: official Java etcd v3 client used to drive t4's etcd API.
   [io.etcd/jetcd-core   "0.7.7"]]

  :jvm-opts
  ["-Djava.awt.headless=true"
   ;; Knossos linearizability search on the register workload under
   ;; partition-halves can balloon: concurrent ops in both partition halves
   ;; produce histories whose interleaving space is exponential. 2 GB OOM'd
   ;; in CI; 6 GB leaves room for Docker + S3 server + 5 DB containers on
   ;; ubuntu-latest (16 GB).
   "-Xmx6g"
   ;; jetcd uses Netty which reflectively accesses JDK internals on Java 17+.
   "--add-opens=java.base/java.lang=ALL-UNNAMED"
   "--add-opens=java.base/java.nio=ALL-UNNAMED"
   "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"]

  :profiles
  {:dev {:dependencies [[org.clojure/tools.namespace "1.5.0"]]}})
