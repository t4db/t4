(defproject jepsen.strata "0.1.0-SNAPSHOT"
  :description "Jepsen linearizability tests for Strata"
  :license     {:name "Apache-2.0"}
  :main        jepsen.strata.core
  :source-paths ["src"]

  :dependencies
  [[org.clojure/clojure  "1.11.3"]
   [jepsen               "0.3.5"]
   ;; jetcd: official Java etcd v3 client used to drive strata's etcd API.
   [io.etcd/jetcd-core   "0.7.7"]]

  :jvm-opts
  ["-Djava.awt.headless=true"
   ;; Knossos WGL checker is memory-hungry for large histories; 4 GB keeps it
   ;; from OOM-ing on ~400-op runs that include nemesis operations.
   "-Xmx2g"
   ;; jetcd uses Netty which reflectively accesses JDK internals on Java 17+.
   "--add-opens=java.base/java.lang=ALL-UNNAMED"
   "--add-opens=java.base/java.nio=ALL-UNNAMED"
   "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"]

  :profiles
  {:dev {:dependencies [[org.clojure/tools.namespace "1.5.0"]]}})
