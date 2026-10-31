(ns jepsen.t4.db
  "Jepsen DB implementation: installs, starts, stops, and wipes T4 nodes."
  (:require [clojure.string           :as str]
            [clojure.tools.logging    :refer [info warn]]
            [jepsen.db                :as db]
            [jepsen.control           :as c]
            [jepsen.control.util      :as cu]))

;; ── Paths ─────────────────────────────────────────────────────────────────────

(def binary      "/usr/local/bin/t4")
(def data-dir    "/var/lib/t4")
(def log-file    "/var/log/t4.log")
(def pid-file    "/var/run/t4.pid")
(def metrics-port 2380)   ; HTTP /healthz — distinct from the gRPC port (3379)
(def peer-port   3380)    ; leader→follower WAL stream (peer gRPC)

;; ── Object store ──────────────────────────────────────────────────────────────
;;
;; The suite runs against one object store, chosen by T4_JEPSEN_STORE: "s3"
;; (the default) or "gcs".
;;
;; Most of what Jepsen checks here cannot tell the two apart. Consensus,
;; replication and fencing all reach the store through t4's object.Store
;; interface, so a linearizability violation found on one backend would be
;; found on both. What differs is the leader lock: on S3 a conditional PUT
;; guarded by an ETag, on GCS a write guarded by a generation. Whether exactly
;; one writer wins a contested lock is covered directly by a unit test; what is
;; left to a fault-injecting run is how each store's *errors* behave when a node
;; cannot reach it, which is what the partition-store nemesis exercises.

(def store
  "The object store under test: where it lives, for the partition nemesis, and
  the flags that point t4 at it."
  (let [choice (or (System/getenv "T4_JEPSEN_STORE") "s3")]
    (case choice
      "s3"  {:name   "s3"
             :host   "s3"
             :port   9000
             :bucket "jepsen"
             :flags  [:--s3-endpoint "http://s3:9000"
                      :--s3-bucket   "jepsen"
                      ;; A profile makes t4 read ~/.aws/credentials; without one
                      ;; it refuses to consult an ambient credentials chain.
                      :--s3-profile  "default"]}
      "gcs" {:name   "gcs"
             :host   "gcs"
             :port   4443
             :bucket "jepsen"
             ;; No credentials flag: an overridden endpoint is unauthenticated,
             ;; which is what the emulator wants.
             :flags  [:--gcs-endpoint "http://gcs:4443"
                      :--gcs-bucket   "jepsen"]}
      (throw (IllegalArgumentException.
               (str "T4_JEPSEN_STORE must be \"s3\" or \"gcs\", got "
                    (pr-str choice)))))))

;; ── Helpers ───────────────────────────────────────────────────────────────────

(defn node-id
  "Stable node identifier derived from the hostname."
  [node]
  (name node))

(defn endpoint
  "etcd endpoint URL for a given node."
  [node]
  (str "http://" (name node) ":3379"))

(defn await-ready!
  "Polls t4's HTTP /healthz until it returns 200, up to 30 s."
  [node]
  (let [url      (str "http://localhost:" metrics-port "/healthz")
        deadline (+ (System/currentTimeMillis) 30000)]
    (loop []
      (let [ok? (try (c/exec :curl :-sf url) true
                     (catch Exception _ false))]
        (cond
          ok?
          (info node "t4 is ready")

          (< (System/currentTimeMillis) deadline)
          (do (Thread/sleep 500) (recur))

          :else
          (let [log-tail (try (c/exec :tail :-n "50" log-file)
                              (catch Exception _ "(log unavailable)"))]
            (throw (ex-info (str "t4 did not become ready in time on " node
                                 "\n--- t4 log ---\n" log-tail)
                            {:node node :url url}))))))))

;; ── AWS credentials ───────────────────────────────────────────────────────────

(defn install-aws-credentials!
  "Write ~/.aws/credentials and ~/.aws/config so the AWS SDK finds them
  regardless of how the process is launched (SSH sessions don't inherit
  the container's environment variables).

  Only for the S3 store: t4 reads them because --s3-profile selects them, and
  the aws CLI reads them when wiping the bucket. On GCS neither applies, and
  leaving AWS credentials on the node would only mislead whoever reads the
  logs."
  []
  (when (= "s3" (:name store))
    (c/su
      (c/exec :mkdir :-p "/root/.aws")
      (c/exec :bash :-c "printf '[default]\\naws_access_key_id = jepsen\\naws_secret_access_key = jepsen123\\n' > /root/.aws/credentials")
      (c/exec :bash :-c "printf '[default]\\nregion = us-east-1\\n' > /root/.aws/config"))))

;; ── Start / stop ──────────────────────────────────────────────────────────────

(defn start!
  [test node]
  (c/su
    (c/exec :mkdir :-p data-dir)
    (apply cu/start-daemon!
      {:logfile log-file
       :pidfile pid-file
       :chdir   "/tmp"}
      binary
      :run
      :--listen              "0.0.0.0:3379"
      :--data-dir            data-dir
      :--node-id             (node-id node)
      :--metrics-addr        (str "0.0.0.0:" metrics-port)
      ;; Enable multi-node mode: nodes elect a leader through the object store
      ;; and replicate WAL entries over a peer gRPC stream.  Without these
      ;; flags every node runs in single-node (roleSingle) mode with no
      ;; replication, making cross-node operations inconsistent from Knossos's
      ;; perspective.
      :--peer-listen         (str "0.0.0.0:" peer-port)
      :--advertise-peer      (str (name node) ":" peer-port)
      :--log-level           "warn"
      ;; Which object store, and how to reach it. See the store descriptor.
      (:flags store))))

(defn stop!
  [test node]
  (c/su (cu/stop-daemon! binary pid-file)))

(defn wipe-store!
  "Removes every object in the shared bucket. Called once (from the first node
  only) to avoid concurrent delete races."
  []
  (c/su
    (case (:name store)
      "s3"
      (c/exec :aws :--endpoint-url (str "http://" (:host store) ":" (:port store))
              :s3 :rm (str "s3://" (:bucket store) "/") :--recursive
              (c/lit "; true"))

      ;; GCS has no recursive delete, so list and delete each object. jq both
      ;; reads the listing and percent-encodes the name, which has to arrive as
      ;; one path segment: an object called "wal/0001" is addressed as
      ;; wal%2F0001, and a bare slash would delete nothing.
      "gcs"
      (let [base (str "http://" (:host store) ":" (:port store)
                      "/storage/v1/b/" (:bucket store) "/o")]
        (c/exec :bash :-c
                (str "curl -sS '" base "' | jq -r '.items[]?.name' | "
                     "while IFS= read -r n; do "
                     "curl -sS -o /dev/null -X DELETE "
                     "\"" base "/$(jq -rn --arg s \"$n\" '$s|@uri')\"; "
                     "done; true"))))))

(defn wipe!
  [test node]
  (c/su
    (c/exec :rm :-rf data-dir)
    (c/exec :mkdir :-p data-dir))
  ;; Clear the shared object store once, from the first node only.
  (when (= node (first (sort (:nodes test))))
    (install-aws-credentials!)
    (wipe-store!)))

;; ── DB record ─────────────────────────────────────────────────────────────────

(defn db
  "Returns a Jepsen DB that manages T4 on each node."
  []
  (reify
    db/DB
    (setup! [_ test node]
      (info node "starting t4")
      (install-aws-credentials!)
      (start! test node)
      (await-ready! node))

    (teardown! [_ test node]
      (info node "tearing down t4")
      (stop!  test node)
      (wipe!  test node))

    db/Primary
    ;; T4 uses S3-based leader election; any node can accept writes.
    (primaries     [_ test]       (:nodes test))
    (setup-primary! [_ test node] nil)

    db/LogFiles
    (log-files [_ test node] [log-file])))
