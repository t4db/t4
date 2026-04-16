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
  the container's environment variables)."
  []
  (c/su
    (c/exec :mkdir :-p "/root/.aws")
    (c/exec :bash :-c "printf '[default]\\naws_access_key_id = jepsen\\naws_secret_access_key = jepsen123\\n' > /root/.aws/credentials")
    (c/exec :bash :-c "printf '[default]\\nregion = us-east-1\\n' > /root/.aws/config")))

;; ── Start / stop ──────────────────────────────────────────────────────────────

(defn start!
  [test node]
  (c/su
    (c/exec :mkdir :-p data-dir)
    (cu/start-daemon!
      {:logfile log-file
       :pidfile pid-file
       :chdir   "/tmp"}
      binary
      :run
      :--listen              "0.0.0.0:3379"
      :--data-dir            data-dir
      :--node-id             (node-id node)
      :--s3-endpoint         "http://minio:9000"
      :--s3-bucket           "jepsen"
      :--metrics-addr        (str "0.0.0.0:" metrics-port)
      :--s3-profile          "default"  ; using profile makes t4 read ~/.aws/credentials file
      ;; Enable multi-node mode: nodes elect a leader via S3 and replicate
      ;; WAL entries over a peer gRPC stream.  Without these flags every node
      ;; runs in single-node (roleSingle) mode with no replication, making
      ;; cross-node operations inconsistent from Knossos's perspective.
      :--peer-listen         (str "0.0.0.0:" peer-port)
      :--advertise-peer      (str (name node) ":" peer-port)
      ;; Shorten the leader-watch interval so a zombie/split-brain leader is
      ;; detected within ~10 s instead of the default 5 minutes.
      :--leader-watch-interval-sec 10
      :--log-level           "warn")))

(defn stop!
  [test node]
  (c/su (cu/stop-daemon! binary pid-file)))

(defn wipe-s3!
  "Removes all objects in the shared S3 bucket. Called once (from the first
  node only) to avoid concurrent delete races."
  []
  (c/su
    (c/exec :aws :--endpoint-url "http://minio:9000"
            :s3 :rm (str "s3://jepsen/") :--recursive
            (c/lit "; true"))))

(defn wipe!
  [test node]
  (c/su
    (c/exec :rm :-rf data-dir)
    (c/exec :mkdir :-p data-dir))
  ;; Clear shared S3 state once, from the first node only.
  (when (= node (first (sort (:nodes test))))
    (install-aws-credentials!)
    (wipe-s3!)))

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
