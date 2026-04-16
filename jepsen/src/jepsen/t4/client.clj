(ns jepsen.t4.client
  "Jepsen client that drives T4 through its etcd v3 API using jetcd."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen.client         :as client])
  (:import [io.etcd.jetcd
            Client ByteSequence]
           ;; jetcd 0.7.x API notes:
           ;;   Op        – factory for Put/Get/Delete transaction ops
           ;;   Cmp       – comparison; Cmp$Op is the inner enum (EQUAL/GREATER/…)
           ;;   CmpTarget – factory: (CmpTarget/modRevision n), (CmpTarget/value bs), …
           ;; CmpResult does NOT exist; use Cmp$Op instead.
           [io.etcd.jetcd.op Op Cmp Cmp$Op CmpTarget]
           [io.etcd.jetcd.options PutOption]
           [java.nio.charset StandardCharsets]
           [java.util.concurrent TimeUnit TimeoutException]))

;; ── Encoding helpers ──────────────────────────────────────────────────────────

(def ^:private charset StandardCharsets/UTF_8)

(defn ^ByteSequence ->bs [s]
  (ByteSequence/from (.getBytes (str s) charset)))

(defn bs-> [^ByteSequence bs]
  (when bs (.toString bs charset)))

;; ── Connection ────────────────────────────────────────────────────────────────

; Keep this short: timed-out ops become :info entries that Knossos WGL must
; place at every possible position in the history — exponential in concurrency.
; 2 s is plenty for in-container gRPC; reduces max concurrent :info window.
(def ^:private timeout-ms 2000)

(defn connect!
  "Opens a jetcd Client pointing at a single t4 node."
  [node]
  (-> (Client/builder)
      (.endpoints (into-array String [(str "http://" (name node) ":3379")]))
      (.build)))

;; ── Register operations ───────────────────────────────────────────────────────
;;
;; A single key holds a small integer.  Three operations:
;;
;;   :read  → Get(key) → current value (nil if absent)
;;   :write → Put(key, value)
;;   :cas   [old new]  → read ModRevision, then
;;                       Txn(if ModRev==rev AND value==old, Then Put(new))
;;
;; CAS is two RPCs (Get + Txn).  A concurrent write between them causes the
;; Txn to fail — correctly returned as :fail to Jepsen.

(def ^:private register-key "/jepsen/register")

(defn do-read [kv]
  (let [resp (-> (.get kv (->bs register-key))
                 (.get timeout-ms TimeUnit/MILLISECONDS))]
    (when (pos? (.getCount resp))
      (-> resp .getKvs first .getValue bs-> Long/parseLong))))

(defn do-write [kv value]
  (-> (.put kv (->bs register-key) (->bs value))
      (.get timeout-ms TimeUnit/MILLISECONDS))
  :ok)

(defn do-cas
  "Returns true if the swap succeeded."
  [kv old-val new-val]
  (let [get-resp (-> (.get kv (->bs register-key))
                     (.get timeout-ms TimeUnit/MILLISECONDS))
        [cur-val mod-rev]
        (if (zero? (.getCount get-resp))
          [nil 0]
          (let [kv-entry (-> get-resp .getKvs first)]
            [(-> kv-entry .getValue bs-> Long/parseLong)
             (.getModRevision kv-entry)]))]
    (when (= cur-val old-val)
      ;; Guard the put with the ModRevision we just read.
      (let [cmp (Cmp. (->bs register-key)
                      Cmp$Op/EQUAL                   ; ← inner enum, not CmpResult
                      (CmpTarget/modRevision mod-rev)) ; ← static factory
            txn (-> (.txn kv)
                    (.If    (into-array Cmp [cmp]))
                    (.Then  (into-array Op  [(Op/put (->bs register-key)
                                                     (->bs new-val)
                                                     (PutOption/DEFAULT))]))
                    (.Else  (into-array Op  []))
                    (.commit)
                    (.get timeout-ms TimeUnit/MILLISECONDS))]
        (.isSucceeded txn)))))

;; ── Client record ─────────────────────────────────────────────────────────────

(defrecord RegisterClient [^Client conn kv]
  client/Client

  (open! [this test node]
    (let [c (connect! node)]
      (assoc this :conn c :kv (.getKVClient c))))

  (setup! [this test])

  (invoke! [this test op]
    (try
      (case (:f op)
        :read  (assoc op :type :ok :value (do-read kv))
        :write (do (do-write kv (:value op))
                   (assoc op :type :ok))
        :cas   (let [[old new] (:value op)
                     swapped?  (do-cas kv old new)]
                 (assoc op :type (if swapped? :ok :fail))))

      (catch TimeoutException _
        (assoc op :type :info :error :timeout))

      (catch io.grpc.StatusRuntimeException e
        ;; UNAVAILABLE = node is partitioned; :fail lets Jepsen account for it.
        (assoc op :type :fail :error (str (.getStatus e))))

      (catch Exception e
        (assoc op :type :fail :error (.getMessage e)))))

  (teardown! [this test])

  (close! [this test]
    (when kv   (.close kv))
    (when conn (.close conn))))

(defn register-client []
  (map->RegisterClient {}))
