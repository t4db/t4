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
           [io.etcd.jetcd.options GetOption PutOption]
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

;; ── Errors ─────────────────────────────────────────────────────────────────────
;;
;; A read that errors changed nothing, so it is a definite :fail. A write or
;; CAS that errors may still have been applied: the leader can commit it and
;; the reply be lost (a partition cuts a forwarded write's response, or the
;; leader loses its lease after committing). It must be :info, or Knossos
;; treats the write as never having happened and flags a later read of its
;; value as a violation.

(defn- error-type [op]
  (if (= :read (:f op)) :fail :info))

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
        (assoc op :type (error-type op) :error (str (.getStatus e))))

      (catch Exception e
        (assoc op :type (error-type op) :error (.getMessage e)))))

  (teardown! [this test])

  (close! [this test]
    (when kv   (.close kv))
    (when conn (.close conn))))

(defn register-client []
  (map->RegisterClient {}))

;; ── Multi-key register operations ─────────────────────────────────────────────
;;
;; Three keys hold small integers. Operations exercise the etcd Txn surface:
;;
;;   :read  → one prefix Get over all three keys → {:a v :b v :c v}
;;   :write {:a v :b v :c v} → Txn(no compares, Then Put(a) Put(b) Put(c))
;;   :cas   [{:a old :b old :c old} {:a new :b new :c new}]
;;            → Txn(If k.value==old for every k, Then Put(k, new) for every k)
;;
;; CAS commits all three Put ops in one WAL entry on the leader (single revision)
;; or none of them. Knossos' multi-register model checks linearizability
;; per-key; the atomic Txn ensures the three keys move together so no
;; per-key reordering is possible across a successful CAS.

(def ^:private multi-keys [:a :b :c])
(def ^:private multi-key-prefix "/jepsen/multi/")

(defn- multi-key-name [k] (str multi-key-prefix (name k)))

(defn do-multi-read
  "Reads all three keys and returns {:a v :b v :c v} (nil for absent). One
  prefix Get reads them at a single revision; three separate Gets could
  straddle a concurrent write and return a mix of two writes."
  [kv]
  (let [resp (-> (.get kv (->bs multi-key-prefix)
                       (-> (GetOption/builder) (.isPrefix true) (.build)))
                 (.get timeout-ms TimeUnit/MILLISECONDS))
        by-key (into {}
                     (for [kv-entry (.getKvs resp)]
                       [(bs-> (.getKey kv-entry))
                        (-> kv-entry .getValue bs-> Long/parseLong)]))]
    (into {} (for [k multi-keys] [k (get by-key (multi-key-name k))]))))

(defn do-multi-write
  "Atomic multi-key blind write. values is a {:a v :b v :c v} map (all keys
  required). Issued as a single Txn with no compares so all puts share a
  revision."
  [kv values]
  (let [puts (for [k multi-keys]
               (Op/put (->bs (multi-key-name k))
                       (->bs (get values k))
                       (PutOption/DEFAULT)))
        txn  (-> (.txn kv)
                 (.If    (into-array Cmp []))
                 (.Then  (into-array Op puts))
                 (.Else  (into-array Op []))
                 (.commit)
                 (.get timeout-ms TimeUnit/MILLISECONDS))]
    (when-not (.isSucceeded txn)
      (throw (ex-info "multi-write txn unexpectedly failed (no compares)" {})))
    :ok))

(defn do-multi-cas
  "Multi-key CAS: succeed only if every key's current value equals old[k],
  in which case all keys are atomically swapped to new[k]. Returns true on
  success, false on a value mismatch."
  [kv old new]
  (let [cmps (for [k multi-keys]
               (Cmp. (->bs (multi-key-name k))
                     Cmp$Op/EQUAL
                     (CmpTarget/value (->bs (get old k)))))
        puts (for [k multi-keys]
               (Op/put (->bs (multi-key-name k))
                       (->bs (get new k))
                       (PutOption/DEFAULT)))
        txn  (-> (.txn kv)
                 (.If    (into-array Cmp cmps))
                 (.Then  (into-array Op  puts))
                 (.Else  (into-array Op  []))
                 (.commit)
                 (.get timeout-ms TimeUnit/MILLISECONDS))]
    (.isSucceeded txn)))

(defrecord MultiRegisterClient [^Client conn kv]
  client/Client

  (open! [this test node]
    (let [c (connect! node)]
      (assoc this :conn c :kv (.getKVClient c))))

  (setup! [this test])

  (invoke! [this test op]
    (try
      (case (:f op)
        :read  (assoc op :type :ok :value (do-multi-read kv))
        :write (do (do-multi-write kv (:value op))
                   (assoc op :type :ok))
        :cas   (let [[old new] (:value op)
                     swapped?  (do-multi-cas kv old new)]
                 (assoc op :type (if swapped? :ok :fail))))

      (catch TimeoutException _
        (assoc op :type :info :error :timeout))

      (catch io.grpc.StatusRuntimeException e
        (assoc op :type (error-type op) :error (str (.getStatus e))))

      (catch Exception e
        (assoc op :type (error-type op) :error (.getMessage e)))))

  (teardown! [this test])

  (close! [this test]
    (when kv   (.close kv))
    (when conn (.close conn))))

(defn multi-register-client []
  (map->MultiRegisterClient {}))
