(ns jepsen.t4.core
  "Entry point for the T4 Jepsen test suite."
  (:require [clojure.tools.logging    :refer [info]]
            [jepsen
             [cli      :as cli]
             [checker  :as checker]
             [control  :as c]
             [generator :as gen]
             [nemesis  :as nemesis]
             [tests    :as tests]]
            [jepsen.checker.timeline  :as timeline]
            [jepsen.os.debian         :as debian]
            [knossos.model            :as model]
            [jepsen.t4
             [db     :as db]
             [client :as client]]))

;; ── Nemeses ───────────────────────────────────────────────────────────────────

(def nemeses
  {:none
   {:nemesis  nemesis/noop
    :gen      nil}

   ;; Randomly bisects the 5 nodes into a 2/3 split and heals.
   :partition-halves
   {:nemesis  (nemesis/partition-random-halves)
    :gen      (gen/cycle
                [(gen/sleep 10)
                 {:type :info :f :start}
                 (gen/sleep 20)
                 {:type :info :f :stop}])}

   ;; Kills and restarts the t4 process on a random node.
   :kill
   {:nemesis  (nemesis/node-start-stopper
                rand-nth
                (fn start [test node] (db/start! test node))
                (fn stop  [test node] (db/stop!  test node)))
    :gen      (gen/cycle
                [(gen/sleep 15)
                 {:type :info :f :start}
                 (gen/sleep 15)
                 {:type :info :f :stop}])}

   ;; Partition a random node away from the S3 server using iptables.
   ;; Tests S3-write failure handling; requires NET_ADMIN cap on node containers.
   :partition-s3
   {:nemesis  (let [isolated (atom nil)]
                (reify nemesis/Nemesis
                  (setup! [this _test] this)
                  (invoke! [this test op]
                    (case (:f op)
                      :isolate-s3
                      (let [node (rand-nth (:nodes test))]
                        (reset! isolated node)
                        (info "isolating" node "from s3")
                        (c/on-nodes test [node]
                          (fn [_ _]
                            (c/su (c/exec :iptables :-A :OUTPUT
                                          :-d "s3" :-p "tcp"
                                          :--dport "9000" :-j "DROP"))))
                        (assoc op :type :info :value node))

                      :heal-s3
                      (if-let [node @isolated]
                        (do (reset! isolated nil)
                            (info "healing" node "→ s3")
                            (c/on-nodes test [node]
                              (fn [_ _]
                                (c/su (c/exec :iptables :-D :OUTPUT
                                              :-d "s3" :-p "tcp"
                                              :--dport "9000" :-j "DROP"))))
                            (assoc op :type :info :value node))
                        (assoc op :type :info :value nil))))
                  (teardown! [this test]
                    (when-let [node @isolated]
                      (c/on-nodes test [node]
                        (fn [_ _]
                          (try (c/su (c/exec :iptables :-D :OUTPUT
                                             :-d "s3" :-p "tcp"
                                             :--dport "9000" :-j "DROP"))
                               (catch Exception _)))))
                    this)))
    :gen      (gen/cycle
                [(gen/sleep 10)
                 {:type :info :f :isolate-s3}
                 (gen/sleep 20)
                 {:type :info :f :heal-s3}])}})

;; ── Workloads ─────────────────────────────────────────────────────────────────

(defn register-workload
  "CAS-register workload: verifies linearizability of read/write/cas on one key."
  []
  {:client  (client/register-client)
   :checker (checker/compose
              {:linearizable (checker/linearizable
                               {:model     (model/cas-register)
                                :algorithm :linear})
               :timeline     (timeline/html)
               :perf         (checker/perf)})
   ;; Mix of reads (50%), writes (30%), CAS (20%).
   :gen     (gen/mix
              [(repeat 5 {:type :invoke :f :read  :value nil})
               (repeat 3 (fn [] {:type :invoke :f :write
                                 :value (rand-int 10)}))
               (repeat 2 (fn [] {:type :invoke :f :cas
                                 :value [(rand-int 10) (rand-int 10)]}))])})

(defn- rand-multi-values
  "Returns a {:a v :b v :c v} map of fresh random small integers — the value
  space the multi-register workload writes and CASes against."
  []
  {:a (rand-int 5) :b (rand-int 5) :c (rand-int 5)})

;; Initial state of the multi-register: every key absent. cas-register's
;; default initial value is nil, which would mismatch the {:a nil :b nil
;; :c nil} map that the client returns before any write — so we seed the
;; model with the absent-everything map explicitly.
(def ^:private multi-register-init
  {:a nil :b nil :c nil})

(defn multi-register-workload
  "Multi-key CAS-register workload: exercises etcd Txn with three keys.

  The whole {:a v :b v :c v} map is treated as one register value; the model
  is knossos.model/cas-register over that map. T4's multi-key Txn writes all
  three keys in a single WAL entry (one revision), so the 3-tuple either
  moves to the new map or stays at the old one — exactly the semantics
  cas-register models. Per-key linearizability is therefore implied: there
  is no schedule in which one key's value moves without the others moving
  to the same revision."
  []
  {:client  (client/multi-register-client)
   :checker (checker/compose
              {:linearizable (checker/linearizable
                               {:model     (model/cas-register multi-register-init)
                                :algorithm :linear})
               :timeline     (timeline/html)
               :perf         (checker/perf)})
   :gen     (gen/mix
              [(repeat 5 {:type :invoke :f :read :value nil})
               (repeat 3 (fn [] {:type :invoke :f :write
                                 :value (rand-multi-values)}))
               (repeat 2 (fn [] {:type :invoke :f :cas
                                 :value [(rand-multi-values)
                                         (rand-multi-values)]}))])})

(def workloads
  {:register       register-workload
   :multi-register multi-register-workload})

;; ── Test constructor ──────────────────────────────────────────────────────────

(defn t4-test
  [opts]
  (let [workload-name (or (:workload opts) :register)
        workload      ((get workloads workload-name register-workload))
        nemesis-name  (or (:nemesis opts) :partition-halves)
        nem           (get nemeses nemesis-name (:partition-halves nemeses))
        time-limit    (or (:time-limit opts) 120)]
    (merge
      tests/noop-test
      {:name      (str "t4 " (name workload-name)
                       " / " (name nemesis-name))
       :os        debian/os
       :db        (db/db)
       :client    (:client workload)
       :nemesis   (:nemesis nem)
       :checker   (:checker workload)
       :generator (gen/phases
                    ;; Main test phase: interleave client ops with nemesis faults.
                    (->> (:gen workload)
                         (gen/stagger 1.0)    ; ~1 op/s × 120 s ≈ 120 ops — low concurrency keeps Knossos WGL from OOMing during partitions
                         (gen/nemesis (:gen nem))
                         (gen/time-limit time-limit))
                    ;; Recovery phase: let the cluster heal, then do a final read.
                    (gen/log "waiting for recovery")
                    (gen/sleep 15)
                    (gen/clients
                      (gen/once {:type :invoke :f :read :value nil})))}
      ;; Strip keys we've already resolved so their raw keyword values
      ;; (e.g. :nemesis :partition-halves) don't overwrite the objects above.
      (dissoc opts :nemesis :workload))))

;; ── CLI ───────────────────────────────────────────────────────────────────────

(def cli-opts
  "Extra CLI options beyond Jepsen's defaults."
  [[nil "--workload NAME"
    "Workload to run: register (default) | multi-register"
    :default :register
    :parse-fn keyword]
   [nil "--nemesis NAME"
    "Nemesis: none | partition-halves | kill | partition-s3 (default: kill)"
    :default :partition-halves
    :parse-fn keyword]])

(defn -main
  [& args]
  (cli/run!
    (merge (cli/single-test-cmd {:test-fn   t4-test
                                 :opt-spec  cli-opts})
           (cli/serve-cmd))
    args))
