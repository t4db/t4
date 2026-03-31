(ns jepsen.strata.core
  "Entry point for the Strata Jepsen test suite."
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
            [jepsen.strata
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

   ;; Kills and restarts the strata process on a random node.
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

   ;; Partition a random node away from MinIO using iptables.
   ;; Tests S3-write failure handling; requires NET_ADMIN cap on node containers.
   :partition-minio
   {:nemesis  (let [isolated (atom nil)]
                (reify nemesis/Nemesis
                  (setup! [this _test] this)
                  (invoke! [this test op]
                    (case (:f op)
                      :isolate-minio
                      (let [node (rand-nth (:nodes test))]
                        (reset! isolated node)
                        (info "isolating" node "from minio")
                        (c/on-nodes test [node]
                          (fn [_ _]
                            (c/su (c/exec :iptables :-A :OUTPUT
                                          :-d "minio" :-p "tcp"
                                          :--dport "9000" :-j "DROP"))))
                        (assoc op :type :info :value node))

                      :heal-minio
                      (if-let [node @isolated]
                        (do (reset! isolated nil)
                            (info "healing" node "→ minio")
                            (c/on-nodes test [node]
                              (fn [_ _]
                                (c/su (c/exec :iptables :-D :OUTPUT
                                              :-d "minio" :-p "tcp"
                                              :--dport "9000" :-j "DROP"))))
                            (assoc op :type :info :value node))
                        (assoc op :type :info :value nil))))
                  (teardown! [this test]
                    (when-let [node @isolated]
                      (c/on-nodes test [node]
                        (fn [_ _]
                          (try (c/su (c/exec :iptables :-D :OUTPUT
                                             :-d "minio" :-p "tcp"
                                             :--dport "9000" :-j "DROP"))
                               (catch Exception _)))))
                    this)))
    :gen      (gen/cycle
                [(gen/sleep 10)
                 {:type :info :f :isolate-minio}
                 (gen/sleep 20)
                 {:type :info :f :heal-minio}])}})

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

(def workloads
  {:register register-workload})

;; ── Test constructor ──────────────────────────────────────────────────────────

(defn strata-test
  [opts]
  (let [workload-name (or (:workload opts) :register)
        workload      ((get workloads workload-name register-workload))
        nemesis-name  (or (:nemesis opts) :partition-halves)
        nem           (get nemeses nemesis-name (:partition-halves nemeses))
        time-limit    (or (:time-limit opts) 120)]
    (merge
      tests/noop-test
      {:name      (str "strata " (name workload-name)
                       " / " (name nemesis-name))
       :os        debian/os
       :db        (db/db)
       :client    (:client workload)
       :nemesis   (:nemesis nem)
       :checker   (:checker workload)
       :generator (gen/phases
                    ;; Main test phase: interleave client ops with nemesis faults.
                    (->> (:gen workload)
                         (gen/stagger 0.3)    ; ~3 ops/s x 120 s ≈ 360 ops — Knossos WGL OOMs above ~500 history events
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
    "Workload to run: register (default)"
    :default :register
    :parse-fn keyword]
   [nil "--nemesis NAME"
    "Nemesis: none | partition-halves | kill | partition-minio (default: partition-halves)"
    :default :partition-halves
    :parse-fn keyword]])

(defn -main
  [& args]
  (cli/run!
    (merge (cli/single-test-cmd {:test-fn   strata-test
                                 :opt-spec  cli-opts})
           (cli/serve-cmd))
    args))
