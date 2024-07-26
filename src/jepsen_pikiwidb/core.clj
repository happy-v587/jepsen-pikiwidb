(ns jepsen-pikiwidb.core
  (:require [jepsen [db :as jdb]
             [cli :as cli]
             [tests :as tests]
             [control :as c]
             [util :as util :refer [parse-long]]]
            [jepsen.os.ubuntu :as ubuntu]
            [jepsen-pikiwidb [client :as rc]]
            [jepsen.tests.cycle.append :as append]
            [jepsen.control.util :as cu]
            [slingshot.slingshot :refer [try+ throw+]]
            [clojure.string :as str]
            [clojure.java.shell :refer [sh]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen-pikiwidb [append :as append]
             [db     :as rdb]
             [nemesis :as nemesis]]))

(def nemeses
  "Types of faults a nemesis can create."
  #{:pause :kill :partition :clock :member :island :mystery})

(def standard-nemeses
  "Combinations of nemeses for tests"
  [[]
   [:pause]
   [:kill]
   [:partition]
   [:island]
   [:mystery]
   [:pause :kill :partition :clock :member]])

(def special-nemeses
  "A map of special nemesis names to collections of faults"
  {:none      []
   :standard  [:pause :kill :partition :clock :member]
   :all       [:pause :kill :partition :clock :member :island :mystery]})

(defn parse-nemesis-spec
  "Takes a comma-separated nemesis string and returns a collection of keyword
  faults."
  [spec]
  (->> (str/split spec #",")
       (map keyword)
       (mapcat #(get special-nemeses % [%]))))

(def workloads
  "A map of workload names to functions that can take opts and construct
  workloads."
  {:append  append/workload})

(def standard-workloads
  "The workload names we run for test-all by default."
  (keys workloads))

(def cli-opts
  "Options for test runners."
  [[nil "--follower-proxy" "If true, proxy requests from followers to leader."
    :default false]

   [nil "--key-count INT" "For the append test, how many keys should we test at once?"
    :parse-fn parse-long
    :validate [pos? "must be positive"]]

   [nil "--max-txn-length INT" "What's the most operations we can execute per transaction?"
    :parse-fn parse-long
    :validate [pos? "must be positive"]]

   [nil "--max-writes-per-key INT" "How many writes can we perform to any single key, for append tests?"
    :parse-fn parse-long
    :validate [pos? "must be positive"]]

   [nil "--nemesis FAULTS" "A comma-separated list of nemesis faults to enable"
    :parse-fn parse-nemesis-spec
    :validate [(partial every? (into nemeses (keys special-nemeses)))
               (str "Faults must be one of " nemeses " or "
                    (cli/one-of special-nemeses))]]

   [nil "--nemesis-interval SECONDS" "How long to wait between nemesis faults."
    :default  3
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "must be a positive number"]]

   [nil "--nuke-after-leave" "If true, kills and wipes data files for nodes after they leave the cluster. Enabling this flag lets the test run for longer, but also prevents us from seeing misbehavior by nodes which SOME nodes think are removed, but which haven't yet figured it out themselves. We have this because Redis doesn't actually shut nodes down when they find out they're removed."
    :default true]

   [nil "--raft-version VERSION" "What version of redis-raft should we test?"
    :default "407ed4e"]

   [nil "--raft-repo URL" "Where we clone redis-raft from?"
    :default "https://github.com/redislabs/redisraft"]

   ["-r" "--rate HZ" "Approximate number of requests per second per thread"
    :default 10
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "must be a positive number"]]

   [nil "--raft-log-max-file-size BYTES" "Size of the raft log, before compaction"
    ; Default is 64MB, but we like to break things. This works out to about
    ; every 5 seconds with 5 clients.
    :default 32000
    :parse-fn parse-long
    :validate [pos? "must be positive"]]

   [nil "--raft-log-max-cache-size BYTES" "Size of the in-memory Raft Log cache"
    :default 1000000
    :parse-fn parse-long
    :validate [pos? "must be positive"]]

   [nil "--tcpdump" "Create tcpdump logs for debugging client-server interaction"
    :default false]

   [nil "--redis-version VERSION" "What version of Redis should we test?"
    :default "6.2.2"]

   [nil "--redis-repo URL" "Where we clone redis from?"
    :default "https://github.com/redis/redis"]

   ["-w" "--workload NAME" "What workload should we run?"
    :parse-fn keyword
    :validate [workloads (cli/one-of workloads)]]])

(defn pikiwidb-test
  "Builds up a PikiwiDB test"
  [opts]
  (let [workload ((workloads (:workload opts)) opts)]
        (merge tests/noop-test
               workload
               {:db              (pdb "0.0.1")
                :version         "0.0.1"
                :os              ubuntu/os
                :pure-generators true}
               opts)))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for browing results"
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn pikiwidb-test :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
