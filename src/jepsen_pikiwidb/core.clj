(ns jepsen-pikiwidb.core
  (:require [jepsen.cli :as cli]
            [jepsen.tests :as tests]))

(defn foo
  "I don't do a whole lot."
  [x]
  (println x "Hello, World!"))

(defn pikiwidb-test
  "Builds up a PikiwiDB test"
  [opts]
  (merge tests/noop-test
         {:pure-generators true}
         opts))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for browing results"
  [& args]
  (cli/run! (cli/single-test-cmd {:test-fn pikiwidb-test})
            args))
