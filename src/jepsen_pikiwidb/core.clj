(ns jepsen-pikiwidb.core
  (:require [jepsen [db :as jdb]
                    [cli :as cli]
                    [tests :as tests]
                    [control :as c]
                    [util :as util] ]
            [jepsen.os.ubuntu :as ubuntu]
            [clojure.string :as str]
            [clojure.java.shell :refer [sh]]
            [slingshot.slingshot :refer [try+ throw+]]
            [clojure.tools.logging :refer [info warn]]))

(def build-file
  "A file we create to track the last built version; speeds up compilation."
  "jepsen-built-version")

(def build-locks
  "We use these locks to prevent concurrent builds."
  (util/named-locks))

(defmacro with-build-version
  "Takes a test, a repo name, a version, and a body. Builds the repo by
  evaluating body, only if it hasn't already been built. Takes out a lock on a
  per-repo basis to prevent concurrent builds. Remembers what version was last
  built by storing a file in the repo directory. Returns the result of body if
  evaluated, or the build directory."
  [node repo-name version & body]
  `(util/with-named-lock build-locks [~node ~repo-name]
     (let [build-file# (str "/" ~repo-name "/" build-file)]
       (if (try+ (= (str ~version) (c/exec :cat build-file#))
                (catch [:exit 1] e# ; Not found
                  false))
         ; Already built
         (str "/" ~repo-name)
         ; Build
         (let [res# (do ~@body)]
           ; Log version
           (c/exec :echo ~version :> build-file#)
           res#)))))

(defn build-pikiwidb!
  "Compiles pikiwidb, and returns the binary file we built."
  [test node]
  (let [version (:version test)]
    (with-build-version node "pikiwidb" version
      (info "Building pikiwidb" (:version test))
      (c/cd "/pikiwidb"
        (c/exec "./build.sh"))
      "/pikiwidb/bin/pikiwidb")))

(defn pdb 
  "PikiwiDB"
  [version]
  (reify jdb/DB
    (setup! [_ test node]
      (info node "installing pikiwidb" (:version test))
      (let [bin (build-pikiwidb! test node)]))

    (teardown! [this test node]
      (info node "tearing down pikiwidb" (:version test))
      )))

(defn pikiwidb-test
  "Builds up a PikiwiDB test"
  [opts]
  (merge tests/noop-test
         {:db (pdb "0.0.1")
          :version "0.0.1"
          :os ubuntu/os
          :pure-generators true}
         opts))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for browing results"
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn pikiwidb-test})
                   (cli/serve-cmd))
            args))
