(defproject jepsen-pikiwidb "0.1.0-SNAPSHOT"
  :description "Jepsen test for PikiwiDB"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [jepsen "0.1.18"]
                 [com.taoensso/carmine "2.19.1"]]
  :repl-options {:init-ns jepsen-pikiwidb.core}
  :main jepsen-pikiwidb.core)
