(defproject microscope/rabbit "0.2.0"
  :description "RabbitMQ implementation for Microscope"
  :url "https://github.com/acessocard/microscope"
  :license {:name "MIT License"
            :url "https://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/clojurescript "1.9.908"]
                 [com.novemberain/langohr "3.6.1"]
                 [microscope "0.2.1-SNAPSHOT"]]

  :profiles {:dev {:src-paths ["dev"]
                   :dependencies [[midje "1.8.3"]
                                  [figwheel-sidecar "0.5.9"]
                                  [com.cemerick/piggieback "0.2.1"]]
                   :repl-options {:nrepl-middleware [cemerick.piggieback/wrap-cljs-repl]}
                   :plugins [[lein-midje "3.2.1"]]}}

  :cljsbuild {:builds [{:source-paths ["src"]
                        :id "prod"
                        :compiler {:output-to "target/main.js"
                                   :optimizations :simple
                                   :hashbang false
                                   :output-wrapper true
                                   :pretty-print true
                                   :target :nodejs}}
                       {:source-paths ["src" "test"]
                        :id "dev"
                        :figwheel true
                        :compiler {:output-to "target/main.js"
                                   :output-dir "target/js"
                                   :main microscope.rabbit.all-tests
                                   :optimizations :none
                                   :target :nodejs}}]})
