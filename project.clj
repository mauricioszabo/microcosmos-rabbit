(defproject microscope/rabbit "0.1.0"
  :description "RabbitMQ implementation for Microscope"
  :url "https://github.com/acessocard/microscope"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [com.novemberain/langohr "3.6.1"]
                 [microscope "0.1.0"]]

  :profiles {:dev {:src-paths ["dev"]
                   :dependencies [[midje "1.8.3"]]
                   :plugins [[lein-midje "3.2.1"]]}})
