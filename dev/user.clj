(ns user
  (:require [microscope.rabbit.queue :as rabbit]
            [microscope.healthcheck :as health]))

(defn stop-system []
  (rabbit/disconnect!)
  (health/stop-health-checker!))
