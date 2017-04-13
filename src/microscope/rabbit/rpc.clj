(ns microscope.rabbit.rpc
  (:require [microscope.rabbit.queue :as rabbit]
            [microscope.io :as io]
            [microscope.healthcheck :as health]
            [microscope.logging :as log]
            [langohr.basic :as basic]
            [langohr.core :as core]))

(defrecord Queue [channel]
  io/IO
  (listen [self function])

  (send! [_ {:keys [payload meta] :or {meta {}}}])

  (ack! [_ {:keys [meta]}]
        (basic/ack channel (:delivery-tag meta)))

  (log-message [_ logger msg]
               (log/info logger "Processing message" :msg msg))

  (reject! [self msg _]
           (basic/reject channel (-> msg :meta :delivery-tag) false))

  health/Healthcheck
  (unhealthy? [_] (when (core/closed? channel)
                    {:channel "is closed"})))

(defn deliver! [queue payload])

(defn- real-rabbit-queue [name opts]
  (let [opts (merge rabbit/default-queue-params opts)
        [connection channel] (rabbit/connection-to-queue name (:prefetch-count opts))]
    (rabbit/define-queue channel name opts)))

(defn queue [name & {:as opts}]
  (fn [params]))

(defn caller [name]
  (fn [params]))
