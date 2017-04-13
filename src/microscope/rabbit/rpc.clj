(ns microscope.rabbit.rpc
  (:require [microscope.rabbit.queue :as rabbit]
            [microscope.io :as io]
            [microscope.healthcheck :as health]
            [microscope.logging :as log]
            [langohr.basic :as basic]
            [langohr.core :as core]
            [langohr.consumers :as consumers]
            [langohr.queue :as queue]))

(defrecord Queue [channel name cid original-meta]
  io/IO
  (listen [self function]
          (let [callback (partial rabbit/callback-payload function 1 self)]
            (consumers/subscribe channel name callback)))

  (send! [_ {:keys [payload meta] :or {meta {}}}]
         (basic/publish channel "" (:reply-to original-meta)
                        (io/serialize-msg payload)
                        (select-keys original-meta [:correlation-id])))

  (ack! [_ {:keys [meta]}]
        (basic/ack channel (:delivery-tag meta)))

  (log-message [_ logger msg]
               (log/info logger "Processing RPC message" :msg msg))

  (reject! [self msg _]
           (basic/reject channel (-> msg :meta :delivery-tag) false))

  health/Healthcheck
  (unhealthy? [_] (when (core/closed? channel)
                    {:channel "is closed"})))

(defn deliver! [queue payload])

(defn- real-rabbit-queue [name opts]
  (let [opts (merge rabbit/default-queue-params opts)
        [connection channel] (rabbit/connection-to-queue name (:prefetch-count opts))]
    (rabbit/define-queue channel name opts)
    (->Queue channel name nil nil)))

(def queues (atom {}))

(declare mocked-rabbit-queue)
(defrecord FakeQueue [name messages cid]
  io/IO

  (listen [self function]
    (add-watch messages :watch (fn [_ _ _ actual]
                                 (let [msg (peek actual)]
                                   (when (and (not= msg :ACK)
                                              (not= msg :REJECT))
                                     (function msg))))))

  (send! [_ {:keys [payload meta] :or {meta {}}}]
         (let [response-queue (mocked-rabbit-queue (str name "-response") cid)]
           (swap! (:messages response-queue)
                  conj
                  {:payload payload :meta (assoc meta :cid cid)})))

  (ack! [_ _])
  (reject! [_ _ _])
  (log-message [_ _ _]))

(defn clear-mocked-env! []
  (doseq [[_ queue] @queues]
    (remove-watch (:messages queue) :watch))
  (reset! queues {}))

(defn- mocked-rabbit-queue [name cid]
  (let [name-k (keyword name)
        mock-queue (get @queues name-k (->FakeQueue name (atom []) cid))]
    (swap! queues assoc name-k mock-queue)
    mock-queue))

(defn queue [name & {:as opts}]
  (let [queue (delay (real-rabbit-queue name opts))]
    (clear-mocked-env!)
    (fn [{:keys [cid mocked meta]}]
      (if mocked
        (mocked-rabbit-queue name cid)
        (assoc @queue :cid cid :original-meta meta)))))

(defn mock-call [queue-name arg]
  (swap! (:messages (mocked-rabbit-queue queue-name {:cid "DONT_MATTER"}))
         conj {:payload arg}))

(defn- caller-fn [queue timeout-milis last-message]
  (fn [cid arg]
    (let [correlation-id (str (gensym))
          queue (assoc queue :cid cid)
          p (promise)]
      (add-watch last-message
                 correlation-id
                 (fn [_ _ _ value]
                   (when (-> value :meta :correlation-id (= correlation-id))
                     (deliver p (:payload value)))))

      (io/send! queue {:payload arg
                       :meta {:reply-to "amq.rabbitmq.reply-to"
                              :correlation-id correlation-id}})
      (let [result (deref p timeout-milis :TIMEOUT)]
        (remove-watch last-message correlation-id)
        (if (= result :TIMEOUT)
          (throw (ex-info "TIMEOUT" {:timeout-milis timeout-milis
                                     :rpc-queue (:name queue)}))
          result)))))

(defn- create-caller [name params]
  (let [queue ((rabbit/queue name) params)
        channel (:channel queue)
        last-message (atom nil)]

    (consumers/subscribe channel
                         "amq.rabbitmq.reply-to"
                         (fn [_ meta payload]
                           (swap! last-message
                                  (constantly {:meta meta
                                               :payload (rabbit/parse-payload payload)})))
                         {:auto-ack true})
    (caller-fn queue (-> params :timeout (or 5000)) last-message)))

(defn- mock-response-of [name responses]
  (get responses (keyword name) (fn [a] (throw (ex-info "RPC not mocked!" {:name name
                                                                           :arg a})))))

(defn caller [name & {:as args}]
  (let [caller (delay (create-caller name args))]
    (fn [params]
      (if (:mocked params)
        (mock-response-of name (:rpc-responses params))
        (partial @caller (:cid params))))))
