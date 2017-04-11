(ns microscope.rabbit.queue
  (:require [cheshire.core :as json]
            [cheshire.generate :as generators]
            [clojure.core :as clj]
            [microscope.io :as io]
            [microscope.healthcheck :as health]
            [microscope.future :as future]
            [microscope.logging :as log]
            [langohr.basic :as basic]
            [langohr.channel :as channel]
            [langohr.consumers :as consumers]
            [langohr.core :as core]
            [langohr.exchange :as exchange]
            [langohr.queue :as queue]
            [environ.core :refer [env]])
  (:import [com.rabbitmq.client LongString]
           [com.fasterxml.jackson.core JsonParseException]))

(generators/add-encoder LongString generators/encode-str)

(defn- parse-meta [meta]
  (let [normalize-kv (fn [[k v]] [(keyword k) (if (instance? LongString v)
                                                (str v)
                                                v)])
        headers (->> meta
                     :headers
                     (map normalize-kv)
                     (into {}))]
    (-> headers (merge meta) (dissoc :headers))))

(def rabbit-default-meta [:cluster-id :app-id :message-id :expiration :type :user-id
                          :delivery-tag :delivery-mode :priority :redelivery?
                          :routing-key :content-type :persistent? :reply-to
                          :content-encoding :correlation-id :exchange :timestamp])

(defn- normalize-headers [meta]
  (let [headers (->> meta
                     (map (fn [[k v]] [(clj/name k) v]))
                     (into {}))]
    (apply dissoc headers (map name rabbit-default-meta))))

(defn- parse-payload [payload]
  (-> payload (String. "UTF-8") io/deserialize-msg))

(defn- retries-so-far [meta]
  (get-in meta [:headers "retries"] 0))

(defn ack-msg [queue meta]
  (basic/ack (:channel queue) (:delivery-tag meta)))

(defn requeue-msg [queue payload meta]
  (basic/publish (:channel queue)
                 ""
                 (:name queue)
                 payload
                 (update meta :headers (fn [hash-map]
                                         (let [map (into {} hash-map)]
                                           (assoc map "retries"
                                                  (-> meta retries-so-far inc)))))))

(defn reject-or-requeue [queue meta payload]
  (let [retries (retries-so-far meta)
        send-to-deadletter #(basic/reject (:channel queue) (:delivery-tag meta) false)]
    (cond
      (>= retries (:max-retries queue)) (send-to-deadletter)
      :requeue-message (do
                         (requeue-msg queue payload meta)
                         (ack-msg queue meta)))))

(defn- callback-payload [callback max-retries self _ meta payload]
  (let [retries (retries-so-far meta)
        reject-msg #(basic/reject (:channel self) (:delivery-tag meta) false)]
    (if (:redelivery? meta)
      (reject-or-requeue self meta payload)
      (let [payload (try (parse-payload payload) (catch JsonParseException _ :INVALID))]
        (if (= payload :INVALID)
          (reject-msg)
          (callback {:payload payload :meta (parse-meta meta)}))))))

(defn- raise-error []
  (throw (IllegalArgumentException.
          (str "Can't publish to queue without CID. Maybe you tried to send a message "
               "using `queue` from components' namespace. Prefer to use the "
               "components' attribute to create one."))))

(defrecord Queue [channel name max-retries cid]
  io/IO
  (listen [self function]
          (let [callback (partial callback-payload function max-retries self)]
            (consumers/subscribe channel name callback)))

  (send! [_ {:keys [payload meta] :or {meta {}}}]
         (when-not cid (raise-error))
         (let [payload (io/serialize-msg payload)
               meta (assoc meta :headers (normalize-headers (assoc meta :cid cid)))]
           (basic/publish channel name "" payload meta)))

  (ack! [_ {:keys [meta]}]
        (basic/ack channel (:delivery-tag meta)))

  (log-message [_ logger msg]
               (log/info logger "Processing message" :msg msg))

  (reject! [self msg _]
           (let [meta (:meta msg)
                 meta (assoc meta :headers (normalize-headers meta))
                 payload (-> msg :payload io/serialize-msg)]
             (reject-or-requeue self meta payload)))

  health/Healthcheck
  (unhealthy? [_] (when (core/closed? channel)
                    {:channel "is closed"})))

(defonce connections (atom {}))

(def ^:private rabbit-config {:hosts (-> env :rabbit-config (json/parse-string true))
                              :queues (-> env :rabbit-queues (json/parse-string true))})

(defn- connection-to-host [host prefetch-count]
  (let [connect! #(let [connection (core/connect (get-in rabbit-config [:hosts host] {}))
                        channel (doto (channel/open connection)
                                      (basic/qos prefetch-count))]
                    [connection channel])]
    (if-let [conn (get @connections host)]
      conn
      (get (swap! connections assoc host (connect!)) host))))

(defn- connection-to-queue [queue-name prefetch-count]
  (let [queue-host (get-in rabbit-config [:queues (keyword queue-name)])]
    (if queue-host
      (connection-to-host (keyword queue-host) prefetch-count)
      (connection-to-host :localhost prefetch-count))))

(defn disconnect! []
  (doseq [[_ [connection channel]] @connections]
    (core/close channel)
    (core/close connection))
  (reset! connections {}))

(def default-queue-params {:exclusive false
                           :auto-ack false
                           :auto-delete false
                           :max-retries 5
                           :prefetch-count (* 5 future/num-cpus)
                           :durable true
                           :ttl (* 24 60 60 1000)})

(defn- real-rabbit-queue [name opts]
  (let [opts (merge default-queue-params opts)
        [connection channel] (connection-to-queue name (:prefetch-count opts))
        dead-letter-name (str name "-dlx")
        dead-letter-q-name (str name "-deadletter")]

    (queue/declare channel name (-> opts
                                    (dissoc :max-retries :ttl :prefetch-count)
                                    (assoc :arguments {"x-dead-letter-exchange" dead-letter-name
                                                       "x-message-ttl" (:ttl opts)})))
    (queue/declare channel dead-letter-q-name
                   {:durable true :auto-delete false :exclusive false})

    (if (:delayed opts)
      (exchange/declare channel name "x-delayed-message"
                        {:arguments {"x-delayed-type" "direct"}})
      (exchange/declare channel name "fanout"))
    (queue/bind channel name name)

    (exchange/fanout channel dead-letter-name {:durable true})
    (queue/bind channel dead-letter-q-name dead-letter-name)
    (->Queue channel name (:max-retries opts) nil)))

(def queues (atom {}))

(defrecord FakeQueue [messages cid delayed]
  io/IO

  (listen [self function]
    (add-watch messages :watch (fn [_ _ _ actual]
                                 (let [msg (peek actual)]
                                   (when (and (not= msg :ACK)
                                              (not= msg :REJECT)
                                              (not (and delayed
                                                        (some-> meta :x-delay (> 0)))))
                                     (function msg))))))

  (send! [_ {:keys [payload meta] :or {meta {}}}]
    (when-not (and delayed (some-> meta :x-delay (> 0)))
      (swap! messages conj {:payload payload :meta (assoc meta :cid cid)})))

  (ack! [_ {:keys [meta]}]
    (swap! messages conj :ACK))

  (reject! [self msg ex]
    (swap! messages conj :REJECT))

  (log-message [_ _ _]))

(defn- mocked-rabbit-queue [name cid delayed]
  (let [name-k (keyword name)
        mock-queue (get @queues name-k (->FakeQueue (atom []) cid delayed))]
    (swap! queues assoc name-k mock-queue)
    mock-queue))

(defn clear-mocked-env! []
  (doseq [[_ queue] @queues]
    (remove-watch (:messages queue) :watch))
  (reset! queues {}))

(defn queue
  "Defines a new RabbitMQ's connection. Valid params ar `:exclusive`, `:auto-delete`,
`:durable` and `:ttl`, all from Rabbit's documentation. We support additional
parameters:

- :max-retries is the number of times we'll try to process this message. Defaults
  to `5`
- :prefetch-count is the number of messages that rabbit will send us. By default,
  rabbit will send ALL messages to us - this is undesirable in most cases. So,
  we implement this as number-of-processors * 5. Changing it to `0` means
  'send all messages'.
"
  [name & {:as opts}]
  (clear-mocked-env!)
  (let [queue (delay (real-rabbit-queue name opts))]
    (fn [{:keys [cid mocked]}]
      (if mocked
        (mocked-rabbit-queue name cid (:delayed opts))
        (assoc @queue :cid cid)))))
