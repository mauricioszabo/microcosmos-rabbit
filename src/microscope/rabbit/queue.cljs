(ns microscope.rabbit.queue
  (:require [clojure.core :as clj]
            [microscope.io :as io]
            ; [microscope.healthcheck :as health]
            [microscope.future :as future]
            [microscope.logging :as log]
            ; [microscope.rabbit.mocks :as mocks]
            [microscope.env :as env]))

(def ^:private amqp (js/require "amqplib"))
; (generators/add-encoder LongString generators/encode-str)
;
; (defn- parse-meta [meta]
;   (let [normalize-kv (fn [[k v]] [(keyword k) (if (instance? LongString v)
;                                                 (str v)
;                                                 v)])
;         headers (->> meta
;                      :headers
;                      (map normalize-kv)
;                      (into {}))]
;     (-> headers (merge meta) (dissoc :headers))))
;
; (def rabbit-default-meta [:cluster-id :app-id :message-id :expiration :type :user-id
;                           :delivery-tag :delivery-mode :priority :redelivery?
;                           :routing-key :content-type :persistent? :reply-to
;                           :content-encoding :correlation-id :exchange :timestamp])
;
; (defn- normalize-headers [meta]
;   (let [headers (->> meta
;                      (map (fn [[k v]] [(clj/name k) v]))
;                      (into {}))]
;     (apply dissoc headers (map name rabbit-default-meta))))
;
; (defn parse-payload [payload]
;   (-> payload (String. "UTF-8") io/deserialize-msg))
;
; (defn- retries-so-far [meta]
;   (get-in meta [:headers "retries"] 0))
;
; (defn ack-msg [queue meta]
;   (basic/ack (:channel queue) (:delivery-tag meta)))
;
; (defn requeue-msg [queue payload meta]
;   (basic/publish (:channel queue)
;                  ""
;                  (:name queue)
;                  payload
;                  (update meta :headers (fn [hash-map]
;                                          (let [map (into {} hash-map)]
;                                            (assoc map "retries"
;                                                   (-> meta retries-so-far inc)))))))
;
; (defn reject-or-requeue [queue meta payload]
;   (let [retries (retries-so-far meta)
;         send-to-deadletter #(basic/reject (:channel queue) (:delivery-tag meta) false)]
;     (cond
;       (>= retries (:max-retries queue)) (send-to-deadletter)
;       :requeue-message (do
;                          (requeue-msg queue payload meta)
;                          (ack-msg queue meta)))))
;
; (defn callback-payload [callback self meta payload]
;   (let [reject-msg #(basic/reject (:channel self) (:delivery-tag meta) false)]
;     (if (:redelivery? meta)
;       (reject-or-requeue self meta payload)
;       (let [payload (try (parse-payload payload) (catch JsonParseException _ :INVALID))]
;         (if (= payload :INVALID)
;           (reject-msg)
;           (callback {:payload payload :meta (parse-meta meta)}))))))
;
(defn- callback-payload [callback msg]
  (let [headers (-> msg .-headers js->clj)
        fields (-> msg .-fields js->clj)
        properties (-> msg .-properties js->clj)
        payload (.-content msg)
        meta (merge headers fields properties)
        message {:meta meta
                 :payload (-> payload .toString io/deserialize-msg)}]
    (callback (with-meta message {:original-msg msg}))))

(defn- raise-error []
  (throw (js/Error.
          (str "Can't publish to queue without CID. Maybe you tried to send a message "
               "using `queue` from components' namespace. Prefer to use the "
               "components' attribute to create one."))))

(defn p [a] (println "DBG" a) a)
(defrecord Queue [channel name max-retries cid]
  io/IO
  (listen [self function]
          (let [callback #(callback-payload function %)]
            (. channel then #(.consume % name callback))))

  (send! [_ {:keys [payload meta] :or {meta {}}}]
         (when-not cid (raise-error))
         (let [payload (io/serialize-msg payload)
               meta (assoc meta :headers (clj->js (assoc meta :cid cid)))]
           (. channel then #(. % publish name ""
                              (. js/Buffer from payload)
                              (clj->js meta)))))

  (ack! [_ msg]
    (println "ACK")
    (. channel (then #(->> msg meta p :original-msg p (.ack %)))))
;         (basic/ack channel (:delivery-tag meta)))
;
  (log-message [_ logger {:keys [payload meta]}])
;     (let [meta (assoc meta :queue name)]
;       (log/info logger "Processing message"
;                 :payload (io/serialize-msg payload)
;                 :meta (io/serialize-msg meta))))
;
  (reject! [self msg _]))
;            (let [meta (:meta msg)
;                  meta (assoc meta :headers (normalize-headers meta))
;                  payload (-> msg :payload io/serialize-msg)]
;              (reject-or-requeue self meta payload)))
;
;   health/Healthcheck
;   (unhealthy? [_] (when (core/closed? channel)
;                     {:channel "is closed"})))
;
(defonce connections (atom {}))

(def ^:private rabbit-config {:hosts (some-> :rabbit-config env/secret-or-env
                                             io/deserialize-msg)
                              :queues (some-> :rabbit-queues env/secret-or-env
                                              io/deserialize-msg)})

(defn- connection-to-host [host prefetch-count]
  (let [connection (delay (.connect amqp))
        channel (delay (. @connection then #(.createChannel %)))]

  ; (let [connect! #(let [connection (core/connect (get-in rabbit-config [:hosts host] {}))])])
  ; (let [connect! #(let [connection (core/connect (get-in rabbit-config [:hosts host] {}))])]))
;                         channel (doto (channel/open connection)
;                                       (basic/qos prefetch-count))]
;                     [connection channel])]
    (if-let [conn (get @connections host)]
      conn
      (get (swap! connections assoc host (future/join [@connection @channel])) host))))
;
(defn connection-to-queue [queue-name prefetch-count]
  (let [queue-host (get-in rabbit-config [:queues (keyword queue-name)])]
    (if queue-host
      (connection-to-host (keyword queue-host) prefetch-count)
      (connection-to-host :localhost prefetch-count))))

(defn disconnect! []
  (doseq [[_ promise] @connections]
    (. promise then (fn [[connection channel]]
                      (.close channel)
                      (.close connection))))
  (reset! connections {}))

(def ^:private os (js/require "os"))
(def default-queue-params {:exclusive false
                           :auto-ack false
                           :auto-delete false
                           :max-retries 5
                           :prefetch-count (* 5 (-> os .cpus .-length))
                           :durable true
                           :ttl (* 24 60 60 1000)})
;
; (defn define-queue [channel name opts]
;   (let [dead-letter-name (str name "-dlx")
;         dead-letter-q-name (str name "-deadletter")]
;     (queue/declare channel name (-> opts
;                                     (dissoc :max-retries :ttl :prefetch-count :route-to)
;                                     (assoc :arguments {"x-dead-letter-exchange" dead-letter-name
;                                                        "x-message-ttl" (:ttl opts)})))
;     (queue/declare channel dead-letter-q-name
;                    {:durable true :auto-delete false :exclusive false})
;     (exchange/fanout channel dead-letter-name {:durable true})
;     (queue/bind channel dead-letter-q-name dead-letter-name)))
;
; (defn- route-exchange [channel exchange-name queue-names opts]
;   (doseq [queue-name queue-names]
;     (do
;       (define-queue channel queue-name opts)
;       (queue/bind channel queue-name exchange-name))))
;
(defn- real-rabbit-queue [name opts]
  (let [dead-letter-name (str name "-dlx")
        dead-letter-q-name (str name "-deadletter")
        queue-args (assoc opts :arguments {"x-dead-letter-exchange" dead-letter-name
                                           "x-message-ttl" (:ttl opts)})
        promise
        (js/Promise. (fn [resolve]
                       (let [opts (merge default-queue-params opts)]
                         (-> (connection-to-queue name (:prefetch-count opts))
                             (.then (fn [[_ channel]]
                                      (-> (. channel assertQueue name (clj->js opts))
                                          (.then #(. channel assertQueue
                                                    dead-letter-q-name #js {:durable true}))
                                          (.then #(. channel assertExchange
                                                    name "direct")) ;(clj->js opts))
                                          (.then #(. channel assertExchange
                                                    dead-letter-name "fanout"
                                                    #js {:durable true}))
                                          (.then #(. channel bindQueue name name))
                                          (.then #(. channel bindQueue name dead-letter-name))
                                          (.then #(resolve channel)))))))))]

    ;
    ;     (define-queue channel name opts)
    ;
    ;     (if (:delayed opts)
    ;       (exchange/declare channel name "x-delayed-message"
    ;                         {:arguments {"x-delayed-type" "direct"}})
    ;       (exchange/declare channel name "fanout"))
    ;
    ;     (route-exchange channel name (or (:route-to opts) [name]) opts)
    (->Queue promise name (:max-retries opts) nil)))
;
(defn queue
  "Defines a new RabbitMQ's connection. Valid params are `:exclusive`, `:auto-delete`,
`:durable` and `:ttl`, all from Rabbit's documentation. We support additional
parameters:

- :max-retries is the number of times we'll try to process this message. Defaults
  to `5`
- :prefetch-count is the number of messages that rabbit will send us. By default,
  rabbit will send ALL messages to us - this is undesirable in most cases. So,
  we implement this as number-of-processors * 5. Changing it to `0` means
  'send all messages'.
- :route-to is a vector of strings. When we publish to this queue, it'll route to
  queue names defined in the vector.

In truth, this function defines an exchange with name defined by `name`. If we pass
`:route-to`, it'll define every queue inside the vector and binds the exchange to each
one, using *fanout*. If `:route-to` is omitted, it creates a queue with the same name
as the exchange, then binds one to another.
"
  [name & {:as opts}]
;   (mocks/clear-mocked-env!)
  (let [queue (delay (real-rabbit-queue name opts))]
    (fn [{:keys [cid mocked]}]
      (if mocked
        nil
;         (mocks/mocked-rabbit-queue name cid false (:delayed opts))
        (assoc @queue :cid cid)))))
