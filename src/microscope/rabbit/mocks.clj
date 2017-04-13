(ns microscope.rabbit.mocks
  (:require [microscope.io :as io]))

(declare mocked-rabbit-queue)
(defrecord FakeQueue [name cid rpc? delayed? messages]
  io/IO

  (listen [self function]
    (add-watch messages :watch (fn [_ _ _ actual]
                                 (let [msg (peek actual)]
                                   (when (and (not= msg :ACK)
                                              (not= msg :REJECT)
                                              (not (and delayed?
                                                        (some-> meta :x-delay (> 0)))))
                                     (function msg))))))

  (send! [self {:keys [payload meta] :or {meta {}}}]
    (when-not (and delayed? (some-> meta :x-delay (> 0)))
      (let [queue (if rpc?
                    (mocked-rabbit-queue (str name "-response") cid rpc? delayed?)
                    self)]
        (swap! (:messages queue) conj {:payload payload :meta (assoc meta :cid cid)}))))

  (ack! [_ {:keys [meta]}])
  (reject! [self msg ex])
  (log-message [_ _ _]))

(def queues (atom {}))
(defn mocked-rabbit-queue [name cid rpc? delayed?]
  (let [name-k (keyword name)
        mock-queue (get @queues name-k (->FakeQueue name cid rpc? delayed? (atom [])))]
    (swap! queues assoc name-k mock-queue)
    mock-queue))

(defn clear-mocked-env! []
  (doseq [[_ queue] @queues]
    (remove-watch (:messages queue) :watch))
  (reset! queues {}))

(defn rpc-call [queue-name arg]
  (swap! (:messages (mocked-rabbit-queue queue-name "DONT_MATTER" true false))
         conj {:payload arg}))

(defn rpc-response-of [name responses]
  (get responses (keyword name) (fn [a] (throw (ex-info "RPC not mocked!" {:name name
                                                                           :arg a})))))
