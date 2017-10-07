(ns microscope.rabbit.queue-test
  (:require [microscope.core :as components]
            [microscope.io :as io]
            [microscope.healthcheck :as health]
            [microscope.future :as future]
            [microscope.rabbit.queue :as rabbit]
            [microscope.rabbit.mocks :as mocks]
            [microscope.logging :as log]
            [microscope.rabbit.async-helper :as helper]))

#?(:cljs (require-macros '[cljs.core.async.macros :refer [go]]))

#?(:clj
   (require '[clojure.test :refer [is run-tests testing deftest]]
            '[clojure.core.async :refer [chan >! timeout go <!]]
            '[microscope.rabbit.async-helper :refer [def-async-test await! await-all!]]
            '[langohr.consumers :as consumers]
            '[langohr.basic :as basic]
            '[langohr.core :as core])
   :cljs
   (require '[clojure.test :refer-macros [is testing run-tests async deftest]]
            '[cljs.core.async :refer [chan >! timeout <!]]
            '[microscope.rabbit.async-helper :refer-macros [def-async-test await! await-all!]]))


#?(:cljs
   (defn raw-consume [channel name callback]
     (. channel then (fn [c]
                       (.consume c name (fn [msg]
                                          (.ack c msg)
                                          (callback (-> msg .-content .toString)))))))
   :clj
   (defn raw-consume [channel name callback]
     (consumers/subscribe channel name (fn [_ meta msg]
                                         (basic/ack channel (:delivery-tag meta))
                                         (callback (String. msg))))))

(defn in-future [f]
  (fn [future _] (future/map f future)))

(defn logger-gen [logger-chan]
  (fn [{:keys [cid]}]
    (reify log/Log
      (log [_ msg type data]
           (go (>! logger-chan (assoc data
                                      :message msg
                                      :type type
                                      :cid cid)))))))

(defn subscribe-all! []
  (let [test-queue (rabbit/queue "test" :max-retries 1)
        real-test-q (test-queue {:cid "FOO"})
        result-queue (rabbit/queue "test-result")
        channel (:channel (result-queue {}))
        logger-chan (chan)
        all-msgs-chan (chan)
        sub (components/subscribe-with :result-q result-queue
                                       :logger (logger-gen logger-chan)
                                       :test-queue test-queue
                                       :result-queue result-queue)
        send-msg (fn [msg {:keys [result-q]}]
                   (future/map (fn [value]
                                 (go (>! all-msgs-chan (:payload value)))
                                 (case (:payload value)
                                   "error" (throw (ex-info "Some Error" {}))
                                   "fatal" (rabbit/disconnect!)
                                   (io/send! result-q value)))
                               msg))
        results-chan (chan)
        dead-chan (chan)]

    (sub :test-queue send-msg)
    (sub :result-queue (in-future #(go (>! results-chan %))))
    (raw-consume (:channel real-test-q) "test-deadletter"
                 #(go (>! dead-chan %)))

    {:send! (fn [ & msgs] (doseq [m msgs] (io/send! (test-queue {:cid "FOO"}) m)))
     :queue real-test-q
     :results results-chan
     :deadletter dead-chan
     :all-messages all-msgs-chan
     :logger logger-chan}))

; (def-async-test "Handling healthchecks" {:teardown (rabbit/disconnect!)}
;   (let [health (chan)
;         q-generator (rabbit/queue "test" :auto-delete true)
;         queue (q-generator {})]
;
;     (testing "health-checks if connections and channels are defined"
;       (.then (health/check {:q queue}) #(go (>! health %)))
;       (is (= {:result true :details {:q nil}}
;              (await! health))))
;
;     (testing "informs that channel is offline"
;       (-> @rabbit/connections :localhost (.then #(-> % second .close)))
;       (.then (health/check {:q queue}) #(go (>! health %)))
;       (is (= {:result false :details {:q {:queue "doesn't exist or error on connection"}}}
;              (await! health))))))
;
(def-async-test "Sending a message to a queue" {:teardown (rabbit/disconnect!)}
  (let [{:keys [send! results]} (subscribe-all!)]
    (send! {:payload {:some "msg"}})
    (is (= {:some "msg"}
           (:payload (await! results))))))

(def-async-test "logs that we're processing a message" {:teardown (rabbit/disconnect!)}
  (let [{:keys [send! logger results]} (subscribe-all!)
        _ (send! {:payload {:some "msg"}})
        {:keys [message cid type payload meta]} (await! logger)]
    (is (map? (await! results)))
    (is (= "Processing message" message))
    (is (= :info type))
    (is (re-matches #"FOO\.[\w\d]+" cid))
    (is (= "{\"some\":\"msg\"}" payload))
    (is (re-find #"\"queue\":\"test\"" meta))))

(def-async-test "attaches metadata into msg" {:teardown (rabbit/disconnect!)}
  (let [{:keys [send! results]} (subscribe-all!)
        _ (send! {:payload {:some "msg"}, :meta {:a 10}})
        meta (:meta (await! results))]
    (is (= 10 (:a meta)))
    (is (re-matches #"FOO\.[\w\d]+" (:cid meta)))))

(def-async-test "when message results in a failure process multiple times (till max-retries)"
  {:teardown (rabbit/disconnect!)}

  (testing "acks the original msg, and generates other to retry things"
    (let [{:keys [send! results all-messages deadletter]} (subscribe-all!)]
      (send! {:payload "error"} {:payload "msg"})
      (is (= "error" (await! all-messages)))
      (is (= "msg" (await! all-messages)))
      (is (map? (await! results)))
      (is (= "error" (await! all-messages)))
      (is (= "\"error\"" (await! deadletter))))))

#?(:clj
   (defn raw-send! [queue msg]
     (basic/publish (:channel queue) (:name queue) "" msg))
   :cljs
   (defn raw-send! [queue msg]
     (. (:channel queue) then
       #(. % publish (:name queue) "" (. js/Buffer from msg)))))

(def-async-test "sends message to deadletter if isn't in JSON format"
  {:teardown (rabbit/disconnect!)}

  (let [{:keys [queue deadletter]} (subscribe-all!)]
    (raw-send! queue "some-strange-msg")
    (is (= "some-strange-msg" (await! deadletter)))))

(defn is-closed? []
  (let [c (chan)]
    (go
     (while (not-empty @rabbit/connections)
       (<! (timeout 100)))
     (>! c true))
    c))

(def-async-test "don't process message if old server died (but mark to retry later)"
  {:teardown (rabbit/disconnect!)}

  (let [{:keys [send! results all-messages deadletter queue]} (subscribe-all!)]
    (send! {:payload "fatal"})
    (is (= "fatal" (first (await-all! [all-messages (timeout 1000)]))))
    (is (await! (is-closed?))))

  (let [{:keys [send! results all-messages deadletter queue]} (subscribe-all!)]
    (is (= "fatal" (first (await-all! [all-messages (timeout 1000)]))))
    (is (await! (is-closed?))))

  (let [{:keys [send! results all-messages deadletter]} (subscribe-all!)]
    (is (nil? (first (await-all! [all-messages (timeout 1000)]))))
    (is (= "\"fatal\"" (await! deadletter)))))

; Mocks
(defn a-function [test-q]
  (let [upcases #(cond-> % (string? %) clojure.string/upper-case)
        publish #(io/send! %2 {:payload %1})
        sub (components/subscribe-with :result-q (rabbit/queue "test-result")
                                       :test-q test-q)]

    (sub :test-q (fn [msg {:keys [result-q]}]
                   (->> msg
                        (future/map :payload)
                        (future/map upcases)
                        (future/intercept #(publish % result-q)))))))

#?(:clj
   (deftest when-mocking-RabbitMQ-queue
     (testing "subscribes correctly to messages"
       (components/mocked
         (a-function (rabbit/queue "test"))
         (io/send! (:test @mocks/queues) {:payload "message"})
         (is (= "MESSAGE"
                (-> @mocks/queues :test-result :messages deref last :payload)))))

     (testing "serializes message before sending it to mocked queue"
       (components/mocked
         (a-function (rabbit/queue "test"))
         (io/send! (:test @mocks/queues) {:payload {:dt #inst "2010-10-20T10:00:00Z"}})
         (is (= [{:dt "2010-10-20T10:00:00Z"}]
                (->> @mocks/queues :test :messages deref (map :payload))))))

     (testing "ignores delayed messages"
       (components/mocked
         (a-function (rabbit/queue "test" :delayed true))
         (io/send! (:test @mocks/queues) {:payload "msg one"})
         (io/send! (:test @mocks/queues) {:payload "msg two", :meta {:x-delay 400}})
         (is (= ["msg one"]
                (->> @mocks/queues :test :messages deref (map :payload)))))))
   :cljs
   (def-async-test "when mocking RabbitMQ queue" {}
     (let [channel (chan)
           done! #(go (>! channel :done))]
       (testing "subscribes correctly to messages"
         (components/mocked
           (a-function (rabbit/queue "test"))
           (.then (io/send! (:test @mocks/queues) {:payload "message"}) done!)
           (await! channel)
           (is (= "MESSAGE"
                  (-> @mocks/queues :test-result :messages deref last :payload)))))

       (testing "serializes message before sending it to mocked queue"
         (components/mocked
           (a-function (rabbit/queue "test"))
           (.then (io/send! (:test @mocks/queues)
                            {:payload {:dt #inst "2010-10-20T10:00:00Z"}})
                  done!)
           (await! channel)
           (is (= [{:dt "2010-10-20T10:00:00.000Z"}]
                  (->> @mocks/queues :test :messages deref (map :payload))))))

       (testing "ignores delayed messages"
         (components/mocked
           (a-function (rabbit/queue "test" :delayed true))
           (.then (io/send! (:test @mocks/queues) {:payload "msg one"}) done!)
           (await! channel)
           (.then (io/send! (:test @mocks/queues) {:payload "msg two",
                                                   :meta {:x-delay 400}})
                  done!)
           (await! channel)
           (is (= ["msg one"]
                  (->> @mocks/queues :test :messages deref (map :payload)))))))))
