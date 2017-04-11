(ns microscope.rabbit.queue-test
  (:require [microscope.core :as components]
            [microscope.io :as io]
            [microscope.healthcheck :as health]
            [microscope.future :as future]
            [microscope.rabbit.queue :as rabbit]
            [microscope.logging :as log]
            [cheshire.core :as json]
            [langohr.core :as core]
            [midje.sweet :refer :all]))

(def all-msgs (atom []))
(def all-processed (atom []))
(def all-deadletters (atom []))
(def last-promise (atom (promise)))

(defn- send-msg [fut-value {:keys [result-q]}]
  (future/map (fn [value]
                (swap! all-msgs conj value)
                (case (:payload value)
                  "error" (throw (Exception. "Some Error"))
                  (io/send! result-q value)))
              fut-value))

(def logger (reify log/Log (log [_ _ type _] nil)))

(defn in-future [f]
  (fn [future _]
    (future/map f future)))

(defn send-messages [msgs]
  (let [test-queue (rabbit/queue "test" :auto-delete true :max-retries 1)
        result-queue (rabbit/queue "test-result" :auto-delete true)
        channel (:channel (result-queue {}))
        deadletter-queue (fn [_] (rabbit/->Queue channel "test-deadletter" 1000 "FOO"))
        sub (components/subscribe-with :result-q result-queue
                                       :logger (fn [_] logger)
                                       :test-queue test-queue
                                       :result-queue result-queue
                                       :deadletter-queue deadletter-queue)]
    (sub :test-queue send-msg)
    (sub :result-queue (in-future #(do
                                     (swap! all-processed conj %)
                                     (when (realized? @last-promise)
                                       (reset! last-promise (promise)))
                                     (deliver @last-promise %))))
    (sub :deadletter-queue (in-future #(swap! all-deadletters conj %)))
    (doseq [msg msgs]
      (io/send! (test-queue {:cid "FOO"}) msg))))

(defn send-and-wait [ & msgs]
  (send-messages msgs)
  (deref @last-promise 1000 {:payload :timeout
                             :meta :timeout}))

(defn prepare-tests []
  (reset! last-promise (promise))
  (reset! all-msgs [])
  (reset! all-processed [])
  (reset! all-deadletters []))

(facts "Handling healthchecks"
  (let [q-generator (rabbit/queue "test" :auto-delete true)]
    (fact "health-checks if connections and channels are defined"
      (health/check {:q (q-generator {})}) => {:result true :details {:q nil}})

    (fact "informs that channel is offline"
      (let [queue (q-generator {})]
        (update-in @rabbit/connections [:localhost 1] core/close)
        (health/check {:q queue})) => {:result false
                                       :details {:q {:channel "is closed"}}}
      (against-background
       (after :facts (do
                       (update-in @rabbit/connections [:localhost 0] core/close)
                       (reset! rabbit/connections {})))))))

(facts "Handling messages on RabbitMQ's queue"
  (fact "handles message if successful"
    (:payload (send-and-wait {:payload {:some "msg"}})) => {:some "msg"})

  (fact "attaches metadata into msg"
    (:meta (send-and-wait {:payload {:some "msg"}, :meta {:a 10}}))
    => (contains {:a 10, :cid "FOO.BAR"}))

  (fact "attaches CID between services"
    (get-in (send-and-wait {:payload "msg"}) [:meta :cid]) => "FOO.BAR")

  (against-background
   (components/generate-cid nil) => "FOO"
   (components/generate-cid "FOO") => "FOO.BAR"
   (components/generate-cid "FOO.BAR") => ..irrelevant..
   (before :facts (prepare-tests))
   (after :facts (rabbit/disconnect!))))

; OH MY GOSH, how difficult is to test asynchronous code!
(fact "when message results in a failure process multiple times (till max-retries)"
  (fact "acks the original msg, and generates other to retry things"
    (:payload (send-and-wait {:payload "error"} {:payload "msg"})) => "msg"
    (reset! last-promise (promise))
    (:payload (send-and-wait {:payload "other-msg"})) => "other-msg"
    (map :payload @all-deadletters) => ["error"]
    (map :payload @all-msgs) => ["error" "msg" "error" "other-msg"])

  (future-fact "sends message to deadletter if isn't in JSON format")
  (future-fact "don't process anything if old server died (but mark to retry later)")

  (against-background
    (before :facts (prepare-tests))
    (after :facts (rabbit/disconnect!))))

; Mocks
(defn a-function [test-q]
  (let [extract-payload :payload
        upcases #(clojure.string/upper-case %)
        publish #(io/send! %2 {:payload %1})
        sub (components/subscribe-with :result-q (rabbit/queue "test-result" :auto-delete true)
                                       :logger (fn [_] logger)
                                       :test-q test-q)]

    (sub :test-q (fn [msg {:keys [result-q]}]
                   (->> msg
                        (future/map extract-payload)
                        (future/map upcases)
                        (future/map #(publish % result-q)))))))

(facts "when mocking RabbitMQ's queue"
  (fact "subscribes correctly to messages"
    (components/mocked
      (a-function (rabbit/queue "test"))
      (io/send! (:test @rabbit/queues) {:payload "message"})
      (-> @rabbit/queues :test-result :messages deref)
      => (just [(contains {:payload "MESSAGE"})])))

  (fact "ignores delayed messages"
    (components/mocked
      (a-function (rabbit/queue "test" :delayed true))
      (io/send! (:test @rabbit/queues) {:payload "msg one"})
      (io/send! (:test @rabbit/queues) {:payload "msg two", :meta {:x-delay 400}})
      (-> @rabbit/queues :test-result :messages deref)
      => (just [(contains {:payload "MSG ONE"})]))))
