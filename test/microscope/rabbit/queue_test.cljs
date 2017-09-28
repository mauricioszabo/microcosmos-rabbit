(ns microscope.rabbit.queue-test
  (:require [clojure.test :refer-macros [deftest is testing run-tests async]]
            [microscope.core :as components]
            [microscope.io :as io]
;             [microscope.healthcheck :as health]
            [microscope.future :as future]
            [microscope.rabbit.queue :as rabbit]
;             [microscope.rabbit.mocks :as mocks]
            [microscope.logging :as log]))
;             [cheshire.core :as json]
;             [langohr.core :as core]
;             [midje.sweet :refer :all]))

(def all-msgs (atom []))
(def all-processed (atom []))
(def all-deadletters (atom []))
(def last-promise (atom nil))

(defn- send-msg [fut-value {:keys [result-q]}]
  (future/map (fn [value]
                (swap! all-msgs conj value)
                (case (:payload value)
                  "error" (throw (js/Error. "Some Error"))
                  (io/send! result-q value)))
              fut-value))

(def logger-msgs (atom nil))
(defn logger-gen [{:keys [cid]}]
  (reset! logger-msgs [])
  (reify log/Log
    (log [_ msg type data]
      (swap! logger-msgs conj (assoc data :message msg :type type :cid cid)))))

(defn in-future [f]
  (fn [future _] (future/map f future)))

; (defn send-messages [msgs]
;   (let [test-queue (rabbit/queue "test" :auto-delete true :max-retries 1)
;         result-queue (rabbit/queue "test-result" :auto-delete true)
;         channel (:channel (result-queue {}))
;         deadletter-queue (fn [_] (rabbit/->Queue channel "test-deadletter" 1000 "FOO"))
;         sub (components/subscribe-with :result-q result-queue
;                                        :logger logger-gen
;                                        :test-queue test-queue
;                                        :result-queue result-queue
;                                        :deadletter-queue deadletter-queue)]
;     (sub :test-queue send-msg)
;     (sub :result-queue (in-future #(do
;                                      (swap! all-processed conj %)
;                                      (when (realized? @last-promise)
;                                        (reset! last-promise (promise)))
;                                      (deliver @last-promise %))))
;     (sub :deadletter-queue (in-future #(swap! all-deadletters conj %)))
;     (doseq [msg msgs]
;       (io/send! (test-queue {:cid "FOO"}) msg))))
;

; (defn- realize-or-deliver [msg]
;   (when (realized? @last-promise)
;     (reset! last-promise (promise)))
;   (deliver @last-promise %))

; CLJS version
(defn prepare-last-msg
  ([])
  ([msg] (reset! last-promise (future/just msg))))

; CLJS version
(defn wait-for-last-message [f]
  (.then @last-promise) f)

(defn send-and-wait [ & msgs]
  (let [test-queue (rabbit/queue "test" :auto-delete true :max-retries 1)
        result-queue (rabbit/queue "test-result" :auto-delete true)
        channel (:channel (result-queue {}))
        deadletter-queue (fn [_] (rabbit/->Queue channel "test-deadletter" 1000 "FOO"))
        sub (components/subscribe-with :result-q result-queue
                                       :logger logger-gen
                                       :test-queue test-queue
                                       :result-queue result-queue
                                       :deadletter-queue deadletter-queue)]


    ; Prepares promise/something to receive the last message. This will
    ; be used to "wait"
    (prepare-last-msg)
    ; Subscribe to the queue we'll send messages. This will deliver to
    ; deadletter if message is "error"
    (sub :test-queue send-msg)

    ; If message is not an error, deliver all processed and prepare last-msg
    (sub :result-queue (in-future #(do
                                     (swap! all-processed conj %)
                                     (prepare-last-msg %))))
    ; If message IS an error, deliver to all-deadletters
    (sub :deadletter-queue (in-future #(swap! all-deadletters conj %)))
    ; Send messages, at last!
    (doseq [msg msgs] (io/send! (test-queue {:cid "FOO"}) msg))))

; (defn send-and-wait [ & msgs]
;   (send-messages msgs)
;   (deref @last-promise 1000 {:payload :timeout
;                              :meta :timeout}))
;
; (defn prepare-tests []
;   (reset! last-promise (promise))
;   (reset! all-msgs [])
;   (reset! all-processed [])
;   (reset! all-deadletters []))
;
; (facts "Handling healthchecks"
;   (let [q-generator (rabbit/queue "test" :auto-delete true)]
;     (fact "health-checks if connections and channels are defined"
;       (health/check {:q (q-generator {})}) => {:result true :details {:q nil}})
;
;     (fact "informs that channel is offline"
;       (let [queue (q-generator {})]
;         (-> @rabbit/connections :localhost second core/close)
;         (health/check {:q queue})) => {:result false
;                                        :details {:q {:channel "is closed"}}}
;       (against-background
;        (after :facts (do
;                        (-> @rabbit/connections :localhost first core/close)
;                        (reset! rabbit/connections {})))))))
;
(deftest rabbitmq-queue-handling
  (testing "handles message if successful"
    (is (= {:some "msg"}
           (:payload (send-and-wait {:payload {:some "msg"}}))))))
;
;   (facts "logs that we're processing a message"
;     (send-and-wait {:payload {:some "msg"}})
;     (first @logger-msgs) => (just {:message "Processing message"
;                                    :type :info
;                                    :cid ..cid..
;                                    :payload "{\"some\":\"msg\"}"
;                                    :meta #"\"queue\":\"test-result\""}))
;
;   (fact "attaches metadata into msg"
;     (:meta (send-and-wait {:payload {:some "msg"}, :meta {:a 10}}))
;     => (contains {:a 10, :cid "FOO.BAR"}))
;
;   (fact "attaches CID between services"
;     (get-in (send-and-wait {:payload "msg"}) [:meta :cid]) => "FOO.BAR")
;
;   (against-background
;    (components/generate-cid nil) => "FOO"
;    (components/generate-cid "FOO") => "FOO.BAR"
;    (components/generate-cid "FOO.BAR") => ..cid..
;    (before :facts (prepare-tests))
;    (after :facts (rabbit/disconnect!))))
;
; ; OH MY GOSH, how difficult is to test asynchronous code!
; (fact "when message results in a failure process multiple times (till max-retries)"
;   (fact "acks the original msg, and generates other to retry things"
;     (:payload (send-and-wait {:payload "error"} {:payload "msg"})) => "msg"
;     (reset! last-promise (promise))
;     (:payload (send-and-wait {:payload "other-msg"})) => "other-msg"
;     (map :payload @all-deadletters) => ["error"]
;     (map :payload @all-msgs) => ["error" "msg" "error" "other-msg"])
;
;   (future-fact "sends message to deadletter if isn't in JSON format")
;   (future-fact "don't process anything if old server died (but mark to retry later)")
;
;   (against-background
;     (before :facts (prepare-tests))
;     (after :facts (rabbit/disconnect!))))
;
; (facts "multi-routing messages"
;   (let [p1 (promise)
;         p2 (promise)
;         first-q (rabbit/queue "first-queue" :auto-delete true :route-to ["second-q"
;                                                                          "third-q"])
;         second-q (rabbit/queue "second-q" :auto-delete true)
;         third-q (rabbit/queue "third-q" :auto-delete true)
;         sub (components/subscribe-with :first-q first-q
;                                        :second-q second-q
;                                        :third-q third-q)]
;     (sub :second-q (fn [f-msg _] (future/map #(deliver p1 (:payload %)) f-msg)))
;     (sub :third-q (fn [f-msg _] (future/map #(deliver p2 (:payload %)) f-msg)))
;
;     (io/send! (first-q {:cid "FOO"}) {:payload "some-msg"})
;     (deref p1 500 :TIMEOUT) => "some-msg"
;     (deref p2 500 :TIMEOUT) => "some-msg")
;
;   (against-background
;     (after :facts (rabbit/disconnect!))))
;
; ; Mocks
; (defn a-function [test-q]
;   (let [extract-payload :payload
;         upcases #(clojure.string/upper-case %)
;         publish #(io/send! %2 {:payload %1})
;         sub (components/subscribe-with :result-q (rabbit/queue "test-result" :auto-delete true)
;                                        :logger logger-gen
;                                        :test-q test-q)]
;
;     (sub :test-q (fn [msg {:keys [result-q]}]
;                    (->> msg
;                         (future/map extract-payload)
;                         (future/map upcases)
;                         (future/map #(publish % result-q)))))))
;
; (facts "when mocking RabbitMQ's queue"
;   (fact "subscribes correctly to messages"
;     (components/mocked
;       (a-function (rabbit/queue "test"))
;       (io/send! (:test @mocks/queues) {:payload "message"})
;       (-> @mocks/queues :test-result :messages deref)
;       => (just [(contains {:payload "MESSAGE"})])))
;
;   (fact "serializes message before sending it to mocked queue"
;     (components/mocked
;       (a-function (rabbit/queue "test"))
;       (io/send! (:test @mocks/queues) {:payload {:dt #inst "2010-10-20T10:00:00Z"}})
;       (->> @mocks/queues :test :messages deref (map :payload))
;       => [{:dt "2010-10-20T10:00:00Z"}]))
;
;   (fact "ignores delayed messages"
;     (components/mocked
;       (a-function (rabbit/queue "test" :delayed true))
;       (io/send! (:test @mocks/queues) {:payload "msg one"})
;       (io/send! (:test @mocks/queues) {:payload "msg two", :meta {:x-delay 400}})
;       (-> @mocks/queues :test-result :messages deref)
;       => (just [(contains {:payload "MSG ONE"})]))))

(run-tests)
