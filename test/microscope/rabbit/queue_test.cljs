(ns microscope.rabbit.queue-test
  (:require-macros [cljs.core.async.macros :refer [go]])
  (:require [clojure.test :refer-macros [deftest is testing run-tests async] :as tst]
            [microscope.core :as components]
            [microscope.io :as io]
            [microscope.healthcheck :as health]
            [microscope.future :as future]
            [microscope.rabbit.queue :as rabbit]
            [cljs.core.async :refer [close! chan >! <!]]
            [microscope.rabbit.async-helper :refer-macros [def-async-test await!]]
;             [microscope.rabbit.mocks :as mocks]
            [microscope.logging :as log]
            [microscope.rabbit.async-helper :as helper]))


(defn in-future [f]
  (fn [future _] (future/map f future)))

(defn subscribe-all! []
  (let [test-queue (rabbit/queue "test" :auto-delete true :max-retries 1)
        result-queue (rabbit/queue "test-result" :auto-delete true)
        channel (:channel (result-queue {}))
        deadletter-queue (fn [_] (rabbit/->Queue channel "test-deadletter" 1000 "FOO"))
        logger-chan (chan)
        all-msgs-chan (chan)
        logger-gen (fn [{:keys [cid]}]
                     (reify log/Log
                       (log [_ msg type data]
                         (go (>! logger-chan (assoc data
                                                    :message msg
                                                    :type type
                                                    :cid cid))))))
        sub (components/subscribe-with :result-q result-queue
                                       :logger logger-gen
                                       :test-queue test-queue
                                       :result-queue result-queue
                                       :deadletter-queue deadletter-queue)
        send-msg (fn [msg {:keys [result-q]}]
                   (future/map (fn [value]
                                 (go (>! all-msgs-chan [:msg (:payload value)]))
                                 (case (:payload value)
                                   "error" (throw (js/Error. "Some Error"))
                                   (io/send! result-q value)))
                               msg))
        msgs-chan (chan)
        deadletter-chan (chan)]

    (sub :test-queue send-msg)
    (sub :result-queue (in-future #(do
                                     (go (>! all-msgs-chan [:alive (:payload %)]))
                                     (go (>! msgs-chan %)))))
    (sub :deadletter-queue (in-future #(do
                                         (go (>! all-msgs-chan [:dead (:payload %)]))
                                         (go (>! deadletter-chan %)))))
    {:send! (fn [ & msgs] (doseq [m msgs] (io/send! (test-queue {:cid "FOO"}) m)))
     :messages msgs-chan
     :all-messages all-msgs-chan
     :deadletter deadletter-chan
     :logger logger-chan}))

(def-async-test "Handling healthchecks" {:teardown (rabbit/disconnect!)}
  (let [health (chan)
        q-generator (rabbit/queue "test" :auto-delete true)
        queue (q-generator {})]

    (testing "health-checks if connections and channels are defined"
      (.then (health/check {:q queue}) #(go (>! health %)))
      (is (= {:result true :details {:q nil}}
             (await! health))))

    (testing "informs that channel is offline"
      (-> @rabbit/connections :localhost (.then #(-> % second .close)))
      (.then (health/check {:q queue}) #(go (>! health %)))
      (is (= {:result false :details {:q {:queue "doesn't exist or error on connection"}}}
             (await! health))))))

(def-async-test "Sending a message to a queue" {:teardown (rabbit/disconnect!)}
  (let [{:keys [send! messages]} (subscribe-all!)]
    (send! {:payload {:some "msg"}})
    (is (= {:some "msg"}
           (:payload (await! messages))))))

(def-async-test "logs that we're processing a message" {:teardown (rabbit/disconnect!)}
  (let [{:keys [send! logger messages]} (subscribe-all!)
        _ (send! {:payload {:some "msg"}})
        {:keys [message cid type payload meta]} (await! logger)]
    (is (map? (await! messages)))
    (is (= "Processing message" message))
    (is (= :info type))
    (is (re-matches #"FOO\.[\w\d]+" cid))
    (is (= "{\"some\":\"msg\"}" payload))
    (is (re-find #"\"queue\":\"test\"" meta))))

  ; (fact "attaches metadata into msg"
  ;   (:meta (send-and-wait {:payload {:some "msg"}, :meta {:a 10}}))
  ;   => (contains {:a 10, :cid "FOO.BAR"}))
  ;
  ; (fact "attaches CID between services"
  ;   (get-in (send-and-wait {:payload "msg"}) [:meta :cid]) => "FOO.BAR")
  ;
  ; (against-background
  ;  (components/generate-cid nil) => "FOO"
  ;  (components/generate-cid "FOO") => "FOO.BAR"
  ;  (components/generate-cid "FOO.BAR") => ..cid..
  ;  (before :facts (prepare-tests))
  ;  (after :facts (rabbit/disconnect!))))

(def-async-test "when message results in a failure process multiple times (till max-retries)"
  {:teardown (rabbit/disconnect!)}

  (testing "acks the original msg, and generates other to retry things"
    (let [{:keys [send! all-messages]} (subscribe-all!)]
      (send! {:payload "error"} {:payload "msg"})
      (is (= [:msg "error"] (await! all-messages)))
      (is (= [:msg "msg"] (await! all-messages)))
      (is (= [:alive "msg"] (await! all-messages)))
      (is (= [:msg "error"] (await! all-messages)))
      (is (= [:dead "error"] (await! all-messages))))))

(run-tests)
