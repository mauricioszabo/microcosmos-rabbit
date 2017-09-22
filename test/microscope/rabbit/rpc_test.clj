(ns microscope.rabbit.rpc-test
  (:require [midje.sweet :refer :all]
            [microscope.rabbit.rpc :as rpc]
            [microscope.io :as io]
            [microscope.rabbit.queue :as rabbit]
            [microscope.rabbit.mocks :as mocks]
            [microscope.future :as future]
            [microscope.core :as components]))

(fact "will create a RPC client"
  (let [sub (components/subscribe-with :increment (rpc/queue "increment"))
        handler (fn [f-msg {:keys [increment]}]
                  (->> f-msg
                       (future/map (comp inc :payload))
                       (future/intercept #(io/send! increment {:payload %}))))
        rpc-constructor (rpc/caller "increment")
        rpc (rpc-constructor {:cid "FOOBAR"})]

    (sub :increment handler)

    (fact "will call RPC function"
      (rpc 20) => 21))
  (background
   (after :facts (rabbit/disconnect!))))

(facts "when mocking server"
  (components/mocked
    (let [sub (components/subscribe-with :rpc-server (rpc/queue "rpc"))]
      (sub :rpc-server (fn [f-val {:keys [rpc-server]}]
                         (->> f-val
                              (future/map :payload)
                              (future/intercept #(io/send! rpc-server
                                                           {:payload (inc %)})))))

      (fact "will publish to a mocked response queue"
        (mocks/rpc-call :rpc 10)
        (-> @mocks/queues :rpc-response :messages deref last :payload) => 11))))

(fact "when mocking client"
  (components/mocked
    {:rpc-responses {:rpc #(+ 2 %)}}
    (let [sub (components/subscribe-with :caller (rpc/caller "rpc")
                                         :queue (rabbit/queue "some-queue")
                                         :response (rabbit/queue "response"))]
      (sub :queue (fn [f-value {:keys [response caller]}]
                    (->> f-value
                         (future/map #(caller (:payload %)))
                         (future/intercept #(io/send! response {:payload %})))))

      (fact "calls mocked remove function"
        (io/send! (:some-queue @mocks/queues) {:payload 90}))
      (-> @mocks/queues :response :messages deref last :payload) => 92)))
