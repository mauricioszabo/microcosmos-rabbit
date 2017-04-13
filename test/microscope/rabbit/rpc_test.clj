(ns microscope.rabbit.rpc-test
  (:require [midje.sweet :refer :all]
            [microscope.rabbit.rpc :as rpc]
            [microscope.io :as io]
            [microscope.rabbit.queue :as rabbit]
            [microscope.future :as future]
            [microscope.core :as components]))

(fact "will create a RPC client"
  (let [subs (components/subscribe-with :increment (rpc/queue "increment"))
        handler (fn [f-msg {:keys [increment]}]
                  (->> f-msg
                       (future/map (comp inc :payload))
                       (future/intercept #(io/send! increment {:payload %}))))
        rpc-constructor (rpc/caller "increment")
        rpc (rpc-constructor {:cid "FOOBAR"})]

    (alter-var-root #'rabbit/rabbit-config
                    (constantly {:queues {:increment "127.0.0.1"}
                                 :hosts nil}))
    (subs :increment handler)

    (fact "will call RPC function"
      (rpc 20) => 21))
  (background
   (after :facts (rabbit/disconnect!))))
