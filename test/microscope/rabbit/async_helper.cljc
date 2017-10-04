#?(:cljs
    (ns microscope.rabbit.async-helper
      (:require-macros [cljs.core.async.macros])
                      ;  [expectations :refer [expect]])
      (:require [cljs.core.async :refer [close! chan >! <! alts!]]
                [clojure.test :refer-macros [deftest async testing]]
                ; [cljs.core.async.macros :include-macros true]
                [clojure.string :as str]))
   :clj
    (ns microscope.rabbit.async-helper
      (:require [clojure.core.async :refer [close! chan >! <! go]]
                [clojure.test :refer [deftest testing]]
                [clojure.string :as str])))

(defmacro await! [chan] `(cljs.core.async/<! ~chan))
(defmacro await-all! [chans] `(cljs.core.async/alts! ~chans))

(defmacro def-async-test [description opts & cmds]
  (assert (map? opts) "second parameter must be a map")
  (let [norm-desc (symbol (-> description
                              (str/replace #"\s" "-")
                              (str/replace #"[^\-\w\d]" "")))]
    `(deftest ~norm-desc
       (cljs.test/async done#
         (cljs.core.async.macros/go
          (let [mark-as-done# (delay
                               ~(if-let [teardown (:teardown opts)]
                                  teardown)
                               (done#))]
            (testing ~description
              (js/setTimeout (fn []
                               (when-not (realized? mark-as-done#)
                                 (cljs.test/is (throw "Async test error - not finalized"))
                                 @mark-as-done#))
                             3000)
              ~@cmds
              @mark-as-done#)))))))
