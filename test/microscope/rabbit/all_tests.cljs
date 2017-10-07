(ns microscope.rabbit.all-tests
  (:require [cljs.nodejs :as nodejs]
            [microscope.rabbit.queue-test]
            [clojure.test :refer [run-tests]]))

(nodejs/enable-util-print!)

(run-tests 'microscope.rabbit.queue-test)
