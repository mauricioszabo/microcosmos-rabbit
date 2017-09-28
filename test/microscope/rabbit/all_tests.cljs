(ns microscope.rabbit.all-tests
  (:require [cljs.nodejs :as nodejs]))

(nodejs/enable-util-print!)

(require 'microscope.rabbit.queue-test)
