(ns microscope.rabbit.node-js
  (:require [microscope.io :as io]
            [microscope.healthcheck :as health]
            [microscope.core :as components]
            [microscope.rabbit.queue :as rabbit]
            [microscope.future :as future]
            [microscope.logging :as log]
            [clojure.walk :as walk]
            [cljs.nodejs :as nodejs]))

(nodejs/enable-util-print!)

(defn- clj-map [map] (-> map js->clj walk/keywordize-keys))
(defn- flatten-map [map] (-> map clj-map seq flatten))

(defn- decorate-component [component]
  (let [js-component (clj->js component)]
    (when (satisfies? io/IO component)
      (aset js-component "send" (fn [msg] (io/send! component (clj-map msg)))))

    (when (satisfies? log/Log component)
      (aset js-component "info"
            (fn [msg data] (apply log/info component msg (flatten-map data))))
      (aset js-component "warning"
            (fn [msg data] (apply log/warning component msg (flatten-map data))))
      (aset js-component "error"
            (fn [msg data] (apply log/error component msg (flatten-map data))))
      (aset js-component "fatal"
            (fn [msg data] (apply log/fatal component msg (flatten-map data)))))

    js-component))

(defn subscribe-with [components]
  (let [subscribe (apply components/subscribe-with (flatten-map components))]
    (fn [component callback]
      (subscribe (keyword component)
                 (fn [f-msg components]
                   (callback (future/map clj->js f-msg)
                             (->> components
                                  (map (fn [[k v]] [k (decorate-component v)]))
                                  (into {})
                                  clj->js)))))))

(defn- js->clj->js [cljs-function]
  (fn [ & args]
    (->> args
         (map js->clj)
         (apply cljs-function)
         clj->js)))

(aset js/module "exports"
      #js {:subscribeWith subscribe-with
           :rabbit #js {:queue (js->clj->js rabbit/queue)}
           :str str
           :io io/IO
           :Queue rabbit/Queue
           :to_js clj->js
           :to_clj js->clj})
