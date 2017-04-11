# Microscope's RabbitMQ

Support for RabbitMQ for Microscope.

RabbitMQ's implementation implements Microscope's IO and Healthcheck interface. Its `ack`
is a simple ACK, and `reject` will try requeue the message for a number of times,
configurable by `:max-retries` parameter, after which it'll just reject it and send it to
a deadletter.

## Mocked version

A mocked version of the queue is simply a group of atoms in the Clojure. They will listen
to each other, and publish/subscribe exactly the same as a real queue, but no Rabbit
installation is needed. It is possible to check the queue's states on `queue` atom, from
`microscope.rabbit.queue` namespace. Notice that this atom is only populated on a mocked
environment

## Usage

```clojure
(require '[microscope.core :as components]
         '[microscope.future :as future]
         '[microscope.queue.rabbit :as rabbit])


(defn increment [n] (inc n))

(defn publish-to [queue result]
  (components/send! queue {:payload result}))

(defn main- [ & args]
  (let [subscribe (subscribe-with :result (rabbit/queue "results")
                                  :numbers (rabbit/queue "numbers")])
    (subscribe :numbers
               (fn [future-n components]
                 (->> future-n
                      (future/map :payload)
                      (future/map increment)
                      (future/map #(publish-to (:result components) %)))))))
```

## Configuration

RabbitMQ's configuration is made entirely of two environment variables. First one
is named `RABBIT_CONFIG`, which will configure the hosts (with aliases) where Rabbit is
installed. Second is `RABBIT_QUEUES`, which will configure where each queue name is
located. So, let's say we have rabbit installed in two machines, one in `localhost`
and another in `192.168.0.30`. Then, we have three queues, one in `localhost`, and two
on this secondary machine. The environment variables' configuration will look like this:

```
RABBIT_CONFIG='{"local-machine":{"host":"localhost"},"remote":{"host":"192.168.0.30","port":1337,"username":"foobar","password":"SuperSecretPassword"}}'
RABBIT_QUEUES='{"numbers":"local-machine","results":"remote","other-result":"remote"}'
```

The complete list of possible parameters is located at:
http://clojurerabbitmq.info/articles/connecting.html

## MIT License

Copyright 2017 AcessoCard

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
