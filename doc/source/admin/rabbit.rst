================================
RabbitMq Driver Deployment Guide
================================

Introduction
------------

The RabbitMQ Driver is a messaging transport backend
in *oslo.messaging*. The driver maps the base oslo.messaging
capabilities for notification message exchange onto the
RabbitMQ distributed message broker. More detail regarding
the RabbitMQ server is available from the `RabbitMQ website`__.

__ https://www.rabbitmq.com/


Abstract
--------

The RabbitMQ Driver is a messaging transport backend supported in
oslo.messaging. Communications between
the driver and RabbitMQ server backend uses the  `AMQP 0-9-1 protocol`_
(Advanced Message Queuing Protocol) which is an open application layer
that allows clients applications to communicate with messaging middleware
brokers in a standard way.
AMQP defines all APIs to request messages from a message queue or to publishes
messages to an exchange. The RabbitMQ driver integrates the kombu
python client for full protocol support and utilizes
the `Producer API`_ to publish notification
messages and the `Consumer API`_ for notification listener subscriptions.
The driver is able to work with a single instance of a RabbitMQ server or
a clustered RabbitMQ server deployment.

.. _AMQP 0-9-1 protocol: https://www.rabbitmq.com/protocol.html
.. _Consumer API: https://kombu.readthedocs.io/en/stable/userguide/consumers.html
.. _Producer API: https://kombu.readthedocs.io/en/stable/userguide/producers.html

Exchange
~~~~~~~~

`Exchange is a AMQP mechanism`_ that it designed to dispatch the messages
like a proxy wrapping.

Messages are always published to exchanges.

An exchange can:

- receives messages from producers
- push messages to `queues`_

Exchanges can distribute message copies to one or many queues using rules
called bindings.

AMQP protocol defines different types of exchanges:

- direct
- topic
- headers
- fanout

An exchange can live without any binding. By default, no exception is
raised if the message is not redirected to any queue, `unless the mandatory
flag is used`_.

*oslo.messaging* allow you to send and consume messages in a related manner
through the *Connection* class.

With mandatory flag RabbitMQ raises a callback if the message is not routed to
any queue.

.. _Exchange is a AMQP mechanism: https://www.rabbitmq.com/tutorials/amqp-concepts.html#exchanges
.. _queues: https://www.rabbitmq.com/queues.html
.. _unless the mandatory flag is used: https://www.rabbitmq.com/reliability.html#routing

Queue
~~~~~

The `AMQP queue`_ is the messages store, it can store the messages in memory
or persist the messages to the disk.

The queue is bound to the exchange through one or more bindings.

Consumers can consume messages from the queue.

Queues have names so that applications can reference them.

Queues have properties that define how they behave.

.. _AMQP queue: https://www.rabbitmq.com/tutorials/amqp-concepts.html#queues

Routing-Key
~~~~~~~~~~~

The routing key is part of the AMQP envelop of the message.
The routing key is set by the producer to route the sended message.
When a message is received, the exchange will try to match the
message routing-key with the binding key of all the queues bound to it.
If no match exist the message will be ignored else the message will be
routed to the corresponding queue who binding key is matched.

Exchange types
~~~~~~~~~~~~~~

`direct`
^^^^^^^^

A direct exchange is an exchange which route messages to queues based on
message routing key. Message will be directly delivered to the queue that
correspond to the routing-key.
*direct* is a type of exchange so RabbitMQ backend does not store the data.

``topic``
^^^^^^^^^

The RabbitMQ topic is an `exchange type`_.
In RabbitMQ messages sent to a `topic exchange`_ can't have an arbitrary
routing_key - it must be a list of words, delimited by dots.
The words can be anything, but usually they specify some features connected
to the message.
A few valid routing key examples: "blue.orange.yellow", "cars.bikes",
"quick.orange.rabbit".
There can be as many words in the routing key as you like,
up to the limit of 255 bytes..
In *oslo.messaging*, a notification listener subscribes to a topic
in a supplied target that is directly mapped by the driver to the
RabbitMQ topic.

.. _exchange type: https://www.rabbitmq.com/tutorials/tutorial-three-python.html
.. _topic exchange: https://www.rabbitmq.com/tutorials/tutorial-five-python.html

``fanout``
^^^^^^^^^^

The fanout exchange will broadcasts all messages it receives to all the queues
it knows.

``headers``
^^^^^^^^^^^

An headers exchange will route messages to queues by using message header
content instead of routing by using the routing key like described previously.
In this case producer will adds message header values as key-value pair in and
he will sends by using the headers exchange.

The exchange will try to match all or any (based on the value of “x-match”)
header value of the received message with the binding value of all
the queues bound to it.

The exchange will determine which header to use to match the binding queues by
using the value of the "x-match" header entry.

If match is not found, the message will be ignored else it route the message
to the queue whose binding value is matched.

Health check with heartbeat frames
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The RabbitMQ driver of *oslo.messaging* allow you to detect dead TCP
connections with heartbeats and TCP keepalives.
The heartbeat function from the driver is build over the `heartbeat_check
feature of kombu client`_ and over the `AMQP 0.9.1 heartbeat
feature`_ implemented by RabbitMQ.
Heartbeating is a technique designed to undo one of TCP/IP's features,
namely its ability to recover from abroken physical connection by closing
only after a quite long time-out.
In some scenarios we need to knowvery rapidly if a peer is disconnected or not
responding for other reasons (e.g. it is looping).
Since heart-beating can be done at a low level, AMQP 0.9.1
implement this as a special type of frame that peers exchange at the transport
level, rather than as a class method.
Heartbeats also defend against certain network equipment which may terminate
"idle" TCP connections when there is no activity on them for a certain
period of time.
The driver will always run the heartbeat in a native python thread and avoid
to inherit the execution model from the parent process to avoid to use green
threads.

.. _heartbeat_check feature of kombu client: http://docs.celeryproject.org/projects/kombu/en/stable/reference/kombu.html?highlight=heartbeat#kombu.Connection.heartbeat_check
.. _AMQP 0.9.1 heartbeat feature: https://www.rabbitmq.com/heartbeats.html

Prerequisites
-------------

In order to run the driver the kombu python client must be
installed. The RabbitMQ driver integrates a
`Python client based on kombu`_ and `on py-amqp`_
for full protocol support and utilizes the Producer API to publish
notification messages and the Consumer API for notification listener
subscriptions.

.. _Python client based on kombu: https://github.com/celery/kombu
.. _on py-amqp:  https://github.com/celery/py-amqp

Source packages for the `kombu library`_ are available via PyPI.
Since the RabbitMQ driver is not an optional extension to *oslo.messaging*
these packages installed by default.

.. _kombu library: https://pypi.org/project/kombu/

Configuration
-------------

Transport URL Enable
~~~~~~~~~~~~~~~~~~~~

In *oslo.messaging*, the ``transport_url`` parameter defines the OpenStack service
backends for RPC and notifications. The URL is of the form::

    transport://user:pass@host1:port[,hostN:portN]/virtual_host

Where the transport value specifies the RPC or notification backend as
one of ``amqp``, ``rabbit``, ``kafka``, etc. To specify and enable the
RabbitMQ driver for notifications, in the section
``[oslo_messaging_notifications]`` of the service configuration file,
specify the ``transport_url`` parameter::

  [oslo_messaging_notifications]
  transport_url = rabbit://username:password@kafkahostname:9092

Note, that if a ``transport_url`` parameter is not specified in the
``[oslo_messaging_notifications]`` section, the ``[DEFAULT] transport_url``
option will be used for both RPC and notifications backends.

Driver Options
~~~~~~~~~~~~~~

It is recommended that the default configuration options provided by
the RabbitMQ driver be used. The configuration options can be modified
in the :oslo.config:group:`oslo_messaging_rabbit` section of the service
configuration file.

Publishing Options
^^^^^^^^^^^^^^^^^^

- :oslo.config:option:`oslo_messaging_rabbit.kombu_compression`
- :oslo.config:option:`oslo_messaging_rabbit.kombu_missing_consumer_retry_timeout`

Consuming Options
^^^^^^^^^^^^^^^^^

- :oslo.config:option:`oslo_messaging_rabbit.rabbit_ha_queues`
- :oslo.config:option:`oslo_messaging_rabbit.rabbit_transient_queues_ttl`

Connection Options
^^^^^^^^^^^^^^^^^^

- :oslo.config:option:`oslo_messaging_rabbit.kombu_reconnect_delay`
- :oslo.config:option:`oslo_messaging_rabbit.kombu_failover_strategy`
- :oslo.config:option:`oslo_messaging_rabbit.rabbit_retry_interval`
- :oslo.config:option:`oslo_messaging_rabbit.rabbit_retry_backoff`
- :oslo.config:option:`oslo_messaging_rabbit.rabbit_interval_max`
- :oslo.config:option:`oslo_messaging_rabbit.rabbit_qos_prefetch_count`

Heartbeat Options
^^^^^^^^^^^^^^^^^

- :oslo.config:option:`oslo_messaging_rabbit.heartbeat_timeout_threshold`
- :oslo.config:option:`oslo_messaging_rabbit.heartbeat_rate`

Security Options
^^^^^^^^^^^^^^^^

- :oslo.config:option:`oslo_messaging_rabbit.ssl`
- :oslo.config:option:`oslo_messaging_rabbit.ssl_version`
- :oslo.config:option:`oslo_messaging_rabbit.ssl_key_file`
- :oslo.config:option:`oslo_messaging_rabbit.ssl_cert_file`
- :oslo.config:option:`oslo_messaging_rabbit.rabbit_login_method`
