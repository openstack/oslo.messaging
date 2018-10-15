-----------------------------
Kafka Driver Deployment Guide
-----------------------------

============
Introduction
============

The Kafka Driver is an experimental messaging transport backend
in *oslo.messaging*. The driver maps the base oslo.messaging
capabilities for notification message exchange onto V2.0 of the
Apache Kafka distributed streaming platform. More detail regarding
the Apache Kafka server is available from the `Apache Kafka website`__.

__ https://kafka.apache.org/

More detail regarding the driver's implementation is available from
the `adding kafka driver specification`_ and the `update kafka driver
specification`_ .

.. _adding kafka driver specification: https://git.openstack.org/cgit/openstack/oslo-specs/tree/specs/liberty/adding-kafka-support.rst
.. _update kafka driver specification: https://git.openstack.org/cgit/openstack/oslo-specs/tree/specs/queens/update-kafka-support.rst

========
Overview
========

The Kafka driver **only** supports use for sending and receiving
*oslo.messaging* notifications. Specifically, the Kafka driver
does not support *oslo.messaging* RPC transfers. Communications between
the driver and Kafka server backend uses a `binary protocol over TCP`_
that defines all APIs as request response message pairs. The Kafka
driver integrates the confluent kafka python client for full
protocol support and utilizes the Producer API to publish notification
messages and the Consumer API for notification listener subscriptions.
The driver is able to work with a single instance of a Kafka server or
a clustered Kafka server deployment.

.. _binary protocol over TCP: https://kafka.apache.org/protocol.html

Hybrid Messaging Deployment
---------------------------

*Oslo.messaging* provides a mechanism to configure separate messaging
backends for RPC and notification communications. This is supported
through the definition of separate RPC and notification
`transport urls`_ in the service configuration. When the Kafka driver
is deployed for *oslo.messaging* notifications, a separate driver and
messaging backend must be deployed for RPC communications. For these
hybrid messaging configurations, either the `rabbit`_ or `amqp`_
drivers can be deployed for *oslo.messaging* RPC.

.. _transport urls: https://docs.openstack.org/oslo.messaging/latest/reference/transport.html
.. _rabbit: https://docs.openstack.org/oslo.messaging/latest/admin/drivers.html#rabbit
.. _amqp: https://docs.openstack.org/oslo.messaging/latest/admin/AMQP1.0.html

Topics and vhost Support
------------------------

The Kafka topic is the feed name to which records are
published. Topics in Kafka are multi-subscriber such that a topic can
have zero, one or many consumers that subscribe to the data written to
it. In *oslo.messaging*, a notification listener subscribes to a topic
in a supplied target that is directly mapped by the driver to the
Kafka topic. The Kafka server architecture does not natively support
vhosts. In order to support the presence of a vhost in the transport
url provided to the driver, the topic created on the Kafka server will
be appended with the virtual host name. This creates a unique topic
per virtual host but **note** there is otherwise no access
control or isolation provided by the Kafka server.

Listener Pools
--------------

The Kafka driver provides support for listener pools. This capability
is realized by mapping the listener pool name to a Kafka server
*consumer group* name. Each record published to a topic will be
delivered to one consumer instance within each subscribing pool
(e.g. *consumer group*). If a listener pool name is not assigned to the
notification listener, a single default *consumer group* will be used by
the Kafka driver and all listeners will be assigned to that
group and the messages will effectively be load balanced across the
competing listener instances.


Synchronous Commit
------------------

A primary functional difference between a Kafka server and a
classic broker queue is that the offset or position of the
message read from the commit log is controlled by the listener
(e.g. consumer). The driver will advance the offset it maintains
linearly as it reads message records from the server. To ensure that
duplicate messages are not generated during downtime or communication
interruption, the driver will *synchronously* commit the consumed
messages prior to the notification listener dispatch. Due to this, the
driver does not support the re-queue operation and the driver can not
replay messages from a Kafka partition.

=============
Prerequisites
=============

In order to run the driver the confluent-kafka python client must be
installed. The Kafka driver integrates a `Python client based on librdkafka`_
for full protocol support and utilizes the Producer API to publish
notification messages and the Consumer API for notification listener
subscriptions.

.. _Python client based on librdkafka: https://github.com/confluentinc/confluent-kafka-python

Source packages for the `confluent-kafka library`_ are available via PyPI.
Since the Kafka driver is an optional extension to *oslo.messaging*
these packages are not installed by default.  Use the 'kafka' extras
tag when installing *oslo.messaging* in order to pull in these extra
packages:

::

   python -m pip install oslo.messaging[kafka]


.. _confluent-kafka library: https://pypi.org/project/confluent-kafka/

=============
Configuration
=============

Transport URL Enable
--------------------

In *oslo.messaging*, the transport_url parameters define the OpenStack service
backends for RPC and Notify. The url is of the form::

    transport://user:pass@host1:port[,hostN:portN]/virtual_host

Where the transport value specifies the rpc or notification backend as
one of ``amqp``, ``rabbit``, ``kafka``, etc. To specify and enable the
Kafka driver for notifications, in the section
``[oslo_messaging_notifications]`` of the service configuration file,
specify the ``transport_url`` parameter::

  [oslo_messaging_notifications]
  transport_url = kafka://username:password@kafkahostname:9092

Note, that if a ``transport_url`` parameter is not specified in the
``[oslo_messaging_notifications]`` section, the ``[DEFAULT]``
transport_url will be used for both RPC and Notify backends.

Driver Options
--------------

It is recommended that the default configuration options provided by
the Kafka driver be used. The configuration options can be modified
in the ``oslo_messaging_kafka`` section of the service configuration file.

Notification Listener Options
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

kafka_max_fetch_bytes
    Initial maximum number of bytes per topic+partition to request
    when fetching messages from the broker.

kafka_consumer_timeout
    Consumer polling interval timeout.

consumer_group
    The default client group identifier.

enable_auto_commit
    Indicator to perform asynchronous consumer message commits.

max_poll_records
    The maximum number of messages to return per consume/poll operation.

Notifier Options
^^^^^^^^^^^^^^^^

producer_batch_timeout
    Delay (ms) to wait for messages in the producer queue to
    accumulate before constructing message sets to transmit to broker

producer_batch_size
    The maximum number of messages batched into one message set

Security Options
^^^^^^^^^^^^^^^^

security_protocol
    The protocol used to communicate with the Kafka brokers.

sasl_mechanisms
    SASL mechanism to use for authentication. Current driver support
    is for PLAIN only.

ssl_cafile
    A file containing the trusted Certificate Authority's digital
    certificate (in PEM format). This certificate is used to
    authenticate the messaging backend.

================
DevStack Support
================

The plugin for the Kafka *oslo.messaging* driver is supported by
DevStack. As the Kafka driver can only be deployed for notifications,
the plugin supports the deployment of several message bus configurations.
In local.conf ``[localrc]`` section, the `devstack-plugin-kafka`_
plugin repository must be enabled. For example:

::

    [[local|localrc]]
    enable_plugin kafka https://git.openstack.org/openstack/devstack-plugin-kafka


Set the Kafka and Scala version and location variables if needed for
the configuration

::

   KAFKA_VERSION=2.0.0
   KAFKA_BASEURL=http://www.apache.org/dist/kafka
   SCALA_VERSION=2.12
   SCALA_BASEURL=http://www.scala-lang.org/riles/archive

The **RPC_** and **NOTIFY_** variables will define the message bus
configuration that will be used. The hybrid configurations will allow
for the *rabbit* and *amqp* drivers to be used for the RPC transports
while the *kafka* driver will be used for the Notify transport. The
setting of the service variables will select which messaging
intermediary is enabled for the configuration:

+------------+--------------------+--------------------+
|            |         RPC        |        NOTIFY      |
|            +-----------+--------+-----------+--------+
|            |  SERVICE  |  PORT  |  SERVICE  |  PORT  |
+------------+-----------+--------+-----------+--------+
|  Config 1  |  rabbit   |  5672  |  kafka    |  9092  |
+------------+-----------+--------+-----------+--------+
|  Config 1  |  amqp     |  5672  |  kafka    |  9092  |
+------------+-----------+--------+-----------+--------+


.. _devstack-plugin-kafka: https://github.com/openstack/devstack-plugin-kafka.git
