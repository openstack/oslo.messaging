=============================
Kafka Driver Deployment Guide
=============================

Introduction
------------

The Kafka Driver is an experimental messaging transport backend
in *oslo.messaging*. The driver maps the base *oslo.messaging*
capabilities for notification message exchange onto v2.0 of the
Apache Kafka distributed streaming platform. More detail regarding
the Apache Kafka server is available from the `Apache Kafka website`__.

More detail regarding the driver's implementation is available from
the `adding kafka driver specification`__ and the `update kafka driver
specification`__.

__ https://kafka.apache.org/
__ https://opendev.org/openstack/oslo-specs/src/branch/master/specs/liberty/adding-kafka-support.rst
__ https://opendev.org/openstack/oslo-specs/src/branch/master/specs/queens/update-kafka-support.rst


Overview
--------

The Kafka driver **only** supports use for sending and receiving
*oslo.messaging* notifications. Specifically, the Kafka driver
does not support *oslo.messaging* RPC transfers. Communications between
the driver and Kafka server backend uses a `binary protocol over TCP`__
that defines all APIs as request response message pairs. The Kafka
driver integrates the ``confluent-kafka`` Python client for full
protocol support and utilizes the Producer API to publish notification
messages and the Consumer API for notification listener subscriptions.
The driver is able to work with a single instance of a Kafka server or
a clustered Kafka server deployment.

__ https://kafka.apache.org/protocol.html

Hybrid Messaging Deployment
~~~~~~~~~~~~~~~~~~~~~~~~~~~

*Oslo.messaging* provides a mechanism to configure separate messaging
backends for RPC and notification communications. This is supported
through the definition of separate RPC and notification
`transport urls`__ in the service configuration. When the Kafka driver
is deployed for *oslo.messaging* notifications, a separate driver and
messaging backend must be deployed for RPC communications. For these
hybrid messaging configurations, either the `rabbit`__ or `amqp`__
drivers can be deployed for *oslo.messaging* RPC.

__ https://docs.openstack.org/oslo.messaging/latest/reference/transport.html
__ https://docs.openstack.org/oslo.messaging/latest/admin/drivers.html#rabbit
__ https://docs.openstack.org/oslo.messaging/latest/admin/AMQP1.0.html

Topics and vhost Support
~~~~~~~~~~~~~~~~~~~~~~~~

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
~~~~~~~~~~~~~~

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
~~~~~~~~~~~~~~~~~~

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


Prerequisites
-------------

In order to run the driver the ``confluent-kafka`` Python client must be
installed. The Kafka driver integrates a Python client based on `librdkafka`__
for full protocol support and utilizes the Producer API to publish
notification messages and the Consumer API for notification listener
subscriptions.

__ https://github.com/confluentinc/confluent-kafka-python

Source packages for the ``confluent-kafka`` library are available via `PyPI`__.
Since the Kafka driver is an optional extension to *oslo.messaging*
these packages are not installed by default.  Use the ``kafka`` extras
tag when installing *oslo.messaging* in order to pull in these extra
packages:

.. code-block:: shell

   $ python -m pip install oslo.messaging[kafka]

__ https://pypi.org/project/confluent-kafka/


Configuration
-------------

Transport URL Enable
~~~~~~~~~~~~~~~~~~~~

In *oslo.messaging*, the ``transport_url`` parameters define the OpenStack
service backends for RPC and Notify. The URL is of the form::

    transport://user:pass@host1:port[,hostN:portN]/virtual_host

Where the transport value specifies the RPC or notification backend as
one of ``amqp``, ``rabbit``, ``kafka``, etc. To specify and enable the
Kafka driver for notifications, in the section
``[oslo_messaging_notifications]`` of the service configuration file,
specify the ``transport_url`` parameter::

    [oslo_messaging_notifications]
    transport_url = kafka://username:password@kafkahostname:9092

Note, that if a ``transport_url`` parameter is not specified in the
``[oslo_messaging_notifications]`` section, the value of ``[DEFAULT]
transport_url`` will be used for both RPC and notification backends.

Driver Options
~~~~~~~~~~~~~~

It is recommended that the default configuration options provided by
the Kafka driver be used. The configuration options can be modified
in the :oslo.config:group:`oslo_messaging_kafka` section of the service
configuration file.

Notification Listener Options
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- :oslo.config:option:`oslo_messaging_kafka.kafka_max_fetch_bytes`
- :oslo.config:option:`oslo_messaging_kafka.kafka_consumer_timeout`
- :oslo.config:option:`oslo_messaging_kafka.consumer_group`
- :oslo.config:option:`oslo_messaging_kafka.enable_auto_commit`
- :oslo.config:option:`oslo_messaging_kafka.max_poll_records`

Notifier Options
^^^^^^^^^^^^^^^^

- :oslo.config:option:`oslo_messaging_kafka.producer_batch_timeout`
- :oslo.config:option:`oslo_messaging_kafka.producer_batch_size`

compression_codec
    The compression codec for all data generated by the producer, valid values
    are: none, gzip, snappy, lz4, zstd. Note that the legal option of this
    depends on the kafka version, please refer to `kafka documentation`_.

.. _kafka documentation: https://kafka.apache.org/documentation/

Security Options
^^^^^^^^^^^^^^^^

- :oslo.config:option:`oslo_messaging_kafka.security_protocol`
- :oslo.config:option:`oslo_messaging_kafka.sasl_mechanism`
- :oslo.config:option:`oslo_messaging_kafka.ssl_cafile`


DevStack Support
----------------

The plugin for the Kafka *oslo.messaging* driver is supported by
DevStack. As the Kafka driver can only be deployed for notifications,
the plugin supports the deployment of several message bus configurations.
In the ``[localrc]`` section of ``local.conf``, the `devstack-plugin-kafka`__
plugin repository must be enabled. For example:

.. code-block:: ini

    [[local|localrc]]
    enable_plugin kafka https://opendev.org/openstack/devstack-plugin-kafka

Set the Kafka and Scala version and location variables if needed for
the configuration

.. code-block:: shell

   KAFKA_VERSION=2.0.0
   KAFKA_BASEURL=http://archive.apache.org/dist/kafka
   SCALA_VERSION=2.12
   SCALA_BASEURL=http://www.scala-lang.org/riles/archive

The ``RPC_`` and ``NOTIFY_`` variables will define the message bus
configuration that will be used. The hybrid configurations will allow
for the *rabbit* and *amqp* drivers to be used for the RPC transports
while the *kafka* driver will be used for the notification transport. The
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

__ https://github.com/openstack/devstack-plugin-kafka.git
