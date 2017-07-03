-----------------------------------------
AMQP 1.0 Protocol Driver Deployment Guide
-----------------------------------------

.. currentmodule:: oslo_messaging

============
Introduction
============

The AMQP 1.0 Protocol Driver is a messaging transport backend
supported in oslo.messaging. The driver maps the base oslo.messaging
capabilities for RPC and Notification message exchange onto version
1.0 of the Advanced Message Queuing Protocol (AMQP 1.0, ISO/IEC
19464). The driver is intended to support any messaging intermediary
(e.g. broker or router) that implements version 1.0 of the AMQP
protocol.

More detail regarding the AMQP 1.0 Protocol is available from the
`AMQP specification`_.

.. _AMQP specification: http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-overview-v1.0-os.html

More detail regarding the driver's implementation is available from
the `oslo specification`_.

.. _oslo specification: https://git.openstack.org/cgit/openstack/oslo-specs/tree/specs/juno/amqp10-driver-implementation.rst

========
Abstract
========

The AMQP 1.0 driver is one of a family of oslo.messaging backend
drivers. It currently supports two types of message intermediaries.
The first type is an AMQP 1.0 messaging broker and the second type is
an AMQP 1.0 message router. The driver should support additional
intermediary types in the future but may require additions to driver
configuration parameters in order to do so.

+--------------+-----------+------------+------------+-----------+
| Intermediary | RPC       | Notify     | Message    | Topology  |
| Type         | Pattern   | Pattern    | Treatment  |           |
+--------------+-----------+------------+------------+-----------+
| Message      | Yes       | `Limited`_ | Direct     | Single or |
| Router       |           |            | Messaging  | Mesh      |
+--------------+-----------+------------+------------+-----------+
| Message      | Yes       | Yes        | Store and  | Single or |
| Broker       |           |            | Forward    | Cluster   |
+--------------+-----------+------------+------------+-----------+

Direct Messaging
----------------

The direct messaging capabilities provided by the message router
intermediary type are recommended for the oslo.messaging RPC
messaging pattern.

.. _Limited:

Additionally, the message router intermediary may be used for the
Notification messaging pattern with the consideration that
notification messages will be dropped when there is no active
consumer. The message router does not provide durability or
store-and-forward capabilities for notification messages.

The driver can readily scale operation from working with a single
instances of a message router to working with a large scale routed
mesh interconnect topology.

Store and Forward
-----------------

The store and forward capabilities provide by the message broker
intermediary type are recommended for the oslo.message Notification
messaging pattern. This driver is able to work with a single instance
of a message broker or a clustered broker deployment.

Oslo.messaging provides a mechanism to configure separate backends for
RPC and Notification communications. This document provides deployment and
configuration information for use of this driver in oslo.messaging.

Addressing
----------

A new address syntax was added to the driver to support efficient
direct message routing. This new syntax will also work with a broker
intermediary backend but is not compatible with the address syntax
previously used by the driver. In order to allow backward compatibility,
the driver will attempt to identify the intermediary type for the
backend in use and will automatically select the 'legacy' syntax for
broker-based backends or the new 'routable' syntax for router-based
backends. An `address mode`_ configuration option is provided to
override this dynamic behavior and force the use of either the legacy
or routable address syntax.

Message Acknowledgement
-----------------------

A primary functional difference between a router and a
broker intermediary type is when message acknowledgement occurs.

The router does not "store" the message hence it does not generate an
acknowledgement. Instead the consuming endpoint is responsible for message
acknowledgement and the router forwards the acknowledgement back to
the sender. This is known as 'end-to-end' acknowledgement. In contrast, a
broker stores then forwards the message so that message acknowledgement is
performed in two stages. In the first stage, a message
acknowledgement occurs between the broker and the Sender. In the
second stage, an acknowledgement occurs between the Server and
the broker.

This difference affects how long the Sender waits for the message
transfer to complete.

::

                                                        +dispatch+
                                                        |  (3)   |
                                                        |        |
                                                        |        v
  +--------------+    (1)    +----------+    (2)     +--------------+
  |    Client    |---------->|  Router  |----------->|    Server    |
  |   (Sender)   |<----------| (Direct) |<-----------|  (Listener)  |
  +--------------+    (5)    +----------+    (4)     +--------------+


For example when a router intermediary is used, the following sequence
occurs:

1. The message is sent to the router
2. The router forwards the message to the Server
3. The Server dispatches  the message to the application
4. The Server indicates the acknowledgement via the router
5. The router forwards the acknowledgement to the Sender

In this sequence, a Sender waits for the message acknowledgement until
step (5) occurs.


::

                                                        +dispatch+
                                                        |  (4)   |
                                                        |        |
                                                        |        v
  +--------------+    (1)    +----------+    (3)     +--------------+
  |    Client    |---------->|  Broker  |----------->|    Server    |
  |   (Sender)   |<----------| (Queue)  |<-----------|  (Listener)  |
  +--------------+    (2)    +----------+    (5)     +--------------+


And when a broker intermediary is used, the following sequence occurs:

1. The message is sent to the broker
2. The broker stores the message and acknowledges the message to the
   Sender
3. The broker sends the message to the Server
4. The Server dispatches the message to the application
5. The Server indicates the acknowledgement to the broker

In this sequence, a Sender waits for the message acknowledgement until
step (2) occurs.

Therefore the broker-based Sender receives the acknowledgement
earlier in the transfer than the routed case. However in the brokered
case receipt of the acknowledgement does not signify that the message
has been (or will ever be) received by the Server.

Batched Notifications **Note Well**
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

While the use of a router intermediary for oslo.messaging Notification is
currently not recommended, it should be noted that the use of a router
intermediary with batched notifications may exacerbate the acknowledgement
wait time for a Sender.

For example, when a batched notification configuration is used where
batch size is set to 100, the Server will wait until 100 notification
messages are buffered (or timeout occurs) before dispatching the
notifications to the application for message acknowledgement. Since
each notifier client can have at most one message outstanding
(e.g. pending acknowledgement), then if the total number of notifying
clients are less than 100 the batch limit will never be met. This will
effectively pause all notifying clients until the batch timeout expires.

=============
Prerequisites
=============

Protocol Engine
---------------

This driver uses the Apache QPID `Proton`_ AMQP 1.0 protocol engine.
This engine consists of a platform specific library and a python
binding.  The driver does not directly interface with the engine API,
as the API is a very low-level interface to the AMQP protocol.
Instead, the driver uses the pure python `Pyngus`_ client API, which
is layered on top of the protocol engine.

.. _Proton: http://qpid.apache.org/proton/index.html
.. _Pyngus: https://github.com/kgiusti/pyngus

In order to run the driver the Proton Python bindings, Proton
library, Proton header files, and Pyngus must be installed. Pre-built
packages for both Pyngus and the Proton protocol engine are available
for various Linux distributions (see `packages`_ below). It is
recommended to use the pre-built packages if they are available for
your platform.

The Proton package includes a C extension that links to the Proton
library. If this library is not installed, then the Proton install
script will attempt to download the necessary Proton C source files from
the Apache repository and build the library locally.

In order to build the Proton C source locally, there are a number of
tools and libraries that need to be present:

* The tools and library necessary for Python development
* The `SWIG`_ wrapper generator
* The `OpenSSL`_ development libraries and headers
* The `Cyrus SASL`_ development libraries and headers

.. _SWIG: http://www.swig.org/index.php
.. _OpenSSL: https://www.openssl.org
.. _Cyrus SASL: https://cyrusimap.org

**Note well**: Currently the Proton Pypi package only supports building
the C extension on Linux systems.

Router Intermediary
-------------------

This driver supports a *router* intermediary that supports version 1.0
of the AMQP protocol. The direct messaging capabilities provided by
this intermediary type are recommended for oslo.messaging RPC.

The driver has been tested with `qpid-dispatch-router`_ router in a
`devstack`_ environment. The version of qpid-dispatch-router
**must** be at least 0.6.0. The qpid-dispatch-router also uses the
Proton engine for its AMQP 1.0 support, so the Proton library must be
installed on the system hosting the qpid-dispatch-router daemon.

.. _qpid-dispatch-router: http://qpid.apache.org/components/dispatch-router/

Pre-built packages for the router are available. See `packages`_ below.

Broker Intermediary
-------------------

This driver supports a *broker* intermediary that supports version 1.0
of the AMQP protocol. The store and forward capabilities provided by
this intermediary type are recommended for oslo.messaging Notifications.

The driver has been tested with the `qpidd`_ broker in a `devstack`_
environment.  The version of qpidd **must** be at least
0.34.  qpidd also uses the Proton engine for its AMQP 1.0 support, so
the Proton library must be installed on the system hosting the qpidd
daemon.

.. _qpidd: http://qpid.apache.org/components/cpp-broker/index.html

Pre-built packages for the broker are available. See `packages`_ below.

See the `oslo specification`_ for additional information regarding testing
done on the driver.


=============
Configuration
=============

Transport URL Enable
--------------------

In oslo.messaging, the transport_url parameters define the OpenStack service
backends for RPC and Notify. The url is of the form:

    transport://user:pass@host1:port[,hostN:portN]/virtual_host

Where the transport value specifies the rpc or notification backend as
one of **amqp**, rabbit, zmq, etc.

To specify and enable the AMQP 1.0 driver for RPC, in the section
[DEFAULT] of the service configuration file, specify the
'transport_url' parameter:

::

  [DEFAULT]
  transport_url = amqp://username:password@routerhostname:5672

To specify and enable the AMQP 1.0 driver for Notify, in the section
[NOTIFICATIONS] of the service configuration file, specify the
'transport_url' parameter:

::

  [NOTIFICATIONS]
  transport_url = amqp://username:password@brokerhostname:5672

Note, that if a 'transport_url' parameter is not specified in the
[NOTIFICATIONS] section, the [DEFAULT] transport_url will be used
for both RPC and Notify backends.

Driver Options
--------------

It is recommended that the default configuration options provided by
the AMQP 1.0 driver be used. The configuration options can be modified
in the oslo_messaging_amqp section of the service configuration file.

Connection Options
^^^^^^^^^^^^^^^^^^

In section [oslo_messaging_amqp]:

#. idle_timeout: Timeout in seconds for inactive connections.
   Default is disabled.

#. connection_retry_interval: Seconds to pause before attempting to
   re-connect.

#. connection_retry_backoff: Connection retry interval increment after
   unsuccessful failover attempt.

#. connection_retry_interval_max: The maximum duration for a
   connection retry interval.

Message Send Options
^^^^^^^^^^^^^^^^^^^^

In section [oslo_messaging_amqp]:

#. link_retry_delay: Time to pause between re-connecting to an AMQP 1.0 link.

#. default_reply_timeout: The deadline for an rpc reply message delivery.

#. default_send_timeout: The deadline for an rpc cast or call message delivery.

#. default_notify_timeout: The deadline for a sent notification
   message delivery.

.. _address mode:

Addressing Options
^^^^^^^^^^^^^^^^^^

In section [oslo_messaging_amqp]:

#. addressing_mode: Indicates addressing mode used by the driver.

#. server_request_prefix: Legacy address prefix used when sending to a
   specific server.

#. broadcast_prefix: Legacy broadcast prefix used when broadcasting to
   all servers.

#. group_request_prefix: Legacy address prefix when sending to any
   server in a group.

#. rpc_address_prefix: Routable address prefix for all generated RPC addresses.

#. notify_address_prefix: Routable address prefix for all generated
   Notification addresses.

#. multicast_address: Appended to address prefix when sending a fanout address.

#. unicast_address: Appended to address prefix when sending to a
   particular RPC/Notification server.

#. anycast_address: Appended to address prefix when sending to a group
   of consumers.

#. default_notification_exchange: Exchange name used in notification
   addresses if not supplied by the application.

#. default_rpc_exchange: Exchange name used in RPC addresses if not
   supplied by the application.

SSL Options
^^^^^^^^^^^

In section [oslo_messaging_amqp]:

#. ssl_ca_file: A file containing the trusted Certificate Authority's
   digital certificate (in PEM format). This certificate is used to
   authenticate the messaging backend.

#. ssl_cert_file: A file containing a digital certificate (in PEM
   format) that is used to identify the driver with the messaging bus
   (i.e. client authentication).

#. ssl_key_file:A file containing the private key used to sign the
   ssl_cert_file certificate (PEM format, optional if private key is
   stored in the certificate itself).

#. ssl_key_password: The password used to decrypt the private key
   (not required if private key is not encrypted).

SASL Options
^^^^^^^^^^^^

In section [oslo_messaging_amqp]:

#. sasl_config_dir: Path to the *directory* that contains the SASL
   configuration.

#. sasl_config_name: The name of SASL configuration file (without
   .conf suffix) in sasl_config_dir

#. username: SASL user identifier for authentication with the message
   bus. Can be overridden by URL.

#. password: Password for username

AMQP Generic Options (**Note Well**)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The AMQP 1.0 driver currently does **not** support the generic *amqp*
options used by pre-1.0 drivers such as *amqp_durable_queues* or
*amqp_auto_delete*.

qpid-dispatch-router
--------------------

First, verify that the Proton library has been installed and is
imported by the qpid-dispatch-router intermediary. This can be checked
by running:

::

  $ qdrouterd --help

and looking for references to qpid-dispatch include and config path
options in the help text. If no qpid-dispatch information is listed,
verify that the Proton libraries are installed and that the version of
the qdrouterd is greater than or equal to 0.6.0.

Second, configure the address patterns used by the driver. This is
done by adding the following to /etc/qpid-dispatch/qdrouterd.conf.

If the legacy syntax for the addressing mode is required, include the
following:

::

  address {
      prefix: unicast
      distribution: closest
  }

  address {
      prefix: exclusive
      distribution: closest
  }

  address {
      prefix: broadcast
      distribution: multicast
  }

For the routable syntax addressing mode, include the following:

::

  address {
      prefix: openstack.org/om/rpc/multicast
      distribution: multicast
  }

  address {
      prefix: openstack.org/om/rpc/unicast
      distribution: closest
  }

  address {
      prefix: openstack.org/om/rpc/anycast
      distribution: balanced
  }

  address {
      prefix: openstack.org/om/notify/multicast
      distribution: multicast
  }

  address {
      prefix: openstack.org/om/notify/unicast
      distribution: closest
  }

  address {
      prefix: openstack.org/om/notify/anycast
      distribution: balanced
  }


**Note well**: For any customization of the `address mode`_ and syntax used,
it is required that the address entity configurations in the
/etc/qpid-dispatch/qdrouterd.conf be updated.

qpidd
-----

First, verify that the Proton library has been installed and is
imported by the qpidd broker.  This can checked by running:

::

    $ qpidd --help

and looking for the AMQP 1.0 options in the help text.  If no AMQP 1.0
options are listed, verify that the Proton libraries are installed and
that the version of qpidd is greater than or equal to 0.34.

Second, configure the default address patterns used by the
driver for a broker-based backend. This is done by adding the
following to /etc/qpid/qpidd.conf:

::

    queue-patterns=exclusive
    queue-patterns=unicast
    topic-patterns=broadcast

These patterns, *exclusive*, *unicast*, and *broadcast* are the
legacy addressing  values used by the driver.  These can be overridden via the
driver configuration options if desired (see above).  If manually overridden,
update the qpidd.conf values to match.

.. _devstack:

================
DevStack Support
================

The plugin for the AMQP 1.0 oslo.messaging driver is supported by
DevStack. The plugin supports the use of either the broker or router
intermediary.

In local.conf [localrc] section, the `devstack-plugin-amqp1`_  plugin
repository must be enabled. For example:

::

    [[local|localrc]]
    enable_plugin amqp1 https://git.openstack.org/openstack/devstack-plugin-amqp1

Set the username and password variables if needed for the
configuration:

::

    AMQP1_USERNAME=queueuser
    AMQP1_PASSWORD=queuepassword

To configure DevStack for operation of the router intermediary, set
the AMQP1 service variable to "qdr":

::

    AMQP1_SERVICE=qdr

Alternatively, to configure DevStack for operation of the broker
intermediary, set the AMQP1 service variable to "qpid":

::

    AMQP1_SERVICE=qpid

.. _devstack-plugin-amqp1: https://github.com/openstack/devstack-plugin-amqp1.git

.. _packages:

======================
Platforms and Packages
======================

PyPi
----

Packages for `Pyngus pypi`_ and the `Proton pypi`_ engine are available on Pypi.

.. _Pyngus pypi: https://pypi.python.org/pypi/pyngus
.. _Proton pypi: https://pypi.python.org/pypi/python-qpid-proton


RHEL and Fedora
---------------

Packages exist in EPEL for RHEL/Centos 7, and Fedora 24+.
Unfortunately, RHEL/Centos 6 base packages include a very old version
of qpidd that does not support AMQP 1.0.  EPEL's policy does not allow
a newer version of qpidd for RHEL/Centos 6.

The following packages must be installed on the system running the
intermediary daemon:

+--------------+--------------------------+
| Intermediary |         Package          |
+--------------+--------------------------+
|  qdrouterd   | qpid-dispatch-router     |
|              | python-qpid-proton       |
+--------------+--------------------------+
|  qpidd       | qpid-cpp-server          |
|              | qpid-proton-c            |
+--------------+--------------------------+


qpidd daemon:

- qpid-cpp-server (version 0.26+)
- qpid-proton-c

The following packages must be installed on the systems running the
services that use the new driver:

- Proton libraries: qpid-proton-c-devel
- Proton python bindings: python-qpid-proton
- pyngus (via Pypi)

Debian and Ubuntu
-----------------

Packages for the Proton library, headers, and Python bindings are
available in the Debian/Testing repository.  Proton packages are not
yet available in the Ubuntu repository.  The version of qpidd on both
platforms is too old and does not support AMQP 1.0.

Until the proper package version arrive the latest packages can be
pulled from the `Apache Qpid PPA`_ on Launchpad:

::

    sudo add-apt-repository ppa:qpid/released

.. _Apache Qpid PPA: https://launchpad.net/~qpid/+archive/ubuntu/released

The following packages must be installed on the system running the
qdrouterd daemon:

- qdrouterd (version 0.6.0+)

The following packages must be installed on the system running the
qpidd daemon:

- qpidd (version 0.34+)

The following packages must be installed on the systems running the
services that use the new driver:

- Proton libraries: libqpid-proton2-dev
- Proton python bindings: python-qpid-proton
- pyngus (via Pypi)

..  LocalWords:  Acknowledgement acknowledgement
