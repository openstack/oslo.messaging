=========================================
AMQP 1.0 Protocol Driver Deployment Guide
=========================================

.. currentmodule:: oslo_messaging

Introduction
------------

The AMQP 1.0 Protocol Driver is a messaging transport backend
supported in oslo.messaging. The driver maps the base *oslo.messaging*
capabilities for RPC and Notification message exchange onto version
1.0 of the Advanced Message Queuing Protocol (AMQP 1.0, ISO/IEC
19464). The driver is intended to support any messaging intermediary
(e.g. broker or router) that implements version 1.0 of the AMQP
protocol.

More detail regarding the AMQP 1.0 Protocol is available from the
`AMQP specification`__.

More detail regarding the driver's implementation is available from
the `oslo specification`__.

__ http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-overview-v1.0-os.html
__ https://opendev.org/openstack/oslo-specs/src/branch/master/specs/juno/amqp10-driver-implementation.rst


Abstract
--------

The AMQP 1.0 driver is one of a family of *oslo.messaging* backend
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
~~~~~~~~~~~~~~~~

The RPC messaging pattern is a synchronous exchange between
client and server that is temporally bracketed. The direct messaging
capabilities provided by the message router are optimal for the
RPC messaging pattern.

The driver can readily scale operation from working with a single
instances of a message router to working with a large scale routed
mesh interconnect topology.

Store and Forward
~~~~~~~~~~~~~~~~~

The Notification messaging pattern is an asynchronous exchange from
a notifier to a listener (e.g. consumer). The listener need not be
present when the notification is sent. Thus, the store and forwarding
capabilities provided by the message broker are required for the
Notification messaging pattern.

This driver is able to work with a single instance of a message broker
or a clustered broker deployment.

.. _Limited:

It is recommended that the message router intermediary not be used
for the Notification messaging pattern due to the consideration that
notification messages will be dropped when there is no active
consumer. The message router does not provide durability or
store-and-forward capabilities for notification messages.

Hybrid Messaging Backends
~~~~~~~~~~~~~~~~~~~~~~~~~

Oslo.messaging provides a mechanism to configure separate backends for
RPC and Notification communications. This is supported through the
specification of separate RPC and Notification `transport urls`_ in the
service configuration. This capability enables the optimal alignment
of messaging patterns to messaging backend and allows for different
messaging backend types to be deployed.

This document provides deployment and configuration information for use
of this driver in hybrid messaging configurations.

Addressing
~~~~~~~~~~

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
~~~~~~~~~~~~~~~~~~~~~~~

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


Prerequisites
-------------

Protocol Engine
~~~~~~~~~~~~~~~

This driver uses the Apache QPID `Proton`__ AMQP 1.0 protocol engine.
This engine consists of a platform specific library and a python
binding.  The driver does not directly interface with the engine API,
as the API is a very low-level interface to the AMQP protocol.
Instead, the driver uses the pure python `Pyngus`__ client API, which
is layered on top of the protocol engine.

In order to run the driver the Proton Python bindings, Proton
library, Proton header files, and Pyngus must be installed.

__ http://qpid.apache.org/proton/index.html
__ https://github.com/kgiusti/pyngus

Source packages for the `Pyngus client API`__ are available via PyPI.

__ https://pypi.org/project/pyngus

Pyngus depends on the Proton Python bindings.  Source packages for the
`Proton Python bindings`__ are also available via PyPI.

__ https://pypi.org/project/python-qpid-proton

Since the AMQP 1.0 driver is an optional extension to Oslo.Messaging
these packages are not installed by default.  Use the 'amqp1' extras
tag when installing Oslo.Messaging in order to pull in these extra
packages:

.. code-block:: shell

    $ python -m pip install oslo.messaging[amqp1]

The Proton package includes a C extension that links to the Proton
library.  The C extension is built locally when the Proton source
packages are install from PyPI.  In order to build the Proton C source
locally, there are a number of tools and libraries that need to be
present on the system:

* The tools and library necessary for Python C development
* The `SWIG`__ wrapper generator
* The `OpenSSL`__ development libraries and headers
* The `Cyrus SASL`__ development libraries and headers

**Note well**: Currently the Proton PyPI package only supports building
the C extension on Linux systems.

Pre-built packages for both Pyngus and Proton engine are available for
various Linux distributions (see `packages`_ below). It is recommended
to use the pre-built packages if they are available for your platform.

__ http://www.swig.org/index.php
__ https://www.openssl.org
__ https://cyrusimap.org

Router Intermediary
~~~~~~~~~~~~~~~~~~~

This driver supports a *router* intermediary that supports version 1.0
of the AMQP protocol. The direct messaging capabilities provided by
this intermediary type are recommended for oslo.messaging RPC.

The driver has been tested with `qpid-dispatch-router`__ router in a
`devstack`_ environment. The version of qpid-dispatch-router
**must** be at least 0.7.0. The qpid-dispatch-router also uses the
Proton engine for its AMQP 1.0 support, so the Proton library must be
installed on the system hosting the qpid-dispatch-router daemon.

Pre-built packages for the router are available. See `packages`_ below.

__ http://qpid.apache.org/components/dispatch-router/

Broker Intermediary
~~~~~~~~~~~~~~~~~~~

This driver supports a *broker* intermediary that supports version 1.0
of the AMQP protocol. The store and forward capabilities provided by
this intermediary type are recommended for *oslo.messaging* Notifications.

The driver has been tested with the `qpidd`__ broker in a `devstack`_
environment.  The version of qpidd **must** be at least
0.34.  qpidd also uses the Proton engine for its AMQP 1.0 support, so
the Proton library must be installed on the system hosting the qpidd
daemon.

Pre-built packages for the broker are available. See `packages`_ below.

See the `oslo specification`__ for additional information regarding testing
done on the driver.

__ http://qpid.apache.org/components/cpp-broker/index.html
__ https://opendev.org/openstack/oslo-specs/src/branch/master/specs/juno/amqp10-driver-implementation.rst


Configuration
-------------

.. _transport urls:

Transport URL Enable
~~~~~~~~~~~~~~~~~~~~

In oslo.messaging, the transport_url parameters define the OpenStack service
backends for RPC and Notify. The url is of the form::

    transport://user:pass@host1:port[,hostN:portN]/virtual_host

Where the transport value specifies the rpc or notification backend as
one of **amqp**, rabbit, kafka, etc.

To specify and enable the AMQP 1.0 driver for RPC, in the ``[DEFAULT]``
section of the service configuration file, specify the
``transport_url`` parameter:

.. code-block:: ini

    [DEFAULT]
    transport_url = amqp://username:password@routerhostname:5672

To specify and enable the AMQP 1.0 driver for Notify, in the
``[NOTIFICATIONS]`` section of the service configuration file, specify the
``transport_url`` parameter:

::

  [NOTIFICATIONS]
  transport_url = amqp://username:password@brokerhostname:5672

Note, that if a 'transport_url' parameter is not specified in the
[NOTIFICATIONS] section, the [DEFAULT] transport_url will be used
for both RPC and Notify backends.

Driver Options
~~~~~~~~~~~~~~

It is recommended that the default configuration options provided by
the AMQP 1.0 driver be used. The configuration options can be modified
in the :oslo.config:group:`oslo_messaging_amqp` section of the service
configuration file.

Connection Options
^^^^^^^^^^^^^^^^^^

- :oslo.config:option:`oslo_messaging_amqp.idle_timeout`
- :oslo.config:option:`oslo_messaging_amqp.connection_retry_interval`
- :oslo.config:option:`oslo_messaging_amqp.connection_retry_backoff`
- :oslo.config:option:`oslo_messaging_amqp.connection_retry_interval_max`

Message Send Options
^^^^^^^^^^^^^^^^^^^^

- :oslo.config:option:`oslo_messaging_amqp.pre_settled`
- :oslo.config:option:`oslo_messaging_amqp.link_retry_delay`
- :oslo.config:option:`oslo_messaging_amqp.default_reply_timeout`
- :oslo.config:option:`oslo_messaging_amqp.default_send_timeout`
- :oslo.config:option:`oslo_messaging_amqp.default_notify_timeout`

.. _address mode:

Addressing Options
^^^^^^^^^^^^^^^^^^

- :oslo.config:option:`oslo_messaging_amqp.addressing_mode`
- :oslo.config:option:`oslo_messaging_amqp.server_request_prefix`
- :oslo.config:option:`oslo_messaging_amqp.broadcast_prefix`
- :oslo.config:option:`oslo_messaging_amqp.group_request_prefix`
- :oslo.config:option:`oslo_messaging_amqp.rpc_address_prefix`
- :oslo.config:option:`oslo_messaging_amqp.notify_address_prefix`
- :oslo.config:option:`oslo_messaging_amqp.multicast_address`
- :oslo.config:option:`oslo_messaging_amqp.unicast_address`
- :oslo.config:option:`oslo_messaging_amqp.anycast_address`
- :oslo.config:option:`oslo_messaging_amqp.default_notification_exchange`
- :oslo.config:option:`oslo_messaging_amqp.default_rpc_exchange`

SSL Options
^^^^^^^^^^^

- :oslo.config:option:`oslo_messaging_amqp.ssl`
- :oslo.config:option:`oslo_messaging_amqp.ssl_ca_file`
- :oslo.config:option:`oslo_messaging_amqp.ssl_cert_file`
- :oslo.config:option:`oslo_messaging_amqp.ssl_key_file`
- :oslo.config:option:`oslo_messaging_amqp.ssl_key_password`

SASL Options
^^^^^^^^^^^^

- :oslo.config:option:`oslo_messaging_amqp.sasl_mechanisms`
- :oslo.config:option:`oslo_messaging_amqp.sasl_config_dir`
- :oslo.config:option:`oslo_messaging_amqp.sasl_config_name`
- :oslo.config:option:`oslo_messaging_amqp.sasl_default_realm`

AMQP Generic Options (**Note Well**)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The AMQP 1.0 driver currently does **not** support the generic *amqp*
options used by pre-1.0 drivers such as *amqp_durable_queues* or
*amqp_auto_delete*.

qpid-dispatch-router
~~~~~~~~~~~~~~~~~~~~

First, verify that the Proton library has been installed and is
imported by the ``qpid-dispatch-router`` intermediary. This can be checked
by running:

::

  $ qdrouterd --help

and looking for references to ``qpid-dispatch`` include and config path
options in the help text. If no ``qpid-dispatch`` information is listed,
verify that the Proton libraries are installed and that the version of
``qdrouterd`` is greater than or equal to 0.6.0.

Second, configure the address patterns used by the driver. This is
done by adding the following to ``/etc/qpid-dispatch/qdrouterd.conf``.

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
`1`/etc/qpid-dispatch/qdrouterd.conf`` be updated.

qpidd
~~~~~

First, verify that the Proton library has been installed and is
imported by the qpidd broker.  This can checked by running:

.. code-block:: shell

    $ qpidd --help

and looking for the AMQP 1.0 options in the help text.  If no AMQP 1.0
options are listed, verify that the Proton libraries are installed and
that the version of qpidd is greater than or equal to 0.34.

Second, configure the default address patterns used by the
driver for a broker-based backend. This is done by adding the
following to ``/etc/qpid/qpidd.conf``:

.. code-block:: ini

    queue-patterns=exclusive
    queue-patterns=unicast
    topic-patterns=broadcast

These patterns, *exclusive*, *unicast*, and *broadcast* are the
legacy addressing  values used by the driver.  These can be overridden via the
driver configuration options if desired (see above).  If manually overridden,
update the ``qpidd.conf`` values to match.


.. _devstack:

DevStack Support
----------------

The plugin for the AMQP 1.0 oslo.messaging driver is supported by
DevStack. The plugin supports the deployment of several different
message bus configurations.

In the ``[localrc]`` section of ``local.conf``, the `devstack-plugin-amqp1`__
plugin repository must be enabled. For example:

.. code-block:: ini

    [[local|localrc]]
    enable_plugin amqp1 https://opendev.org/openstack/devstack-plugin-amqp1

Set the username and password variables if needed for the
configuration:

.. code-block:: shell

    AMQP1_USERNAME=queueuser
    AMQP1_PASSWORD=queuepassword

The AMQP1_SERVICE variable identifies the message bus configuration that
will be used. In addition to the AMQP 1.0 driver being used for both the
RPC and Notification messaging communications, a hybrid configuration is
supported in the plugin that will deploy AMQP 1.0 for the RPC backend and
the oslo_messaging rabbit driver for the Notification backend. Additionally,
the plugin supports a setting for a pre-provisioned messaging bus that
prevents the plugin from creating the messaging bus. The setting of the
AMQP1_SERVICE variable will select which messaging intermediary will be used
for the RPC and Notification messaging backends:

+---------------+------------------+------------------+
| AMQP1_SERVICE |   RPC Backend    |  Notify Backend  |
+---------------+------------------+------------------+
|               |                  |                  |
|     qpid      |   qpidd broker   |   qpidd broker   |
|               |                  |                  |
+---------------+------------------+------------------+
|               |                  |                  |
|   qpid-dual   | qdrouterd router |   qpidd broker   |
|               |                  |                  |
+---------------+------------------+------------------+
|               |                  |                  |
|  qpid-hybrid  | qdrouterd router |  rabbitmq broker |
|               |                  |                  |
+---------------+------------------+------------------+
|               |                  |                  |
|   external    |  pre-provisioned | pre-provisioned  |
|               |  message bus     | message bus      |
|               |                  |                  |
+---------------+------------------+------------------+

__ https://github.com/openstack/devstack-plugin-amqp1.git


.. _packages:

Platforms and Packages
----------------------

PyPi
~~~~

Packages for `Pyngus`__ and the `Proton`__ engine are available on PyPI.

__ https://pypi.org/project/pyngus
__ https://pypi.org/project/python-qpid-proton

RHEL and Fedora
~~~~~~~~~~~~~~~

Packages exist in EPEL for RHEL/Centos 7 and 8, and Fedora 26+.

The following packages must be installed on the system running the
``qdrouterd`` daemon:

- ``qpid-dispatch-router``
- ``python-qpid-proton``

The following packages must be installed on the system running the
``qpidd`` daemon:

- ``qpid-cpp-server`` (version 0.26+)
- ``qpid-proton-c``

The following packages must be installed on the systems running the
services that use the new driver:

- Proton libraries: ``qpid-proton-c-devel``
- Proton python bindings: ``python-qpid-proton``
- ``pyngus`` (via PyPI)

Debian and Ubuntu
~~~~~~~~~~~~~~~~~

.. todo:: Is this still true?

Packages for the Proton library, headers, and Python bindings are
available in the Debian/Testing repository.  Proton packages are not
yet available in the Ubuntu repository.  The version of qpidd on both
platforms is too old and does not support AMQP 1.0.

Until the proper package version arrive the latest packages can be
pulled from the `Apache Qpid PPA`__ on Launchpad:

.. code-block:: shell

    $ sudo add-apt-repository ppa:qpid/released

The following packages must be installed on the system running the
``qdrouterd`` daemon:

- ``qdrouterd`` (version 0.8.0+)

The following packages must be installed on the system running the
``qpidd`` daemon:

- ``qpidd`` (version 0.34+)

The following packages must be installed on the systems running the
services that use the new driver:

- Proton libraries: ``libqpid-proton2-dev``
- Proton python bindings: ``python-qpid-proton``
- ``pyngus`` (via Pypi)

__ https://launchpad.net/~qpid/+archive/ubuntu/released
