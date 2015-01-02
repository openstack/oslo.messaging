-------------------------
AMQP 1.0 Protocol Support
-------------------------

.. currentmodule:: oslo_messaging

============
Introduction
============

This release of oslo.messaging includes an experimental driver that
provides support for version 1.0 of the Advanced Message Queuing
Protocol (AMQP 1.0, ISO/IEC 19464).

The current implementation of this driver is considered
*experimental*.  It is not recommended that this driver be used in
production systems.  Rather, this driver is being provided as a
*technical preview*, in hopes that it will encourage further testing
by the AMQP 1.0 community.

More detail regarding the driver's implementation is available from the `specification`_.

.. _specification: https://git.openstack.org/cgit/openstack/oslo-specs/tree/specs/juno/amqp10-driver-implementation.rst

=============
Prerequisites
=============

This driver uses the Apache QPID `Proton`_ AMQP 1.0 protocol engine.
This engine consists of a platform specific library and a python
binding.  The driver does not directly interface with the engine API,
as the API is a very low-level interface to the AMQP protocol.
Instead, the driver uses the pure python `Pyngus`_ client API, which
is layered on top of the protocol engine.

.. _Proton: http://qpid.apache.org/proton/index.html
.. _Pyngus: https://github.com/kgiusti/pyngus

In order to run the driver the Proton Python bindings, Proton
library, Proton header files, and Pyngus must be installed.

Pyngus is available via `Pypi`__.

.. __: https://pypi.python.org/pypi/pyngus

While the Proton Python bindings are available via `Pypi`__, it
includes a C extension that requires the Proton library and header
files be pre-installed in order for the binding to install properly.
If the target platform's distribution provides a pre-packaged version
of the Proton Python binding (see packages_ below), it is recommended
to use these pre-built packages instead of pulling them from Pypi.

.. __: https://pypi.python.org/pypi/python-qpid-proton

The driver also requires a *broker* that supports version 1.0 of the
AMQP protocol.

The driver has only been tested using `qpidd`_ in a `patched
devstack`_ environment.  The version of qpidd **must** be at least
0.26.  qpidd also uses the Proton engine for its AMQP 1.0 support, so
the Proton library must be installed on the system hosting the qpidd
daemon.

.. _qpidd: http://qpid.apache.org/components/cpp-broker/index.html
.. _patched devstack: https://review.openstack.org/#/c/109118/

At present, RabbitMQ does not work with this driver.  This driver
makes use of the *dynamic* flag on the link Source to automatically
provision a node at the peer.  RabbitMQ's AMQP 1.0 implementation has
yet to implement this feature.

See the `specification`_ for additional information regarding testing
done on the driver.

=============
Configuration
=============

driver
------

It is recommended to start with the default configuration options
supported by the driver.  The remaining configuration steps described
below assume that none of the driver's options have been manually
overridden.

 **Note Well:** The driver currently does **not** support the generic
 *amqp* options used by the existing drivers, such as
 *amqp_durable_queues* or *amqp_auto_delete*.  Support for these are
 TBD.

qpidd
-----

First, verify that the Proton library has been installed and is
imported by the qpidd broker.  This can checked by running::

    $ qpidd --help

and looking for the AMQP 1.0 options in the help text.  If no AMQP 1.0
options are listed, verify that the Proton libraries are installed and
that the version of qpidd is greater than or equal to 0.26.

Second, configure the address patterns used by the driver.  This is
done by adding the following to /etc/qpid/qpidd.conf::

    queue-patterns=exclusive
    queue-patterns=unicast
    topic-patterns=broadcast

These patterns, *exclusive*, *unicast*, and *broadcast* are the
default values used by the driver.  These can be overridden via the
driver configuration options if desired.  If manually overridden,
update the qpidd.conf values to match.

services
--------

The new driver is selected by specifying **amqp** as the transport
name.  For example::

  from oslo import messaging
  from olso.config import cfg

  amqp_transport = messaging.get_transport(cfg.CONF,
                       "amqp://me:passwd@host:5672")


The new driver can be loaded and used by existing applications by
specifying *amqp* as the RPC backend in the service's configuration
file.  For example, in nova.conf::

    rpc_backend = amqp

.. _packages:

======================
Platforms and Packages
======================

Pyngus is available via Pypi.

Pre-built packages for the Proton library and qpidd are available for
some popular distributions:

RHEL and Fedora
---------------

Packages exist in EPEL for RHEL/Centos 7, and Fedora 19+.
Unfortunately, RHEL/Centos 6 base packages include a very old version
of qpidd that does not support AMQP 1.0.  EPEL's policy does not allow
a newer version of qpidd for RHEL/Centos 6.

The following packages must be installed on the system running the
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
pulled from the `Apache Qpid PPA`_ on Launchpad::

    sudo add-apt-repository ppa:qpid/released

.. _Apache Qpid PPA: https://launchpad.net/~qpid/+archive/ubuntu/released

The following packages must be installed on the system running the
qpidd daemon:

- qpidd (version 0.26+)
- libqpid-proton2

The following packages must be installed on the systems running the
services that use the new driver:

- Proton libraries: libqpid-proton2-dev
- Proton python bindings: python-qpid-proton
- pyngus (via Pypi)
