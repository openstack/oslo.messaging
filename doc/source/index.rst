oslo.messaging
==============

The Oslo messaging API supports RPC and notifications over a number of
different messsaging transports.

Contents
========

.. toctree::
   :maxdepth: 1

   transport
   target
   server
   rpcclient
   notifier
   notification_listener
   serializer
   exceptions
   opts
   conffixture

Release Notes
=============

1.3.0a9
-------

Changes since 1.3.0a8:

* 856764_: Handle RabbitMQ consumer cancel notifications
* 856764_: Slow down RabbitMQ reconnection attempts
* 1287542_: Fix issue with duplicate SSL config options registered

.. _856764: https://bugs.launchpad.net/oslo.messaging/+bug/856764
.. _1287542: https://bugs.launchpad.net/oslo.messaging/+bug/1287542

1.3.0a8
-------

Changes since 1.3.0a7:

* notification_listener_: More notification listener API additions
* log_handler_: Add notifier log handler
* 1282038_: Fix Qpid driver regression
* 1276163_: Fix ExpectedException handling
* 1261631_: Fix RabbitMQ and Qpid reconnection behaviour
* 1269890_: Support different RabbitMQ login methods
* 1277168_: Switch from oslo.sphinx to oslosphinx
* Convert to oslo.test library
* Improve configuration help strings

.. _notification_listener: https://blueprints.launchpad.net/oslo.messaging/+spec/notification-subscriber-server
.. _log_handler: https://blueprints.launchpad.net/oslo.messaging/+spec/log-handler
.. _1282038: https://bugs.launchpad.net/oslo.messaging/+bug/1282038
.. _1276163: https://bugs.launchpad.net/oslo.messaging/+bug/1276163
.. _1261631: https://bugs.launchpad.net/oslo.messaging/+bug/1261631
.. _1269890: https://bugs.launchpad.net/oslo.messaging/+bug/1269890
.. _1277168: https://bugs.launchpad.net/oslo.messaging/+bug/1277168

Thanks to Andreas Jaeger, Ben Nemec, Dirk Mueller, Doug Hellmann,
Flavio Percoco, Ihar Hrachyshka, Jeremy Hanmer, Joe Harrison,
Kurt Griffiths, Lance Bragstad, Mehdi Abaakouk and Xavier Queralt
for their contributions to this release.

1.3.0a7
-------

Changes since 1.3.0a6:

* notification_listener_: Add notification listener API
* 1272271_: Fix regression in RabbitMQ reconnection support
* 1273455_: Remove use of deprecated stevedore API

.. _notification_listener: https://blueprints.launchpad.net/oslo.messaging/+spec/notification-subscriber-server
.. _1272271: https://bugs.launchpad.net/oslo.messaging/+bug/1272271
.. _1273455: https://bugs.launchpad.net/oslo.messaging/+bug/1273455

Thanks to Ala Rezmerita, Doug Hellmann and Mehdi Abaakouk for their
contributions to this release.

1.3.0a6
-------

Changes since 1.3.0a5:

* 1241566_: Enable sample config file generator include oslo.messaging options

.. _1241566: https://bugs.launchpad.net/oslo.messaging/+bug/1241566

Thanks to Andreas Jaeger for his contributions to this release.

1.3.0a5
-------

Changes since 1.3.0a3:

* routing_notifier_: Add a routing notifier

.. _routing_notifier: http://blueprints.launchpad.net/oslo.messaging/+spec/configurable-notification

Thanks to Dirk Mueller and Sandy Walsh for their contributions to this
release.

1.3.0a3
-------

Changes since 1.3.0a2:

* aliases_: Add transport aliases API
* 1257293_: Fix duplicate topic messages for Qpid topology=2
* 1251757_: Fix issue with qpid driver reconnects
* Add Sample priority to notifier API
* Remove eventlet related code in amqp driver
* Significant progress on Python 3 support.
* Sync some changes from RPC code in oslo-incubator.

.. _aliases: https://blueprints.launchpad.net/oslo.messaging/+spec/transport-aliases
.. _1257293: https://bugs.launchpad.net/oslo/+bug/1257293
.. _1251757: https://bugs.launchpad.net/oslo/+bug/1251757

Thanks to Chang Bo Guo, Eric Guo, Ihar Hrachyshka, Joe Gordon,
Kenneth Giusti, Lance Bragstad, Mehdi Abaakouk, Nikhil Manchanda,
Sandy Walsh, Stanislav Kudriashev, Victor Stinner and Zhongyue Luo for
their contributions to this release!

1.3.0a2
-------

Changes since 1.3.0a1:

* logging_and_notification_: Notifications can now be sent using a python logging handler.
* Notifier.warning() was added as an alias of Notifier.warn().
* Notifier.audit() has been added.
* 1178375_: Support a new qpid topology.
* TransportURL.hosts is no longer a read-only property.
* MessagingException now behaves more like normal exceptions.
* Fix sending of notifications.
* Many internal cleanups.

.. _logging_and_notification: https://blueprints.launchpad.net/oslo.messaging/+spec/logging-and-notification
.. _1178375: https://bugs.launchpad.net/oslo/+bug/1178375

Thanks to Chang Bo Guo, Christian Strack, Julien Danjou, Kenneth Giusti
and Russell Bryant for their contributions to this release!

1.2.0a1
-------

* Initial release of oslo.messaging_.

.. _oslo.messaging: https://wiki.openstack.org/wiki/Oslo/Messaging

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

