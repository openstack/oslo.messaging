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
   serializer
   exceptions
   conffixture

Release Notes
=============

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

