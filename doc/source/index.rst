oslo.messaging
==============

The Oslo messaging API supports RPC and notifications over a number of
different messsaging transports.

Contents
========

.. toctree::
   :maxdepth: 1

   transport
   executors
   target
   server
   rpcclient
   notifier
   notification_listener
   serializer
   exceptions
   opts
   conffixture
   AMQP1.0
   FAQ
   contributing

Release Notes
=============

1.4.0.0a4
---------

Changes since 1.4.0.0a3:

* 1314129_: on python 2.6 use simplejson for better performance
* 1342088_: fix bogus locking in fake driver
* Enabled hacking checks H305 and H307

.. _1314129: https://bugs.launchpad.net/oslo.messaging/+bug/1314129
.. _1342088: https://bugs.launchpad.net/oslo.messaging/+bug/1342088

Thanks to Christian Berendt, Ihar Hrachyshka and Nejc Saje for their
contributions to this release.

1.4.0.0a3
---------

Changes since 1.4.0.0a2:

* cleanup:  replaced 'e.g.' with 'for example'.
* trollius: fix AMQPListener for polling with timeout.
* cleanup: fixed pep8 issue E265.
* docs: remove duplicate docs for MessageHandlingServer.
* docs: add 'docs' tox environment.
* cleanup: use assertEqual() instead assertIs() for strings.
* tests: re-organize the unit tests directory.

Thanks to Boris Pavlovic, Christian Berendt, Mark McLoughlin,
Victor Stinner and YAMAMOTO Takashi for their contributions to
this release.

1.4.0.0a2
---------

Changes since 1.4.0.0a1:

* docs: fix Notifier instantiation example.
* docs: cleanup formatting of examples in TransportURL docs.
* tests: fix slow notification listener unit tests.
* cleanup: remove unprintable character from source file.
* python3: replace usage of str() with six.text_type.

Thanks to Aaron Rosen, Gauvain Pocentek, Mark McLoughlin, Paul Michali
and Thomas Herve for their contributions to this release.

1.4.0.0a1
---------

Changes since 1.3.0:

Dependency changes:

* Newer oslo.config is required (1.2.1, previously 1.2.0)
* Newer six is required (1.7.0, previously 1.5.2)
* Hacking 0.9.1 is now required for testing (previously 0.8.0)

New features:

* 1282639_: A new 'retry' parameter to control transport reconnection retries in RPC clients and notifiers.
* 1316891_: TransportURL objects are now hashable.
* Full support of multiple hosts in transport URLs.
* Listener.poll() now has an optional timeout parameter in preparion for trollius/asyncio support.

Bug fixes:

* 1316681_: Fix the notify method of the routing notifier.
* 1325750_: Clarify confusing rabbitmq log message if credentials checking fails.
* 1256345_: Allow setting 'exchange' in the target object used by RPC clients and servers.
* 1310397_: Enable log messages to handle exceptions containing unicode.
* 1314129_: Synced jsonutils from oslo-incubator to solve slowness on python 2.6.

Logging:

* Initial infrastructure to allow translating log messages.
* 1321274_: Replace string format arguments with function parameters.
* 1317950_: Debug level logs should not be translated.
* 1286306_: Remove str() from LOG.* and exceptions.

Python3:

* Removes use of contextlib.nested.
* 1280033_: Use six rather than the py3kcompat oslo-incubator module.

Tests:

* Import run_cross_tests.sh from oslo-incubator.
* Ensures listener queues exist in fake driver.
* 1331459_: Disable connection pool in qpid interfaces tests.
* 1283926_: Fixed the issue for pop exception (rabbit tests race condition).

RabbitMQ driver:

* 1261631_: Select AMQP message broker at random.

Qpid driver:

* 1318742_: Explicitly name subscription queue for responses.
* 1300318_: Ensure routing key is specified in the address for a direct producer.
* 1261631_: Select AMQP message broker at random.
* 1303890_: Update ensure()/reconnect() to catch MessagingError.

ZeroMQ driver:

* 1330460_: Handle unused allowed_remote_exmods in _multi_send.
* 1332588_: Set correct group for matchmaker_redis options.
* 1301132_: zmq: switch back to not using message envelopes.
* Fix passing envelope variable as timeout.
* 1300539_: Logical error in blockless fanout of zmq.
* 1301132_: Oslo-messaging-zmq-receiver cannot recive any messages.

Docs:

* RPC server doc: use the blocking executor.
* Cleaned up references to executor specific RPCServer types.
* Add an example usage of RPCClient retry parameter.
* Fix typo in docstring of notify/notifier.

Cleanups:

* Improve layout of unit test directory.
* 1327473_: Removes the use of mutables as default args.
* rabbit/qpid: remove the args/kwargs from ensure().
* Removes unused config option - allowed_rpc_exception_modules
* 1323975_: remove default=None for config options
* Use a for loop to set the defaults for __call__ params
* Use mock's call assert methods over call_args_list
* Remove rendundant parentheses of cfg help strings
* Remove old drivers dead code
* 1277104_: Trival:Fix assertEqual arguments order

Thanks to Aaron Rosen, Ala Rezmerita, Andreas Jaeger, Boris Pavlovic,
ChangBo Guo, Christian Berendt, Davanum Srinivas, Doug Hellmann, Elena
Ezhova, Eric Guo, Gauvain Pocentek, Gordon Sim, Ihar Hrachyshka, James
Carey, Jamie Lennox, Joshua Harlow, Li Ma, Mark McLoughlin, Mehdi
Abaakouk, Numan Siddique, Russell Bryant, Victor Stinner and
zhangjialong for their contributions to this release.

.. _1256345: https://bugs.launchpad.net/oslo.messaging/+bug/1256345
.. _1261631: https://bugs.launchpad.net/oslo.messaging/+bug/1261631
.. _1277104: https://bugs.launchpad.net/oslo.messaging/+bug/1277104
.. _1280033: https://bugs.launchpad.net/oslo.messaging/+bug/1280033
.. _1282639: https://bugs.launchpad.net/oslo.messaging/+bug/1282639
.. _1283926: https://bugs.launchpad.net/oslo.messaging/+bug/1283926
.. _1286306: https://bugs.launchpad.net/oslo.messaging/+bug/1286306
.. _1300318: https://bugs.launchpad.net/oslo.messaging/+bug/1300318
.. _1300539: https://bugs.launchpad.net/oslo.messaging/+bug/1300539
.. _1301132: https://bugs.launchpad.net/oslo.messaging/+bug/1301132
.. _1303890: https://bugs.launchpad.net/oslo.messaging/+bug/1303890
.. _1310397: https://bugs.launchpad.net/oslo.messaging/+bug/1310397
.. _1314129: https://bugs.launchpad.net/oslo.messaging/+bug/1314129
.. _1316681: https://bugs.launchpad.net/oslo.messaging/+bug/1316681
.. _1316891: https://bugs.launchpad.net/oslo.messaging/+bug/1316891
.. _1317950: https://bugs.launchpad.net/oslo.messaging/+bug/1317950
.. _1318742: https://bugs.launchpad.net/oslo.messaging/+bug/1318742
.. _1321274: https://bugs.launchpad.net/oslo.messaging/+bug/1321274
.. _1323975: https://bugs.launchpad.net/oslo.messaging/+bug/1323975
.. _1325750: https://bugs.launchpad.net/oslo.messaging/+bug/1325750
.. _1327473: https://bugs.launchpad.net/oslo.messaging/+bug/1327473
.. _1330460: https://bugs.launchpad.net/oslo.messaging/+bug/1330460
.. _1331459: https://bugs.launchpad.net/oslo.messaging/+bug/1331459
.. _1332588: https://bugs.launchpad.net/oslo.messaging/+bug/1332588

1.3.1
-----

Changes since 1.3.0:

* 1318742_: Explicitly name subscription queue for responses.
* 1300318_: Ensure routing key is specified in the address for a direct producer.
* 1303890_: Update ensure()/reconnect() to catch MessagingError.
* 1283926_: Fixed the issue for pop exception (rabbit tests race condition).
* Ensures listener queues exist in fake driver.
* Fixes incorrect exchange lock in fake driver

.. _1283926: https://bugs.launchpad.net/oslo.messaging/+bug/1283926
.. _1300318: https://bugs.launchpad.net/oslo.messaging/+bug/1300318
.. _1303890: https://bugs.launchpad.net/oslo.messaging/+bug/1303890
.. _1318742: https://bugs.launchpad.net/oslo.messaging/+bug/1318742


Thanks to Alan Pevec, Gordon Sim, Mark McLoughlin, Matt Riedemann,
Mehdi Abaakouk, Nejc Saje, Numan Siddique and Russell Bryant for their
contributions to this release.

1.3.0
-----

Changes since 1.3.0a9:

* Expose LoggingErrorNotificationHandler in the public API
* 1288425_: Add kombu driver library to requirements.txt
* 1255239_: Add unit tests for the qpid driver
* 1261631_: Add unit test for Qpid reconnect order
* 1282706_: Fixed inconsistent eventlet test failures
* 1297161_: Fixed pep8 failure due to pyflakes bug
* 1286984_: Build log_handler documentation

.. _1288425: https://bugs.launchpad.net/oslo.messaging/+bug/1288425
.. _1255239: https://bugs.launchpad.net/oslo.messaging/+bug/1255239
.. _1261631: https://bugs.launchpad.net/oslo.messaging/+bug/1261631
.. _1282706: https://bugs.launchpad.net/oslo.messaging/+bug/1282706
.. _1297161: https://bugs.launchpad.net/oslo.messaging/+bug/1297161
.. _1286984: https://bugs.launchpad.net/oslo.messaging/+bug/1286984

Thanks to Alex Holden, ChangBo Guo, Clint Byrum, Doug Hellmann, Ihar
Hrachyshka, Lance Bragstad and Numan Siddique for their contributions
to this release.

1.3.0a9
-------

Changes since 1.3.0a8:

* 856764_: Handle RabbitMQ consumer cancel notifications
* 856764_: Slow down RabbitMQ reconnection attempts
* 1287542_: Fix issue with duplicate SSL config options registered

.. _856764: https://bugs.launchpad.net/oslo.messaging/+bug/856764
.. _1287542: https://bugs.launchpad.net/oslo.messaging/+bug/1287542

Thanks to Chet Burgess, Doug Hellmann and Nicolas Simonds for their
contributions to this release.

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

