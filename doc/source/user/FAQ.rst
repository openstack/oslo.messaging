============================
 Frequently Asked Questions
============================

I don't need notifications on the message bus. How do I disable them?
=====================================================================

Notification messages can be disabled using the ``noop`` notify
driver. Set ``driver = noop`` in your configuration file under the
[oslo_messaging_notifications] section.

Why does the notification publisher create queues, too? Shouldn't the subscriber do that?
=========================================================================================

The notification messages are meant to be used for integration with
external services, including services that are not part of
OpenStack. To ensure that the subscriber does not miss any messages if
it starts after the publisher, ``oslo.messaging`` ensures that
subscriber queues exist when notifications are sent.

How do I change the queue names where notifications are published?
==================================================================

Notifications are published to the configured exchange using a topic
built from a base value specified in the configuration file and the
notification "level". The default topic is ``notifications``, so an
info-level notification is published to the topic
``notifications.info``. A subscriber queue of the same name is created
automatically for each of these topics. To change the queue names,
change the notification topic using the ``topics``
configuration option in ``[oslo_messaging_notifications]``. The option
accepts a list of values, so it is possible to publish to multiple topics.

What are the other choices of notification drivers available?
=============================================================

- messaging    Send notifications using the 1.0 message format.
- messagingv2  Send notifications using the 2.0 message format (with a message envelope).
- routing      Configurable routing notifier (by priority or event_type).
- log          Publish notifications via Python logging infrastructure.
- test         Store notifications in memory for test verification.
- noop         Disable sending notifications entirely.
