============================
 Frequently Asked Questions
============================

I don't need notifications on the message bus. How do I disable them?
=====================================================================

Notification messages can be disabled using the ``noop`` notify
driver. Set ``notification_driver = noop`` in your configuration file.

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
change the notification topic using the ``notification_topics``
configuration option. The option accepts a list of values, so it is
possible to publish to multiple topics.
