------------------------------
Pika Driver Deployment Guide
------------------------------

.. currentmodule:: oslo_messaging

.. warning:: the Pika driver is no longer maintained and will be
   removed from Oslo.Messaging at a future date.  It is recommended that
   all users of the Pika driver transition to using the Rabbit driver.

============
Introduction
============

Pika is a pure-Python implementation of the AMQP 0-9-1 protocol including
RabbitMQ's extensions. It is very actively supported and recommended by
RabbitMQ developers

========
Abstract
========

PikaDriver is one of oslo.messaging backend drivers. It supports RPC and Notify
patterns. Currently it could be the only oslo.messaging driver across the
OpenStack cluster. This document provides deployment information for this
driver in oslo_messaging.

This driver is able to work with single instance of RabbitMQ server or
RabbitMQ cluster.


=============
Configuration
=============

Enabling (mandatory)
--------------------

To enable the driver, in the section [DEFAULT] of the conf file,
the 'transport_url' parameter  should be set to
`pika://user:pass@host1:port[,hostN:portN]`

        [DEFAULT]
        transport_url = pika://guest:guest@localhost:5672


Connection options (optional)
-----------------------------

In section [oslo_messaging_pika]:
#. channel_max - Maximum number of channels to allow,

#. frame_max (default - pika default value): The maximum byte size for
   an AMQP frame,

#. heartbeat_interval (default=1): How often to send heartbeats for
   consumer's connections in seconds. If 0 - disable heartbeats,

#. ssl (default=False): Enable SSL if True,

#. ssl_options (default=None): Arguments passed to ssl.wrap_socket,

#. socket_timeout (default=0.25): Set timeout for opening new connection's
   socket,

#. tcp_user_timeout (default=0.25): Set TCP_USER_TIMEOUT in seconds for
   connection's socket,

#. host_connection_reconnect_delay (default=0.25): Set delay for reconnection
   to some host after connection error


Connection pool options (optional)
----------------------------------

In section [oslo_messaging_pika]:

#. pool_max_size (default=10): Maximum number of connections to keep queued,

#. pool_max_overflow (default=0): Maximum number of connections to create above
   `pool_max_size`,

#. pool_timeout (default=30): Default number of seconds to wait for a
   connections to available,

#. pool_recycle (default=600): Lifetime of a connection (since creation) in
   seconds or None for no recycling. Expired connections are closed on acquire,

#. pool_stale (default=60): Threshold at which inactive (since release)
   connections are considered stale in seconds or None for no staleness.
   Stale connections are closed on acquire.")

RPC related options (optional)
------------------------------

In section [oslo_messaging_pika]:

#. rpc_queue_expiration (default=60): Time to live for rpc queues without
   consumers in seconds,

#. default_rpc_exchange (default="${control_exchange}_rpc"): Exchange name for
   sending RPC messages,

#. rpc_reply_exchange', default=("${control_exchange}_rpc_reply"): Exchange
   name for receiving RPC replies,

#. rpc_listener_prefetch_count (default=100): Max number of not acknowledged
   message which RabbitMQ can send to rpc listener,

#. rpc_reply_listener_prefetch_count (default=100): Max number of not
   acknowledged message which RabbitMQ can send to rpc reply listener,

#. rpc_reply_retry_attempts (default=-1): Reconnecting retry count in case of
   connectivity problem during sending reply. -1 means infinite retry during
   rpc_timeout,

#. rpc_reply_retry_delay (default=0.25) Reconnecting retry delay in case of
   connectivity problem during sending reply,

#. default_rpc_retry_attempts (default=-1): Reconnecting retry count in case of
   connectivity problem during sending RPC message, -1 means infinite retry. If
   actual retry attempts in not 0 the rpc request could be processed more than
   one time,

#. rpc_retry_delay (default=0.25): Reconnecting retry delay in case of
   connectivity problem during sending RPC message

$control_exchange in this code is value of [DEFAULT].control_exchange option,
which is "openstack" by default

Notification related options (optional)
---------------------------------------

In section [oslo_messaging_pika]:

#. notification_persistence (default=False): Persist notification messages,

#. default_notification_exchange (default="${control_exchange}_notification"):
   Exchange name for sending notifications,

#. notification_listener_prefetch_count (default=100): Max number of not
   acknowledged message which RabbitMQ can send to notification listener,

#. default_notification_retry_attempts (default=-1): Reconnecting retry count
    in case of connectivity problem during sending notification, -1 means
    infinite retry,

#. notification_retry_delay (default=0.25): Reconnecting retry delay in case of
   connectivity problem during sending notification message

$control_exchange in this code is value of [DEFAULT].control_exchange option,
which is "openstack" by default

DevStack Support
----------------

Pika driver is supported by DevStack. To enable it you should edit
local.conf [localrc] section and add next there:

    enable_plugin pika https://git.openstack.org/openstack/devstack-plugin-pika
