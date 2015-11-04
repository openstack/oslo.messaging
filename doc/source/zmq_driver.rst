------------------------------
ZeroMQ Driver Deployment Guide
------------------------------

.. currentmodule:: oslo_messaging

============
Introduction
============

0MQ (also known as ZeroMQ or zmq) is embeddable networking library
but acts like a concurrency framework. It gives you sockets
that carry atomic messages across various transports
like in-process, inter-process, TCP, and multicast. You can connect
sockets N-to-N with patterns like fan-out, pub-sub, task distribution,
and request-reply. It's fast enough to be the fabric for clustered
products. Its asynchronous I/O model gives you scalable multi-core
applications, built as asynchronous message-processing tasks. It has
a score of language APIs and runs on most operating systems.

Originally the zero in 0MQ was meant as "zero broker" and (as close to)
"zero latency" (as possible). Since then, it has come to encompass
different goals: zero administration, zero cost, and zero waste.
More generally, "zero" refers to the culture of minimalism that permeates
the project.

More detail regarding ZeroMQ library is available from the `specification`_.

.. _specification: http://zguide.zeromq.org/page:all

========
Abstract
========

In Folsom, OpenStack introduced an optional messaging system using ZeroMQ.
For some deployments, especially for those super-large scenarios, it may be
desirable to use a broker-less RPC mechanism to scale out.

The original deployment guide only supports Nova in `folsom`_.

.. _folsom: http://ewindisch.github.io/nova

Currently, ZeroMQ is one of the RPC backend drivers in oslo.messaging. In the
Juno release, as almost all the core projects in OpenStack have switched to
oslo_messaging, ZeroMQ can be the only RPC driver across the OpenStack cluster.
This document provides deployment information for this driver in oslo_messaging.

Other than AMQP-based drivers, like RabbitMQ, ZeroMQ doesn't have
any central brokers in oslo.messaging, instead, each host (running OpenStack
services) is both ZeroMQ client and server. As a result, each host needs to
listen to a certain TCP port for incoming connections and directly connect
to other hosts simultaneously.

Topics are used to identify the destination for a ZeroMQ RPC call. There are
two types of topics, bare topics and directed topics. Bare topics look like
'compute', while directed topics look like 'compute.machine1'.

========
Scenario
========

Assuming the following systems as a goal.

::

    +--------+
    | Client |
    +----+---+
         |
    -----+---------+-----------------------+---------------------
                   |                       |
          +--------+------------+  +-------+----------------+
          | Controller Node     |  | Compute Node           |
          |  Nova               |  |  Neutron               |
          |  Keystone           |  |  Nova                  |
          |  Glance             |  |   nova-compute         |
          |  Neutron            |  |  Ceilometer            |
          |  Cinder             |  |  Oslo-zmq-receiver     |
          |  Ceilometer         |  +------------------------+
          |  Oslo-zmq-receiver  |
          |  Horizon            |
          +---------------------+

=============
Configuration
=============

Enabling (mandatory)
--------------------

To enable the driver, in the section [DEFAULT] of the conf file,
the 'rpc_backend' flag must be set to 'zmq' and the 'rpc_zmq_host' flag
must be set to the hostname of the current node.

        [DEFAULT]
        rpc_backend = zmq
        rpc_zmq_host = {hostname}


Match Making (mandatory)
------------------------

The ZeroMQ driver implements a matching capability to discover hosts available
for communication when sending to a bare topic. This allows broker-less
communications.

The MatchMaker is pluggable and it provides two different MatchMaker classes.

DummyMatchMaker: default matchmaker driver for all-in-one scenario (messages
are sent to itself).

RedisMatchMaker: loads the hash table from a remote Redis server, supports
dynamic host/topic registrations, host expiration, and hooks for consuming
applications to acknowledge or neg-acknowledge topic.host service availability.

To set the MatchMaker class, use option 'rpc_zmq_matchmaker' in [DEFAULT].

        rpc_zmq_matchmaker = dummy
        or
        rpc_zmq_matchmaker = redis

To specify the Redis server for RedisMatchMaker, use options in
[matchmaker_redis] of each project.

        [matchmaker_redis]
        host = 127.0.0.1
        port = 6379
        password = None


MatchMaker Data Source (mandatory)
----------------------------------

MatchMaker data source is stored in files or Redis server discussed in the
previous section. How to make up the database is the key issue for making ZeroMQ
driver work.

If deploying the RedisMatchMaker, a Redis server is required. Each (K, V) pair
stored in Redis is that the key is a base topic and the corresponding values are
hostname arrays to be sent to.


Proxy to avoid blocking (optional)
----------------------------------

Each machine running OpenStack services, or sending RPC messages, may run the
'oslo-messaging-zmq-broker' daemon. This is needed to avoid blocking
if a listener (server) appears after the sender (client).

Running the local broker (proxy) or not is defined by the option 'zmq_use_broker'
(True by default). This option can be set in [DEFAULT] section.

For example::

        zmq_use_broker = False


In case of using the broker all publishers (clients) talk to servers over
the local broker connecting to it via IPC transport.

The IPC runtime directory, 'rpc_zmq_ipc_dir', can be set in [DEFAULT] section.

For example::

        rpc_zmq_ipc_dir = /var/run/openstack

The parameters for the script oslo-messaging-zmq-receiver should be::

        oslo-messaging-zmq-broker
            --config-file /etc/oslo/zeromq.conf
            --log-file /var/log/oslo/zmq-broker.log

You can specify ZeroMQ options in /etc/oslo/zeromq.conf if necessary.

Listening Address (optional)
----------------------------

All services bind to an IP address or Ethernet adapter. By default, all services
bind to '*', effectively binding to 0.0.0.0. This may be changed with the option
'rpc_zmq_bind_address' which accepts a wildcard, IP address, or Ethernet adapter.

This configuration can be set in [DEFAULT] section.

For example::

        rpc_zmq_bind_address = *

Currently zmq driver uses dynamic port binding mechanism, which means that
each listener will allocate port of a random number. Ports range is controlled
by two options 'rpc_zmq_min_port' and 'rpc_zmq_max_port'. Change them to
restrict current service's port binding range. 'rpc_zmq_bind_port_retries'
controls number of retries before 'ports range exceeded' failure.

For example::

        rpc_zmq_min_port = 9050
        rpc_zmq_max_port = 10050
        rpc_zmq_bind_port_retries = 100


DevStack Support
----------------

ZeroMQ driver has been supported by DevStack. The configuration is as follows::

        ENABLED_SERVICES+=,-rabbit,zeromq
        ZEROMQ_MATCHMAKER=redis

In local.conf [localrc] section need to enable zmq plugin which lives in
`devstack-plugin-zmq`_ repository.

For example::

    enable_plugin zmq https://github.com/openstack/devstack-plugin-zmq.git

.. _devstack-plugin-zmq: https://github.com/openstack/devstack-plugin-zmq.git


Current Status
--------------

The current development status of ZeroMQ driver is shown in `wiki`_.

.. _wiki: https://wiki.openstack.org/ZeroMQ
