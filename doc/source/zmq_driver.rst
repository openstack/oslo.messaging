------------------------------
ZeroMQ Driver Deployment Guide
------------------------------

.. currentmodule:: oslo_messaging

============
Introduction
============

0MQ (also known as ZeroMQ or zmq) looks like an embeddable
networking library but acts like a concurrency framework. It gives
you sockets that carry atomic messages across various transports
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

Other than AMQP-based drivers, like RabbitMQ or Qpid, ZeroMQ doesn't have
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
-------------------------

The ZeroMQ driver implements a matching capability to discover hosts available
for communication when sending to a bare topic. This allows broker-less
communications.

The MatchMaker is pluggable and it provides two different MatchMaker classes.

MatchMakerLocalhost: default matchmaker driver for all-in-one scenario (messages
are sent to itself).

MatchMakerRing: loads a static hash table from a JSON file, sends messages to
a certain host via directed topics or cycles hosts per bare topic and supports
broker-less fanout messaging. On fanout messages returns an array of directed
topics (messages are sent to all destinations).

MatchMakerRedis: loads the hash table from a remote Redis server, supports
dynamic host/topic registrations, host expiration, and hooks for consuming
applications to acknowledge or neg-acknowledge topic.host service availability.

To set the MatchMaker class, use option 'rpc_zmq_matchmaker' in [DEFAULT].

        rpc_zmq_matchmaker = local
        or
        rpc_zmq_matchmaker = ring
        or
        rpc_zmq_matchmaker = redis

To specify the ring file for MatchMakerRing, use option 'ringfile' in
[matchmaker_ring].

For example::

        [matchmaker_ring]
        ringfile = /etc/oslo/oslo_matchmaker_ring.json

To specify the Redis server for MatchMakerRedis, use options in
[matchmaker_redis] of each project.

        [matchmaker_redis]
        host = 127.0.0.1
        port = 6379
        password = None

MatchMaker Data Source (mandatory)
-----------------------------------

MatchMaker data source is stored in files or Redis server discussed in the
previous section. How to make up the database is the key issue for making ZeroMQ
driver work.

If deploying the MatchMakerRing, a ring file is required. The format of the ring
file should contain a hash where each key is a base topic and the values are
hostname arrays to be sent to.

For example::

        /etc/oslo/oslo_matchmaker_ring.json
        {
            "scheduler": ["host1", "host2"],
            "conductor": ["host1", "host2"],
        }

The AMQP-based methods like RabbitMQ and Qpid don't require any knowledge
about the source and destination of any topic. However, ZeroMQ driver
with MatchMakerRing does. The challenging task is that you should learn
and get all the (K, V) pairs from each OpenStack project to make up the
matchmaker ring file.

If deploying the MatchMakerRedis, a Redis server is required. Each (K, V) pair
stored in Redis is that the key is a base topic and the corresponding values are
hostname arrays to be sent to.

Message Receivers (mandatory)
-------------------------------

Each machine running OpenStack services, or sending RPC messages, must run the
'oslo-messaging-zmq-receiver' daemon. This receives replies to call requests and
routes responses via IPC to blocked callers.

The way that deploy the receiver process is to run it under a new user 'oslo'
and give all openstack daemons access via group membership of 'oslo' - this
supports using /var/run/openstack as a shared IPC directory for all openstack
processes, allowing different services to be hosted on the same server, served
by a single oslo-messaging-zmq-reciever process.

The IPC runtime directory, 'rpc_zmq_ipc_dir', can be set in [DEFAULT] section.

For example::

        rpc_zmq_ipc_dir = /var/run/openstack

The parameters for the script oslo-messaging-zmq-receiver should be::

        oslo-messaging-zmq-receiver
            --config-file /etc/oslo/zeromq.conf
            --log-file /var/log/oslo/zmq-receiver.log

You can specify ZeroMQ options in /etc/oslo/zeromq.conf if necessary.

Listening Ports (mandatory)
---------------------------

The ZeroMQ driver uses TCP to communicate. The port is configured with
'rpc_zmq_port' in [DEFAULT] section of each project, which defaults to 9501.

For example::

        rpc_zmq_port = 9501

Thread Pool (optional)
-----------------------

Each service will launch threads for incoming requests. These threads are
maintained via a pool, the maximum number of threads is limited by
rpc_thread_pool_size. The default value is 1024. (This is a common RPC
configuration variable, also applicable to Kombu and Qpid)

This configuration can be set in [DEFAULT] section.

For example::

        rpc_thread_pool_size = 1024

Listening Address (optional)
------------------------------

All services bind to an IP address or Ethernet adapter. By default, all services
bind to '*', effectively binding to 0.0.0.0. This may be changed with the option
'rpc_zmq_bind_address' which accepts a wildcard, IP address, or Ethernet adapter.

This configuration can be set in [DEFAULT] section.

For example::

        rpc_zmq_bind_address = *

DevStack Support
----------------

ZeroMQ driver has been supported by DevStack. The configuration is as follows::

        ENABLED_SERVICES+=,-rabbit,-qpid,zeromq
        ZEROMQ_MATCHMAKER=redis

Current Status
---------------

The current development status of ZeroMQ driver is shown in `wiki`_.

.. _wiki: https://wiki.openstack.org/ZeroMQ

