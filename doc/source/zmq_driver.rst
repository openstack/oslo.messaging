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

Currently, ZeroMQ is one of the RPC backend drivers in oslo.messaging. ZeroMQ
can be the only RPC driver across the OpenStack cluster.
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
must be set to the hostname of the current node. ::

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

To set the MatchMaker class, use option 'rpc_zmq_matchmaker' in [DEFAULT]. ::

        rpc_zmq_matchmaker = dummy

or::

        rpc_zmq_matchmaker = redis

To specify the Redis server for RedisMatchMaker, use options in
[matchmaker_redis] of each project. ::

        [matchmaker_redis]
        host = 127.0.0.1
        port = 6379

In order to cleanup redis storage from expired records (e.g. target listener
goes down) TTL may be applied for keys. Configure 'zmq_target_expire' option
which is 120 (seconds) by default. The option is related not specifically to
redis so it is also defined in [DEFAULT] section. If option value is <= 0
then keys don't expire and live forever in the storage.

MatchMaker Data Source (mandatory)
----------------------------------

MatchMaker data source is stored in files or Redis server discussed in the
previous section. How to make up the database is the key issue for making ZeroMQ
driver work.

If deploying the RedisMatchMaker, a Redis server is required. Each (K, V) pair
stored in Redis is that the key is a base topic and the corresponding values are
hostname arrays to be sent to.


Proxy and huge number of TCP sockets
------------------------------------

The most heavily used RPC pattern (CALL) may consume too many TCP sockets in
directly connected configuration. To solve the issue ROUTER proxy may be used.
In order to configure driver to use ROUTER proxy set up the 'use_router_proxy'
option to True in [DEFAULT] section (False is set by default).

For example::

        use_router_proxy = True

Not less than 3 proxies should be running on controllers or on stand alone
nodes. The parameters for the script oslo-messaging-zmq-proxy should be::

        oslo-messaging-zmq-proxy
            --type ROUTER
            --config-file /etc/oslo/zeromq.conf
            --log-file /var/log/oslo/zmq-router-proxy.log


Proxy for fanout publishing
---------------------------

Fanout-based patterns like CAST+Fanout and notifications always use proxy
as they act over PUB/SUB, 'use_pub_sub' option defaults to True. In such case
publisher proxy should be running. Publisher-proxies are independent from each
other. Recommended number of proxies in the cloud is not less than 3. You
may run them on a standalone nodes or on controller nodes.
The parameters for the script oslo-messaging-zmq-proxy should be::

        oslo-messaging-zmq-proxy
            --type PUBLISHER
            --config-file /etc/oslo/zeromq.conf
            --log-file /var/log/oslo/zmq-publisher-proxy.log

Actually PUBLISHER is the default value for the parameter --type, so
could be omitted::

        oslo-messaging-zmq-proxy
            --config-file /etc/oslo/zeromq.conf
            --log-file /var/log/oslo/zmq-publisher-proxy.log

If not using PUB/SUB (use_pub_sub = False) then fanout will be emulated over
direct DEALER/ROUTER unicast which is possible but less efficient and therefore
is not recommended. In a case of direct DEALER/ROUTER unicast proxy is not
needed.

This option can be set in [DEFAULT] section.

For example::

        use_pub_sub = True


In case of using a proxy all publishers (clients) talk to servers over
the proxy connecting to it via TCP.

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
