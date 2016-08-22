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

Another option is to use a router proxy. It is not a broker because it
doesn't assume any message ownership or persistence or replication etc. It
performs only a redirection of messages to endpoints taking routing info from
message envelope.

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
          |  Cinder             |  |                        |
          |  Ceilometer         |  +------------------------+
          |  zmq-proxy          |
          |  Redis              |
          |  Horizon            |
          +---------------------+

=============
Configuration
=============

Enabling (mandatory)
--------------------

To enable the driver the 'transport_url' option must be set to 'zmq://'
in the section [DEFAULT] of the conf file, the 'rpc_zmq_host' flag
must be set to the hostname of the current node. ::

        [DEFAULT]
        transport_url = "zmq://"

        [oslo_messaging_zmq]
        rpc_zmq_host = {hostname}


Match Making (mandatory)
------------------------

The ZeroMQ driver implements a matching capability to discover hosts available
for communication when sending to a bare topic. This allows broker-less
communications.

The MatchMaker is pluggable and it provides two different MatchMaker classes.

MatchmakerDummy: default matchmaker driver for all-in-one scenario (messages
are sent to itself; used mainly for testing).

MatchmakerRedis: loads the hash table from a remote Redis server, supports
dynamic host/topic registrations, host expiration, and hooks for consuming
applications to acknowledge or neg-acknowledge topic.host service availability.

For ZeroMQ driver Redis is configured in transport_url also. For using Redis
specify the URL as follows::

        [DEFAULT]
        transport_url = "zmq+redis://127.0.0.1:6379"

In order to cleanup redis storage from expired records (e.g. target listener
goes down) TTL may be applied for keys. Configure 'zmq_target_expire' option
which is 120 (seconds) by default. The option is related not specifically to
redis so it is also defined in [oslo_messaging_zmq] section. If option value
is <= 0 then keys don't expire and live forever in the storage.

MatchMaker Data Source (mandatory)
----------------------------------

MatchMaker data source is stored in files or Redis server discussed in the
previous section. How to make up the database is the key issue for making ZeroMQ
driver work.

If deploying the MatchmakerRedis, a Redis server is required. Each (K, V) pair
stored in Redis is that the key is a base topic and the corresponding values are
hostname arrays to be sent to.


HA for Redis database
---------------------

Single node Redis works fine for testing, but for production there is some
availability guarantees wanted. Without Redis database zmq deployment should
continue working anyway, because there is no need in Redis for services when
connections established already. But if you would like to restart some services
or run more workers or add more hardware nodes to the deployment you will need
nodes discovery mechanism to work and it requires Redis.

To provide database recovery in situations when redis node goes down for example,
we use Sentinel solution and redis master-slave-slave configuration (if we have
3 controllers and run Redis on each of them).

To deploy redis with HA follow the `sentinel-install`_ instructions. From the
messaging driver's side you will need to setup following configuration ::

        [DEFAULT]
        transport_url = "zmq+redis://host1:26379,host2:26379,host3:26379"


Restrict the number of TCP sockets on controller
------------------------------------------------

The most heavily used RPC pattern (CALL) may consume too many TCP sockets on
controller node in directly connected configuration. To solve the issue
ROUTER proxy may be used.

In order to configure driver to use ROUTER proxy set up the 'use_router_proxy'
option to true in [oslo_messaging_zmq] section (false is set by default).

For example::

        use_router_proxy = true

Not less than 3 proxies should be running on controllers or on stand alone
nodes. The parameters for the script oslo-messaging-zmq-proxy should be::

        oslo-messaging-zmq-proxy
            --config-file /etc/oslo/zeromq.conf
            --log-file /var/log/oslo/zeromq-router-proxy.log
            --host node-123
            --frontend-port 50001
            --backend-port 50002
            --publisher-port 50003
            --debug True

Command line arguments like host, frontend_port, backend_port and publisher_port
respectively can also be set in [zmq_proxy_opts] section of a configuration
file (i.e., /etc/oslo/zeromq.conf). All arguments are optional.

Port value of 0 means random port (see the next section for more details).

Fanout-based patterns like CAST+Fanout and notifications always use proxy
as they act over PUB/SUB, 'use_pub_sub' option defaults to true. In such case
publisher proxy should be running. Actually proxy does both: routing to a
DEALER endpoint for direct messages and publishing to all subscribers over
zmq.PUB socket.

If not using PUB/SUB (use_pub_sub = false) then fanout will be emulated over
direct DEALER/ROUTER unicast which is possible but less efficient and therefore
is not recommended. In a case of direct DEALER/ROUTER unicast proxy is not
needed.

This option can be set in [oslo_messaging_zmq] section.

For example::

        use_pub_sub = true


In case of using a proxy all publishers (clients) talk to servers over
the proxy connecting to it via TCP.

You can specify ZeroMQ options in /etc/oslo/zeromq.conf if necessary.


Listening Address (optional)
----------------------------

All services bind to an IP address or Ethernet adapter. By default, all services
bind to '*', effectively binding to 0.0.0.0. This may be changed with the option
'rpc_zmq_bind_address' which accepts a wildcard, IP address, or Ethernet adapter.

This configuration can be set in [oslo_messaging_zmq] section.

For example::

        rpc_zmq_bind_address = *

Currently zmq driver uses dynamic port binding mechanism, which means that
each listener will allocate port of a random number (static, i.e. fixed, ports
may only be used for sockets inside proxies now). Ports range is controlled
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


Example of local.conf::

    [[local|localrc]]
    DATABASE_PASSWORD=password
    ADMIN_PASSWORD=password
    SERVICE_PASSWORD=password
    SERVICE_TOKEN=password

    enable_plugin zmq https://github.com/openstack/devstack-plugin-zmq.git

    OSLOMSG_REPO=https://review.openstack.org/openstack/oslo.messaging
    OSLOMSG_BRANCH=master

    ZEROMQ_MATCHMAKER=redis
    LIBS_FROM_GIT=oslo.messaging
    ENABLE_DEBUG_LOG_LEVEL=True


.. _devstack-plugin-zmq: https://github.com/openstack/devstack-plugin-zmq.git
.. _sentinel-install: http://redis.io/topics/sentinel
