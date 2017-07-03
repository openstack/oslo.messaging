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

Other than AMQP-based drivers, like RabbitMQ, default ZeroMQ doesn't have
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


===================
Basic Configuration
===================

Enabling (mandatory)
--------------------

To enable the driver the 'transport_url' option must be set to 'zmq://'
in the section [DEFAULT] of the conf file, the 'rpc_zmq_host' option
must be set to the hostname of the current node. ::

        [DEFAULT]
        transport_url = "zmq://"

        [oslo_messaging_zmq]
        rpc_zmq_host = {hostname}

Default configuration of zmq driver is called 'Static Direct Connections' (To
learn more about zmq driver configurations please proceed to the corresponding
section 'Existing Configurations'). That means that all services connect
directly to each other and all connections are static so we open them at the
beginning of service's lifecycle and close them only when service quits. This
configuration is the simplest one since it doesn't require any helper services
(proxies) other than matchmaker to be running.


Matchmaking (mandatory)
-----------------------

The ZeroMQ driver implements a matching capability to discover hosts available
for communication when sending to a bare topic. This allows broker-less
communications.

The Matchmaker is pluggable and it provides two different Matchmaker classes.

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
which is 300 (seconds) by default. The option is related not specifically to
redis so it is also defined in [oslo_messaging_zmq] section. If option value
is <= 0 then keys don't expire and live forever in the storage.

The other option is 'zmq_target_update' (180 seconds by default) which
specifies how often each RPC-Server should update the matchmaker. This option's
optimal value generally is zmq_target_expire / 2 (or 1.5). It is recommended to
calculate it based on 'zmq_target_expire' so services records wouldn't expire
earlier than being updated from alive services.

Generally matchmaker can be considered as an alternate approach to services
heartbeating.


Matchmaker Data Source (mandatory)
----------------------------------

Matchmaker data source is stored in files or Redis server discussed in the
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
        transport_url = "zmq+sentinel://host1:26379,host2:26379,host3:26379"


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

        rpc_zmq_min_port = 49153
        rpc_zmq_max_port = 65536
        rpc_zmq_bind_port_retries = 100


=======================
Existing Configurations
=======================


Static Direct Connections
-------------------------

The example of service config file::

    [DEFAULT]
    transport_url = "zmq+redis://host-1:6379"

    [oslo_messaging_zmq]
    use_pub_sub = false
    use_router_proxy = false
    use_dynamic_connections = false
    zmq_target_expire = 60
    zmq_target_update = 30
    rpc_zmq_min_port = 49153
    rpc_zmq_max_port = 65536

In both static and dynamic direct connections configuration it is necessary to
configure firewall to open binding port range on each node::

    iptables -A INPUT -p tcp --match multiport --dports 49152:65535 -j ACCEPT


The sequrity recommendation here (it is general for any RPC backend) is to
setup private network for message bus and another open network for public APIs.
ZeroMQ driver doesn't support authentication and encryption on its level.

As stated above this configuration is the simplest one since it requires only a
Matchmaker service to be running. That is why driver's options configured by
default in a way to use this type of topology.

The biggest advantage of static direct connections (other than simplicity) is
it's huge performance. On small deployments (20 - 50 nodes) it can outperform
brokered solutions (or solutions with proxies) 3x - 5x times. It becomes possible
because this configuration doesn't have a central node bottleneck so it's
throughput is limited by only a TCP and network bandwidth.

Unfortunately this approach can not be applied as is on a big scale (over 500 nodes).
The main problem is the number of connections between services and particularly
the number of connections on each controller node grows (in a worst case) as
a square function of number of the whole running services. That's not
appropriate.

However this approach can be successfully used and is recommended to be used
when services on controllers doesn't talk to agent services on resource nodes
using oslo.messaging RPC, but RPC is used only to communicate controller
services between each other.

Examples here may be Cinder+Ceph backend and Ironic how it utilises
oslo.messaging.

For all the other cases like Nova and Neutron on a big scale using proxy-based
configurations or dynamic connections configuration is more appropriate.

The exception here may be the case when using OpenStack services inside Docker
containers with Kubernetes. Since Kubernetes already solves similar problems by
using KubeProxy and virtual IP addresses for each container. So it manages all
the traffic using iptables which is more than appropriate to solve the problem
described above.

Summing up it is recommended to use this type of zmq configuration for

1. Small clouds (up to 100 nodes)
2. Cinder+Ceph deployment
3. Ironic deployment
4. OpenStack + Kubernetes (OpenStack in containers) deployment


Dynamic Direct Connections
--------------------------
The example of service config file::

    [DEFAULT]
    transport_url = "zmq+redis://host-1:6379"

    [oslo_messaging_zmq]
    use_pub_sub = false
    use_router_proxy = false

    use_dynamic_connections = true
    zmq_failover_connections = 2
    zmq_linger = 60

    zmq_target_expire = 60
    zmq_target_update = 30
    rpc_zmq_min_port = 49153
    rpc_zmq_max_port = 65536

The 'use_dynamic_connections = true' obviously states that connections are dynamic.
'zmq_linger' become crucial with dynamic connections in order to avoid socket
leaks. If socket being connected to a wrong (dead) host which somehow still
present in the Matchmaker and message was sent, then the socket can not be closed
until message stays in the queue (the default linger is infinite waiting). So
need to specify linger explicitly.

Services often run more than one worker on the same topic. Workers are equal, so
any can handle the message. In order to connect to more than one available worker
need to setup 'zmq_failover_connections' option to some value (2 by default which
means 2 additional connections). Take care because it may also result in slow-down.

All recommendations regarding port ranges described in previous section are also
valid here.

Most things are similar to what we had with static connections the only
difference is that each message causes connection setup and disconnect afterwards
immediately after message was sent.

The advantage of this deployment is that average number of connections on
controller node at any moment is not high even for quite large deployments.

The disadvantage is overhead caused by need to connect/disconnect per message.
So this configuration can with no doubt be considered as the slowest one. The
good news is the RPC of OpenStack doesn't require "thousands message per second"
bandwidth per each particular service (do not confuse with central broker/proxy
bandwidth which is needed as high as possible for a big scale and can be a
serious bottleneck).

One more bad thing about this particular configuration is fanout. Here it is
completely linear complexity operation and it suffers the most from
connect/disconnect overhead per message. So for fanout it is fair to say that
services can have significant slow-down with dynamic connections.

The recommended way to solve this problem is to use combined solution with
proxied PUB/SUB infrastructure for fanout and dynamic direct connections for
direct message types (plain CAST and CALL messages). This combined approach
will be described later in the text.


Router Proxy
------------

The example of service config file::

    [DEFAULT]
    transport_url = "zmq+redis://host-1:6379"

    [oslo_messaging_zmq]
    use_pub_sub = false
    use_router_proxy = true
    use_dynamic_connections = false

The example of proxy config file::

    [DEFAULT]
    transport_url = "zmq+redis://host-1:6379"

    [oslo_messaging_zmq]
    use_pub_sub = false

    [zmq_proxy_opt]
    host = host-1

RPC may consume too many TCP sockets on controller node in directly connected
configuration. To solve the issue ROUTER proxy may be used.

In order to configure driver to use ROUTER proxy set up the 'use_router_proxy'
option to true in [oslo_messaging_zmq] section (false is set by default).

Pay attention to 'use_pub_sub = false' line, which has to match for all
services and proxies configs, so it wouldn't work if proxy uses PUB/SUB and
services don't.

Not less than 3 proxies should be running on controllers or on stand alone
nodes. The parameters for the script oslo-messaging-zmq-proxy should be::

        oslo-messaging-zmq-proxy
            --config-file /etc/oslo/zeromq.conf
            --log-file /var/log/oslo/zeromq-router-proxy.log
            --host node-123
            --frontend-port 50001
            --backend-port 50002
            --debug

Config file for proxy consists of default section, 'oslo_messaging_zmq' section
and additional 'zmq_proxy_opts' section.

Command line arguments like host, frontend_port, backend_port and publisher_port
respectively can also be set in 'zmq_proxy_opts' section of a configuration
file (i.e., /etc/oslo/zeromq.conf). All arguments are optional.

Port value of 0 means random port (see the next section for more details).

Take into account that --debug flag makes proxy to make a log record per every
dispatched message which influences proxy performance significantly. So it is
not recommended flag to use in production. Without --debug there will be only
Matchmaker updates or critical errors in proxy logs.

In this configuration we use proxy as a very simple dispatcher (so it has the
best performance with minimal overhead). The only thing proxy does is getting
binary routing-key frame from the message and dispatch message on this key.

In this kind of deployment client is in charge of doing fanout. Before sending
fanout message client takes a list of available hosts for the topic and sends
as many messages as the number of hosts it got.

This configuration just uses DEALER/ROUTER pattern of ZeroMQ and doesn't use
PUB/SUB as it was stated above.

Disadvantage of this approach is again slower client fanout. But it is much
better than with dynamic direct connections because we don't need to connect
and disconnect per each message.


ZeroMQ PUB/SUB Infrastructure
-----------------------------

The example of service config file::

    [DEFAULT]
    transport_url = "zmq+redis://host-1:6379"

    [oslo_messaging_zmq]
    use_pub_sub = true
    use_router_proxy = true
    use_dynamic_connections = false

The example of proxy config file::

    [DEFAULT]
    transport_url = "zmq+redis://host-1:6379"

    [oslo_messaging_zmq]
    use_pub_sub = true

    [zmq_proxy_opt]
    host = host-1

It seems obvious that fanout pattern of oslo.messaging maps on ZeroMQ PUB/SUB
pattern, but it is only at first glance. It does really, but lets look a bit
closer.

First caveat is that in oslo.messaging it is a client who makes fanout (and
generally initiates conversation), server is passive. While in ZeroMQ publisher
is a server and subscribers are clients. And here is the problem: RPC-servers
are subscribers in terms of ZeroMQ PUB/SUB, they hold the SUB socket and wait
for messages. And they don't know anything about RPC-clients, and clients
generally come later than servers. So servers don't have a PUB to subscribe
on start, so we need to introduce something in the middle, and here the proxy
plays the role.

Publisher proxy has ROUTER socket on the front-end and PUB socket on the back-end.
So client connects to ROUTER and sends a single message to a publisher proxy.
Proxy redirects this message to PUB socket which performs actual publishing.

Command to run central publisher proxy::

        oslo-messaging-zmq-proxy
            --config-file /etc/oslo/zeromq.conf
            --log-file /var/log/oslo/zeromq-router-proxy.log
            --host node-123
            --frontend-port 50001
            --publisher-port 50003
            --debug

When we run a publisher proxy we need to specify a --publisher-port option.
Random port will be picked up otherwise and clients will get it from the
Matchmaker.

The advantage of this approach is really fast fanout, while it takes time on
proxy to publish, but ZeroMQ PUB/SUB is one of the fastest fanout pattern
implementations. It also makes clients faster, because they need to send only a
single message to a proxy.

In order to balance load and HA it is recommended to have at least 3 proxies basically,
but the number of running proxies is not limited. They also don't form a cluster,
so there are no limitations on number caused by consistency algorithm requirements.

The disadvantage is that number of connections on proxy increased twice compared
to previous deployment, because we still need to use router for direct messages.

The documented limitation of ZeroMQ PUB/SUB is 10k subscribers.

In order to limit the number of subscribers and connections the local proxies
may be used. In order to run local publisher the following command may be used::


        oslo-messaging-zmq-proxy
            --local-publisher
            --config-file /etc/oslo/zeromq.conf
            --log-file /var/log/oslo/zeromq-router-proxy.log
            --host localhost
            --publisher-port 60001
            --debug

Pay attention to --local-publisher flag which specifies the type of a proxy.
Local publishers may be running on every single node of a deployment. To make
services use of local publishers the 'subscribe_on' option has to be specified
in service's config file::

    [DEFAULT]
    transport_url = "zmq+redis://host-1:6379"

    [oslo_messaging_zmq]
    use_pub_sub = true
    use_router_proxy = true
    use_dynamic_connections = false
    subscribe_on = localhost:60001

If we forgot to specify the 'subscribe_on' services will take info from Matchmaker
and still connect to a central proxy, so the trick wouldn't work. Local proxy
gets all the needed info from the matchmaker in order to find central proxies
and subscribes on them. Frankly speaking you can pub a central proxy in the
'subscribe_on' value, even a list of hosts may be passed the same way as we do
for the transport_url::

    subscribe_on = host-1:50003,host-2:50003,host-3:50003

This is completely valid, just not necessary because we have information about
central proxies in Matchmaker. One more thing to highlight about 'subscribe_on'
is that it has higher priority than Matchmaker if being explicitly mentioned.

Concluding all the above, fanout over PUB/SUB proxies is the best choice
because of static connections infrastructure, fail over when one or some publishers
die, and ZeroMQ PUB/SUB high performance.


What If Mix Different Configurations?
-------------------------------------

Three boolean variables 'use_pub_sub', 'use_router_proxy' and 'use_dynamic_connections'
give us exactly 8 possible combinations. But from practical perspective not all
of them are usable. So lets discuss only those which make sense.

The main recommended combination is Dynamic Direct Connections plus PUB/SUB
infrastructure. So we deploy PUB/SUB proxies as described in corresponding
paragraph (either with local+central proxies or with only a central proxies).
And the services configuration file will look like the following::

    [DEFAULT]
    transport_url = "zmq+redis://host-1:6379"

    [oslo_messaging_zmq]
    use_pub_sub = true
    use_router_proxy = false
    use_dynamic_connections = true

So we just tell the driver not to pass direct messages CALL and CAST over router,
but send them directly to RPC servers. All the details of configuring services
and port ranges has to be taken from 'Dynamic Direct Connections' paragraph.
So it's combined configuration. Currently it is the best choice from number of
connections perspective.

Frankly speaking, deployment from the 'ZeroMQ PUB/SUB Infrastructure' section is
also a combination of 'Router Proxy' with PUB/SUB, we've just used the same
proxies for both.

Here we've discussed combination inside the same service. But configurations can
also be combined on a higher level, a level of services. So you could have for
example a deployment where Cinder uses static direct connections and Nova/Neutron
use combined PUB/SUB + dynamic direct connections. But such approach needs additional
caution and may be confusing for cloud operators. Still it provides maximum
optimization of performance and number of connections on proxies and controller
nodes.


================
DevStack Support
================

ZeroMQ driver can be tested on a single node deployment with DevStack. Take
into account that on a single node it is not that obvious any performance
increase compared to other backends. To see significant speed up you need at least
20 nodes.

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
