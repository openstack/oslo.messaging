#    Copyright 2016 Mirantis, Inc.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import socket

from oslo_config import cfg

from oslo_messaging._drivers import base
from oslo_messaging._drivers import common
from oslo_messaging import server


MATCHMAKER_BACKENDS = ('redis', 'sentinel', 'dummy')
MATCHMAKER_DEFAULT = 'redis'


zmq_opts = [
    cfg.StrOpt('rpc_zmq_bind_address', default='*',
               deprecated_group='DEFAULT',
               help='ZeroMQ bind address. Should be a wildcard (*), '
                    'an ethernet interface, or IP. '
                    'The "host" option should point or resolve to this '
                    'address.'),

    cfg.StrOpt('rpc_zmq_matchmaker', default=MATCHMAKER_DEFAULT,
               choices=MATCHMAKER_BACKENDS,
               deprecated_group='DEFAULT',
               help='MatchMaker driver.'),

    cfg.IntOpt('rpc_zmq_contexts', default=1,
               deprecated_group='DEFAULT',
               help='Number of ZeroMQ contexts, defaults to 1.'),

    cfg.IntOpt('rpc_zmq_topic_backlog',
               deprecated_group='DEFAULT',
               help='Maximum number of ingress messages to locally buffer '
                    'per topic. Default is unlimited.'),

    cfg.StrOpt('rpc_zmq_ipc_dir', default='/var/run/openstack',
               deprecated_group='DEFAULT',
               help='Directory for holding IPC sockets.'),

    cfg.StrOpt('rpc_zmq_host', default=socket.gethostname(),
               sample_default='localhost',
               deprecated_group='DEFAULT',
               help='Name of this node. Must be a valid hostname, FQDN, or '
                    'IP address. Must match "host" option, if running Nova.'),

    cfg.IntOpt('zmq_linger', default=-1,
               deprecated_group='DEFAULT',
               deprecated_name='rpc_cast_timeout',
               help='Number of seconds to wait before all pending '
                    'messages will be sent after closing a socket. '
                    'The default value of -1 specifies an infinite linger '
                    'period. The value of 0 specifies no linger period. '
                    'Pending messages shall be discarded immediately '
                    'when the socket is closed. Positive values specify an '
                    'upper bound for the linger period.'),

    cfg.IntOpt('rpc_poll_timeout', default=1,
               deprecated_group='DEFAULT',
               help='The default number of seconds that poll should wait. '
                    'Poll raises timeout exception when timeout expired.'),

    cfg.IntOpt('zmq_target_expire', default=300,
               deprecated_group='DEFAULT',
               help='Expiration timeout in seconds of a name service record '
                    'about existing target ( < 0 means no timeout).'),

    cfg.IntOpt('zmq_target_update', default=180,
               deprecated_group='DEFAULT',
               help='Update period in seconds of a name service record '
                    'about existing target.'),

    cfg.BoolOpt('use_pub_sub', default=False,
                deprecated_group='DEFAULT',
                help='Use PUB/SUB pattern for fanout methods. '
                     'PUB/SUB always uses proxy.'),

    cfg.BoolOpt('use_router_proxy', default=False,
                deprecated_group='DEFAULT',
                help='Use ROUTER remote proxy.'),

    cfg.BoolOpt('use_dynamic_connections', default=False,
                help='This option makes direct connections dynamic or static. '
                     'It makes sense only with use_router_proxy=False which '
                     'means to use direct connections for direct message '
                     'types (ignored otherwise).'),

    cfg.IntOpt('zmq_failover_connections', default=2,
               help='How many additional connections to a host will be made '
                    'for failover reasons. This option is actual only in '
                    'dynamic connections mode.'),

    cfg.PortOpt('rpc_zmq_min_port',
                default=49153,
                deprecated_group='DEFAULT',
                help='Minimal port number for random ports range.'),

    cfg.IntOpt('rpc_zmq_max_port',
               min=1,
               max=65536,
               default=65536,
               deprecated_group='DEFAULT',
               help='Maximal port number for random ports range.'),

    cfg.IntOpt('rpc_zmq_bind_port_retries',
               default=100,
               deprecated_group='DEFAULT',
               help='Number of retries to find free port number before '
                    'fail with ZMQBindError.'),

    cfg.StrOpt('rpc_zmq_serialization', default='json',
               choices=('json', 'msgpack'),
               deprecated_group='DEFAULT',
               help='Default serialization mechanism for '
                    'serializing/deserializing outgoing/incoming messages'),

    cfg.BoolOpt('zmq_immediate', default=True,
                help='This option configures round-robin mode in zmq socket. '
                     'True means not keeping a queue when server side '
                     'disconnects. False means to keep queue and messages '
                     'even if server is disconnected, when the server '
                     'appears we send all accumulated messages to it.'),

    cfg.IntOpt('zmq_tcp_keepalive', default=-1,
               help='Enable/disable TCP keepalive (KA) mechanism. '
                    'The default value of -1 (or any other negative value) '
                    'means to skip any overrides and leave it to OS default; '
                    '0 and 1 (or any other positive value) mean to '
                    'disable and enable the option respectively.'),

    cfg.IntOpt('zmq_tcp_keepalive_idle', default=-1,
               help='The duration between two keepalive transmissions in '
                    'idle condition. '
                    'The unit is platform dependent, for example, '
                    'seconds in Linux, milliseconds in Windows etc. '
                    'The default value of -1 (or any other negative value '
                    'and 0) means to skip any overrides and leave it '
                    'to OS default.'),

    cfg.IntOpt('zmq_tcp_keepalive_cnt', default=-1,
               help='The number of retransmissions to be carried out before '
                    'declaring that remote end is not available. '
                    'The default value of -1 (or any other negative value '
                    'and 0) means to skip any overrides and leave it '
                    'to OS default.'),

    cfg.IntOpt('zmq_tcp_keepalive_intvl', default=-1,
               help='The duration between two successive keepalive '
                    'retransmissions, if acknowledgement to the previous '
                    'keepalive transmission is not received. '
                    'The unit is platform dependent, for example, '
                    'seconds in Linux, milliseconds in Windows etc. '
                    'The default value of -1 (or any other negative value '
                    'and 0) means to skip any overrides and leave it '
                    'to OS default.'),

    cfg.IntOpt('rpc_thread_pool_size', default=100,
               help='Maximum number of (green) threads to work concurrently.'),

    cfg.IntOpt('rpc_message_ttl', default=300,
               help='Expiration timeout in seconds of a sent/received message '
                    'after which it is not tracked anymore by a '
                    'client/server.'),

    cfg.BoolOpt('rpc_use_acks', default=False,
                help='Wait for message acknowledgements from receivers. '
                     'This mechanism works only via proxy without PUB/SUB.'),

    cfg.IntOpt('rpc_ack_timeout_base', default=15,
               help='Number of seconds to wait for an ack from a cast/call. '
                    'After each retry attempt this timeout is multiplied by '
                    'some specified multiplier.'),

    cfg.IntOpt('rpc_ack_timeout_multiplier', default=2,
               help='Number to multiply base ack timeout by after each retry '
                    'attempt.'),

    cfg.IntOpt('rpc_retry_attempts', default=3,
               help='Default number of message sending attempts in case '
                    'of any problems occurred: positive value N means '
                    'at most N retries, 0 means no retries, None or -1 '
                    '(or any other negative values) mean to retry forever. '
                    'This option is used only if acknowledgments are '
                    'enabled.'),

    cfg.ListOpt('subscribe_on',
                default=[],
                help='List of publisher hosts SubConsumer can subscribe on. '
                     'This option has higher priority then the default '
                     'publishers list taken from the matchmaker.'),
]


def register_opts(conf, url):
    opt_group = cfg.OptGroup(name='oslo_messaging_zmq',
                             title='ZeroMQ driver options')
    conf.register_opts(zmq_opts, group=opt_group)
    conf.register_opts(server._pool_opts)
    conf.register_opts(base.base_opts)
    return common.ConfigOptsProxy(conf, url, opt_group.name)
