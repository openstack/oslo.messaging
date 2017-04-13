#    Copyright 2014, Red Hat, Inc.
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

from oslo_config import cfg


amqp1_opts = [
    cfg.StrOpt('container_name',
               deprecated_group='amqp1',
               help='Name for the AMQP container. must be globally unique.'
                    ' Defaults to a generated UUID'),

    cfg.IntOpt('idle_timeout',
               default=0,  # disabled
               deprecated_group='amqp1',
               help='Timeout for inactive connections (in seconds)'),

    cfg.BoolOpt('trace',
                default=False,
                deprecated_group='amqp1',
                help='Debug: dump AMQP frames to stdout'),

    cfg.BoolOpt('ssl',
                default=False,
                help=("Attempt to connect via SSL. If no other ssl-related "
                      "parameters are given, it will use the system's "
                      "CA-bundle to verify the server's certificate.")),

    cfg.StrOpt('ssl_ca_file',
               default='',
               deprecated_group='amqp1',
               help="CA certificate PEM file used to verify the server's"
               ' certificate'),

    cfg.StrOpt('ssl_cert_file',
               default='',
               deprecated_group='amqp1',
               help='Self-identifying certificate PEM file'
               ' for client authentication'),

    cfg.StrOpt('ssl_key_file',
               default='',
               deprecated_group='amqp1',
               help='Private key PEM file used to sign ssl_cert_file'
               ' certificate (optional)'),

    cfg.StrOpt('ssl_key_password',
               deprecated_group='amqp1',
               secret=True,
               help='Password for decrypting ssl_key_file (if encrypted)'),

    cfg.BoolOpt('allow_insecure_clients',
                default=False,
                deprecated_group='amqp1',
                # marked as deprecated in Ocata
                deprecated_for_removal=True,
                deprecated_reason="Not applicable - not a SSL server",
                help='Accept clients using either SSL or plain TCP'),

    cfg.StrOpt('sasl_mechanisms',
               default='',
               deprecated_group='amqp1',
               help='Space separated list of acceptable SASL mechanisms'),

    cfg.StrOpt('sasl_config_dir',
               default='',
               deprecated_group='amqp1',
               help='Path to directory that contains the SASL configuration'),

    cfg.StrOpt('sasl_config_name',
               default='',
               deprecated_group='amqp1',
               help='Name of configuration file (without .conf suffix)'),

    cfg.StrOpt('sasl_default_realm',
               default='',
               help='SASL realm to use if no realm present in username'),

    cfg.StrOpt('username',
               default='',
               deprecated_group='amqp1',
               deprecated_for_removal=True,
               deprecated_reason='Should use configuration option '
                                 'transport_url to provide the username.',
               help='User name for message broker authentication'),

    cfg.StrOpt('password',
               default='',
               deprecated_group='amqp1',
               secret=True,
               deprecated_for_removal=True,
               deprecated_reason='Should use configuration option '
                                 'transport_url to provide the password.',
               help='Password for message broker authentication'),

    # Network connection failure retry options

    cfg.IntOpt('connection_retry_interval',
               default=1,
               min=1,
               help='Seconds to pause before attempting to re-connect.'),

    cfg.IntOpt('connection_retry_backoff',
               default=2,
               min=0,
               help='Increase the connection_retry_interval by this many'
               ' seconds after each unsuccessful failover attempt.'),

    cfg.IntOpt('connection_retry_interval_max',
               default=30,
               min=1,
               help='Maximum limit for connection_retry_interval'
                    ' + connection_retry_backoff'),

    # Message send retry and timeout options

    cfg.IntOpt('link_retry_delay',
               default=10,
               min=1,
               help='Time to pause between re-connecting an AMQP 1.0 link that'
               ' failed due to a recoverable error.'),

    cfg.IntOpt('default_reply_retry',
               default=0,
               min=-1,
               help='The maximum number of attempts to re-send a reply message'
               ' which failed due to a recoverable error.'),

    cfg.IntOpt('default_reply_timeout',
               default=30,
               min=5,
               help='The deadline for an rpc reply message delivery.'),

    cfg.IntOpt('default_send_timeout',
               default=30,
               min=5,
               help='The deadline for an rpc cast or call message delivery.'
               ' Only used when caller does not provide a timeout expiry.'),

    cfg.IntOpt('default_notify_timeout',
               default=30,
               min=5,
               help='The deadline for a sent notification message delivery.'
               ' Only used when caller does not provide a timeout expiry.'),

    # Sender link cache maintenance:
    cfg.IntOpt('default_sender_link_timeout',
               default=600,
               min=1,
               help='The duration to schedule a purge of idle sender links.'
               ' Detach link after expiry.'),

    # Addressing:

    cfg.StrOpt('addressing_mode',
               default='dynamic',
               help="Indicates the addressing mode used by the driver.\n"
               "Permitted values:\n"
               "'legacy'   - use legacy non-routable addressing\n"
               "'routable' - use routable addresses\n"
               "'dynamic'  - use legacy addresses if the message bus does not"
               " support routing otherwise use routable addressing"),

    # Legacy addressing customization:

    cfg.StrOpt('server_request_prefix',
               default='exclusive',
               deprecated_group='amqp1',
               help="address prefix used when sending to a specific server"),

    cfg.StrOpt('broadcast_prefix',
               default='broadcast',
               deprecated_group='amqp1',
               help="address prefix used when broadcasting to all servers"),

    cfg.StrOpt('group_request_prefix',
               default='unicast',
               deprecated_group='amqp1',
               help="address prefix when sending to any server in group"),

    # Routable addressing customization:
    #
    # Addresses a composed of the following string values using a template in
    # the form of:
    # $(address_prefix)/$(*cast)/$(exchange)/$(topic)[/$(server-name)]
    # where *cast is one of the multicast/unicast/anycast values used to
    # identify the delivery pattern used for the addressed message

    cfg.StrOpt('rpc_address_prefix',
               default='openstack.org/om/rpc',
               help="Address prefix for all generated RPC addresses"),

    cfg.StrOpt('notify_address_prefix',
               default='openstack.org/om/notify',
               help="Address prefix for all generated Notification addresses"),

    cfg.StrOpt('multicast_address',
               default='multicast',
               help="Appended to the address prefix when sending a fanout"
               " message. Used by the message bus to identify fanout"
               " messages."),

    cfg.StrOpt('unicast_address',
               default='unicast',
               help="Appended to the address prefix when sending to a"
               " particular RPC/Notification server. Used by the message bus"
               " to identify messages sent to a single destination."),

    cfg.StrOpt('anycast_address',
               default='anycast',
               help="Appended to the address prefix when sending to a group of"
               " consumers. Used by the message bus to identify messages that"
               " should be delivered in a round-robin fashion across"
               " consumers."),

    cfg.StrOpt('default_notification_exchange',
               help="Exchange name used in notification addresses.\n"
               "Exchange name resolution precedence:\n"
               "Target.exchange if set\n"
               "else default_notification_exchange if set\n"
               "else control_exchange if set\n"
               "else 'notify'"),

    cfg.StrOpt('default_rpc_exchange',
               help="Exchange name used in RPC addresses.\n"
               "Exchange name resolution precedence:\n"
               "Target.exchange if set\n"
               "else default_rpc_exchange if set\n"
               "else control_exchange if set\n"
               "else 'rpc'"),

    # Message Credit Levels

    cfg.IntOpt('reply_link_credit',
               default=200,
               min=1,
               help='Window size for incoming RPC Reply messages.'),

    cfg.IntOpt('rpc_server_credit',
               default=100,
               min=1,
               help='Window size for incoming RPC Request messages'),

    cfg.IntOpt('notify_server_credit',
               default=100,
               min=1,
               help='Window size for incoming Notification messages'),

    # Settlement control

    cfg.MultiStrOpt('pre_settled',
                    default=['rpc-cast', 'rpc-reply'],
                    help="Send messages of this type pre-settled.\n"
                    "Pre-settled messages will not receive acknowledgement\n"
                    "from the peer. Note well: pre-settled messages may be\n"
                    "silently discarded if the delivery fails.\n"
                    "Permitted values:\n"
                    "'rpc-call' - send RPC Calls pre-settled\n"
                    "'rpc-reply'- send RPC Replies pre-settled\n"
                    "'rpc-cast' - Send RPC Casts pre-settled\n"
                    "'notify'   - Send Notifications pre-settled\n")
]
