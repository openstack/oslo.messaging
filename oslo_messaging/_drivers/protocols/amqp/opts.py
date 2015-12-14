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

    cfg.StrOpt('container_name',
               default=None,
               deprecated_group='amqp1',
               help='Name for the AMQP container'),

    cfg.IntOpt('idle_timeout',
               default=0,  # disabled
               deprecated_group='amqp1',
               help='Timeout for inactive connections (in seconds)'),

    cfg.BoolOpt('trace',
                default=False,
                deprecated_group='amqp1',
                help='Debug: dump AMQP frames to stdout'),

    cfg.StrOpt('ssl_ca_file',
               default='',
               deprecated_group='amqp1',
               help="CA certificate PEM file to verify server certificate"),

    cfg.StrOpt('ssl_cert_file',
               default='',
               deprecated_group='amqp1',
               help='Identifying certificate PEM file to present to clients'),

    cfg.StrOpt('ssl_key_file',
               default='',
               deprecated_group='amqp1',
               help='Private key PEM file used to sign cert_file certificate'),

    cfg.StrOpt('ssl_key_password',
               default=None,
               deprecated_group='amqp1',
               secret=True,
               help='Password for decrypting ssl_key_file (if encrypted)'),

    cfg.BoolOpt('allow_insecure_clients',
                default=False,
                deprecated_group='amqp1',
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

    cfg.StrOpt('username',
               default='',
               deprecated_group='amqp1',
               help='User name for message broker authentication'),

    cfg.StrOpt('password',
               default='',
               deprecated_group='amqp1',
               secret=True,
               help='Password for message broker authentication')
]
