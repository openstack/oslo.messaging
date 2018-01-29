#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

from oslo_config import cfg

from oslo_messaging._drivers import common

KAFKA_OPTS = [
    cfg.StrOpt('kafka_default_host', default='localhost',
               deprecated_for_removal=True,
               deprecated_reason="Replaced by [DEFAULT]/transport_url",
               help='Default Kafka broker Host'),

    cfg.PortOpt('kafka_default_port', default=9092,
                deprecated_for_removal=True,
                deprecated_reason="Replaced by [DEFAULT]/transport_url",
                help='Default Kafka broker Port'),

    cfg.IntOpt('kafka_max_fetch_bytes', default=1024 * 1024,
               help='Max fetch bytes of Kafka consumer'),

    cfg.FloatOpt('kafka_consumer_timeout', default=1.0,
                 help='Default timeout(s) for Kafka consumers'),

    cfg.IntOpt('pool_size', default=10,
               deprecated_for_removal=True,
               deprecated_reason='Driver no longer uses connection pool. ',
               help='Pool Size for Kafka Consumers'),

    cfg.IntOpt('conn_pool_min_size', default=2,
               deprecated_for_removal=True,
               deprecated_reason='Driver no longer uses connection pool. ',
               help='The pool size limit for connections expiration policy'),

    cfg.IntOpt('conn_pool_ttl', default=1200,
               deprecated_for_removal=True,
               deprecated_reason='Driver no longer uses connection pool. ',
               help='The time-to-live in sec of idle connections in the pool'),

    cfg.StrOpt('consumer_group', default="oslo_messaging_consumer",
               help='Group id for Kafka consumer. Consumers in one group '
                    'will coordinate message consumption'),

    cfg.FloatOpt('producer_batch_timeout', default=0.,
                 help="Upper bound on the delay for KafkaProducer batching "
                      "in seconds"),

    cfg.IntOpt('producer_batch_size', default=16384,
               help='Size of batch for the producer async send'),

    cfg.BoolOpt('enable_auto_commit',
                default=False,
                help='Enable asynchronous consumer commits'),

    cfg.IntOpt('max_poll_records', default=500,
               help='The maximum number of records returned in a poll call'),

    cfg.StrOpt('security_protocol', default='PLAINTEXT',
               choices=('PLAINTEXT', 'SASL_PLAINTEXT', 'SSL', 'SASL_SSL'),
               help='Protocol used to communicate with brokers'),

    cfg.StrOpt('sasl_mechanism',
               default='PLAIN',
               help='Mechanism when security protocol is SASL'),

    cfg.StrOpt('ssl_cafile',
               default='',
               help='CA certificate PEM file used to verify the server'
               ' certificate')
]


def register_opts(conf, url):
    opt_group = cfg.OptGroup(name='oslo_messaging_kafka',
                             title='Kafka driver options')
    conf.register_group(opt_group)
    conf.register_opts(KAFKA_OPTS, group=opt_group)
    return common.ConfigOptsProxy(conf, url, opt_group.name)
