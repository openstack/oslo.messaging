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

import logging

from oslo_config import cfg
from oslo_utils import importutils

from oslo_messaging._drivers.zmq_driver.matchmaker import base
from oslo_messaging._drivers.zmq_driver import zmq_address

redis = importutils.try_import('redis')
LOG = logging.getLogger(__name__)


matchmaker_redis_opts = [
    cfg.StrOpt('host',
               default='127.0.0.1',
               help='Host to locate redis.'),
    cfg.IntOpt('port',
               default=6379,
               help='Use this port to connect to redis host.'),
    cfg.StrOpt('password',
               default='',
               secret=True,
               help='Password for Redis server (optional).'),
]


class RedisMatchMaker(base.MatchMakerBase):

    def __init__(self, conf, *args, **kwargs):
        super(RedisMatchMaker, self).__init__(conf, *args, **kwargs)
        self.conf.register_opts(matchmaker_redis_opts, "matchmaker_redis")

        self._redis = redis.StrictRedis(
            host=self.conf.matchmaker_redis.host,
            port=self.conf.matchmaker_redis.port,
            password=self.conf.matchmaker_redis.password,
        )

    def _get_hosts_by_key(self, key):
        return self._redis.lrange(key, 0, -1)

    def register(self, target, hostname, listener_type):

        if target.topic and target.server:
            key = zmq_address.target_to_key(target, listener_type)
            if hostname not in self._get_hosts_by_key(key):
                self._redis.lpush(key, hostname)

        if target.topic:
            key = zmq_address.prefix_str(target.topic, listener_type)
            if hostname not in self._get_hosts_by_key(key):
                self._redis.lpush(key, hostname)

        if target.server:
            key = zmq_address.prefix_str(target.server, listener_type)
            if hostname not in self._get_hosts_by_key(key):
                self._redis.lpush(key, hostname)

    def unregister(self, target, hostname, listener_type):
        key = zmq_address.target_to_key(target, listener_type)
        self._redis.lrem(key, 0, hostname)

    def get_hosts(self, target, listener_type):
        hosts = []
        key = zmq_address.target_to_key(target, listener_type)
        hosts.extend(self._get_hosts_by_key(key))
        return hosts
