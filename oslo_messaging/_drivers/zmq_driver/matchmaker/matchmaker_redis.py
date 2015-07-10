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
import redis

from oslo_messaging._drivers.zmq_driver.matchmaker import base


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

        self._redis = redis.StrictRedis(
            host=self.conf.matchmaker_redis.host,
            port=self.conf.matchmaker_redis.port,
            password=self.conf.matchmaker_redis.password,
        )

    def register(self, topic, hostname):
        if hostname not in self.get_hosts(topic):
            self._redis.lpush(topic, hostname)

    def get_hosts(self, topic):
        return self._redis.lrange(topic, 0, -1)[::-1]
