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

import inspect
import logging

from oslo_config import cfg
from oslo_utils import importutils

from oslo_messaging._drivers.zmq_driver.matchmaker import base
from oslo_messaging._drivers.zmq_driver import zmq_address
from retrying import retry

redis = importutils.try_import('redis')
redis_sentinel = importutils.try_import('redis.sentinel')
LOG = logging.getLogger(__name__)


matchmaker_redis_opts = [
    cfg.StrOpt('host',
               default='127.0.0.1',
               help='Host to locate redis.'),
    cfg.PortOpt('port',
                default=6379,
                help='Use this port to connect to redis host.'),
    cfg.StrOpt('password',
               default='',
               secret=True,
               help='Password for Redis server (optional).'),
    cfg.ListOpt('sentinel_hosts',
                default=[],
                help='List of Redis Sentinel hosts (fault tolerance mode) e.g.\
                [host:port, host1:port ... ]'),
    cfg.StrOpt('sentinel_group_name',
               default='oslo-messaging-zeromq',
               help='Redis replica set name.'),
    cfg.IntOpt('wait_timeout',
               default=500,
               help='Time in ms to wait between connection attempts.'),
    cfg.IntOpt('check_timeout',
               default=20000,
               help='Time in ms to wait before the transaction is killed.'),
    cfg.IntOpt('socket_timeout',
               default=1000,
               help='Timeout in ms on blocking socket operations'),
]

_PUBLISHERS_KEY = "PUBLISHERS"
_ROUTERS_KEY = "ROUTERS"
_RETRY_METHODS = ("get_hosts", "get_publishers", "get_routers")


def retry_if_connection_error(ex):
        return isinstance(ex, redis.ConnectionError)


def retry_if_empty(hosts):
    return not hosts


def apply_retrying(obj, cfg):
    for attr_name, attr in inspect.getmembers(obj):
        if not (inspect.ismethod(attr) or inspect.isfunction(attr)):
            continue
        if attr_name in _RETRY_METHODS:
            setattr(
                obj,
                attr_name,
                retry(
                    wait_fixed=cfg.matchmaker_redis.wait_timeout,
                    stop_max_delay=cfg.matchmaker_redis.check_timeout,
                    retry_on_exception=retry_if_connection_error,
                    retry_on_result=retry_if_empty
                )(attr))


class RedisMatchMaker(base.MatchMakerBase):

    def __init__(self, conf, *args, **kwargs):
        super(RedisMatchMaker, self).__init__(conf, *args, **kwargs)
        self.conf.register_opts(matchmaker_redis_opts, "matchmaker_redis")

        self.sentinel_hosts = self._extract_sentinel_options()
        if not self.sentinel_hosts:
            self.standalone_redis = self._extract_standalone_redis_options()
            self._redis = redis.StrictRedis(
                host=self.standalone_redis["host"],
                port=self.standalone_redis["port"],
                password=self.standalone_redis["password"]
            )
        else:
            socket_timeout = self.conf.matchmaker_redis.socket_timeout / 1000.
            sentinel = redis.sentinel.Sentinel(
                sentinels=self.sentinel_hosts,
                socket_timeout=socket_timeout
            )

            self._redis = sentinel.master_for(
                self.conf.matchmaker_redis.sentinel_group_name,
                socket_timeout=socket_timeout
            )
        apply_retrying(self, self.conf)

    def _extract_sentinel_options(self):
        if self.url and self.url.hosts:
            if len(self.url.hosts) > 1:
                return [(host.hostname, host.port) for host in self.url.hosts]
        elif self.conf.matchmaker_redis.sentinel_hosts:
            s = self.conf.matchmaker_redis.sentinel_hosts
            return [tuple(i.split(":")) for i in s]

    def _extract_standalone_redis_options(self):
        if self.url and self.url.hosts:
            redis_host = self.url.hosts[0]
            return {"host": redis_host.hostname,
                    "port": redis_host.port,
                    "password": redis_host.password}
        else:
            return {"host": self.conf.matchmaker_redis.host,
                    "port": self.conf.matchmaker_redis.port,
                    "password": self.conf.matchmaker_redis.password}

    def register_publisher(self, hostname):
        host_str = ",".join(hostname)
        self._redis.sadd(_PUBLISHERS_KEY, host_str)

    def unregister_publisher(self, hostname):
        host_str = ",".join(hostname)
        self._redis.srem(_PUBLISHERS_KEY, host_str)

    def get_publishers(self):
        hosts = []
        hosts.extend([tuple(host_str.split(","))
                      for host_str in
                      self._get_hosts_by_key(_PUBLISHERS_KEY)])
        return hosts

    def register_router(self, hostname):
        self._redis.sadd(_ROUTERS_KEY, hostname)

    def unregister_router(self, hostname):
        self._redis.srem(_ROUTERS_KEY, hostname)

    def get_routers(self):
        return self._get_hosts_by_key(_ROUTERS_KEY)

    def _get_hosts_by_key(self, key):
        return self._redis.smembers(key)

    def register(self, target, hostname, listener_type, expire=-1):

        def register_key(key):
            self._redis.sadd(key, hostname)
            if expire > 0:
                self._redis.expire(key, expire)

        if target.topic and target.server:
            key = zmq_address.target_to_key(target, listener_type)
            register_key(key)

        if target.topic:
            key = zmq_address.prefix_str(target.topic, listener_type)
            register_key(key)

    def unregister(self, target, hostname, listener_type):
        key = zmq_address.target_to_key(target, listener_type)
        self._redis.srem(key, hostname)

    def get_hosts(self, target, listener_type):
        LOG.debug("[Redis] get_hosts for target %s", target)
        hosts = []
        key = zmq_address.target_to_key(target, listener_type)
        hosts.extend(self._get_hosts_by_key(key))

        if not hosts and target.topic and target.server:
            key = zmq_address.prefix_str(target.topic, listener_type)
            hosts.extend(self._get_hosts_by_key(key))

        return hosts
