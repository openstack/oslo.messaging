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
from retrying import retry

from oslo_config import cfg
from oslo_utils import importutils

from oslo_messaging._drivers.zmq_driver.matchmaker import zmq_matchmaker_base
from oslo_messaging._drivers.zmq_driver import zmq_address
from oslo_messaging._i18n import _LW

redis = importutils.try_import('redis')
redis_sentinel = importutils.try_import('redis.sentinel')
LOG = logging.getLogger(__name__)


matchmaker_redis_opts = [
    cfg.StrOpt('host',
               default='127.0.0.1',
               deprecated_for_removal=True,
               deprecated_reason="Replaced by [DEFAULT]/transport_url",
               help='Host to locate redis.'),
    cfg.PortOpt('port',
                default=6379,
                deprecated_for_removal=True,
                deprecated_reason="Replaced by [DEFAULT]/transport_url",
                help='Use this port to connect to redis host.'),
    cfg.StrOpt('password',
               default='',
               secret=True,
               deprecated_for_removal=True,
               deprecated_reason="Replaced by [DEFAULT]/transport_url",
               help='Password for Redis server (optional).'),
    cfg.ListOpt('sentinel_hosts',
                default=[],
                deprecated_for_removal=True,
                deprecated_reason="Replaced by [DEFAULT]/transport_url",
                help='List of Redis Sentinel hosts (fault tolerance mode) e.g.\
                [host:port, host1:port ... ]'),
    cfg.StrOpt('sentinel_group_name',
               default='oslo-messaging-zeromq',
               help='Redis replica set name.'),
    cfg.IntOpt('wait_timeout',
               default=2000,
               help='Time in ms to wait between connection attempts.'),
    cfg.IntOpt('check_timeout',
               default=20000,
               help='Time in ms to wait before the transaction is killed.'),
    cfg.IntOpt('socket_timeout',
               default=10000,
               help='Timeout in ms on blocking socket operations'),
]

_PUBLISHERS_KEY = "PUBLISHERS"
_ROUTERS_KEY = "ROUTERS"


def redis_connection_warn(func):
    def func_wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except redis.ConnectionError:
            LOG.warning(_LW("Redis is currently not available. "
                            "Messages are being sent to known targets using "
                            "existing connections. But new nodes "
                            "can not be discovered until Redis is up "
                            "and running."))
            raise zmq_matchmaker_base.MatchmakerUnavailable()
    return func_wrapper


def no_reraise(func):
    def func_wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except zmq_matchmaker_base.MatchmakerUnavailable:
            pass
    return func_wrapper


def empty_list_on_error(func):
    def func_wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except zmq_matchmaker_base.MatchmakerUnavailable:
            return []
    return func_wrapper


def retry_if_connection_error(ex):
    return isinstance(ex, zmq_matchmaker_base.MatchmakerUnavailable)


def retry_if_empty(hosts):
    return not hosts


class MatchmakerRedis(zmq_matchmaker_base.MatchmakerBase):

    def __init__(self, conf, *args, **kwargs):
        super(MatchmakerRedis, self).__init__(conf, *args, **kwargs)
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

    def _add_key_with_expire(self, key, value, expire):
        self._redis.sadd(key, value)
        if expire > 0:
            self._redis.expire(key, expire)

    @no_reraise
    @redis_connection_warn
    def register_publisher(self, hostname, expire=-1):
        host_str = ",".join(hostname)
        self._add_key_with_expire(_PUBLISHERS_KEY, host_str, expire)

    @no_reraise
    @redis_connection_warn
    def unregister_publisher(self, hostname):
        host_str = ",".join(hostname)
        self._redis.srem(_PUBLISHERS_KEY, host_str)

    @empty_list_on_error
    @redis_connection_warn
    def get_publishers(self):
        hosts = []
        hosts.extend([tuple(host_str.split(","))
                      for host_str in
                      self._get_hosts_by_key(_PUBLISHERS_KEY)])
        return hosts

    @no_reraise
    @redis_connection_warn
    def register_router(self, hostname, expire=-1):
        self._add_key_with_expire(_ROUTERS_KEY, hostname, expire)

    @no_reraise
    @redis_connection_warn
    def unregister_router(self, hostname):
        self._redis.srem(_ROUTERS_KEY, hostname)

    @empty_list_on_error
    @redis_connection_warn
    def get_routers(self):
        return self._get_hosts_by_key(_ROUTERS_KEY)

    def _get_hosts_by_key(self, key):
        return self._redis.smembers(key)

    @redis_connection_warn
    def register(self, target, hostname, listener_type, expire=-1):
        if target.topic and target.server:
            key = zmq_address.target_to_key(target, listener_type)
            self._add_key_with_expire(key, hostname, expire)

        if target.topic:
            key = zmq_address.prefix_str(target.topic, listener_type)
            self._add_key_with_expire(key, hostname, expire)

    @no_reraise
    @redis_connection_warn
    def unregister(self, target, hostname, listener_type):
        if target.topic and target.server:
            key = zmq_address.target_to_key(target, listener_type)
            self._redis.srem(key, hostname)

        if target.topic:
            key = zmq_address.prefix_str(target.topic, listener_type)
            self._redis.srem(key, hostname)

    @redis_connection_warn
    def get_hosts(self, target, listener_type):
        hosts = []

        if target.topic and target.server:
            key = zmq_address.target_to_key(target, listener_type)
            hosts.extend(self._get_hosts_by_key(key))

        if not hosts and target.topic:
            key = zmq_address.prefix_str(target.topic, listener_type)
            hosts.extend(self._get_hosts_by_key(key))

        LOG.debug("[Redis] get_hosts for target %(target)s: %(hosts)s",
                  {"target": target, "hosts": hosts})

        return hosts

    def get_hosts_retry(self, target, listener_type):
        return self._retry_method(target, listener_type, self.get_hosts)

    @redis_connection_warn
    def get_hosts_fanout(self, target, listener_type):
        LOG.debug("[Redis] get_hosts for target %s", target)

        hosts = []

        if target.topic and target.server:
            key = zmq_address.target_to_key(target, listener_type)
            hosts.extend(self._get_hosts_by_key(key))

        key = zmq_address.prefix_str(target.topic, listener_type)
        hosts.extend(self._get_hosts_by_key(key))

        return hosts

    def get_hosts_fanout_retry(self, target, listener_type):
        return self._retry_method(target, listener_type, self.get_hosts_fanout)

    def _retry_method(self, target, listener_type, method):
        conf = self.conf

        @retry(retry_on_result=retry_if_empty,
               wrap_exception=True,
               wait_fixed=conf.matchmaker_redis.wait_timeout,
               stop_max_delay=conf.matchmaker_redis.check_timeout)
        def _get_hosts_retry(target, listener_type):
            return method(target, listener_type)
        return _get_hosts_retry(target, listener_type)
