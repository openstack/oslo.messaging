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

import abc
import functools
import logging
import random
import time

from oslo_config import cfg
from oslo_utils import importutils
import six
import tenacity

from oslo_messaging._drivers.zmq_driver.matchmaker import zmq_matchmaker_base
from oslo_messaging._drivers.zmq_driver import zmq_address
from oslo_messaging._drivers.zmq_driver import zmq_updater
from oslo_messaging._i18n import _LE, _LI, _LW

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
                help='List of Redis Sentinel hosts (fault tolerance mode), '
                     'e.g., [host:port, host1:port ... ]'),
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
               help='Timeout in ms on blocking socket operations.'),
]

_PUBLISHERS_KEY = "PUBLISHERS"
_ROUTERS_KEY = "ROUTERS"


def write_to_redis_connection_warn(func):
    @functools.wraps(func)
    def func_wrapper(self, *args, **kwargs):
        # try to perform a write operation to all available hosts
        success = False
        for redis_instance in self._redis_instances:
            if not redis_instance._is_available:
                continue
            try:
                func(self, redis_instance, *args, **kwargs)
                success = True
            except redis.ConnectionError:
                LOG.warning(_LW("Redis host %s is not available now."),
                            redis_instance._address)
                redis_instance._is_available = False
                redis_instance._ready_from = float("inf")
        if not success:
            raise zmq_matchmaker_base.MatchmakerUnavailable()
    return func_wrapper


def read_from_redis_connection_warn(func):
    @functools.wraps(func)
    def func_wrapper(self, *args, **kwargs):
        # try to perform a read operation from any available and ready host
        for redis_instance in self._redis_instances:
            if not redis_instance._is_available \
                    or redis_instance._ready_from > time.time():
                continue
            try:
                return func(self, redis_instance, *args, **kwargs)
            except redis.ConnectionError:
                LOG.warning(_LW("Redis host %s is not available now."),
                            redis_instance._address)
                redis_instance._is_available = False
                redis_instance._ready_from = float("inf")
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


def is_empty(hosts):
    return not hosts


@six.add_metaclass(abc.ABCMeta)
class MatchmakerRedisBase(zmq_matchmaker_base.MatchmakerBase):

    def __init__(self, conf, *args, **kwargs):
        if redis is None:
            raise ImportError(_LE("Redis package is not available!"))

        super(MatchmakerRedisBase, self).__init__(conf, *args, **kwargs)

        self.conf.register_opts(matchmaker_redis_opts, "matchmaker_redis")

    @abc.abstractmethod
    def _sadd(self, key, value, expire):
        pass

    @abc.abstractmethod
    def _srem(self, key, value):
        pass

    @abc.abstractmethod
    def _smembers(self, key):
        pass

    @abc.abstractmethod
    def _ttl(self, key):
        pass

    @no_reraise
    def register_publisher(self, hostname, expire=-1):
        hostname = ','.join(hostname)
        self._sadd(_PUBLISHERS_KEY, hostname, expire)
        self._sadd(hostname, ' ', expire)

    @no_reraise
    def unregister_publisher(self, hostname):
        hostname = ','.join(hostname)
        self._srem(_PUBLISHERS_KEY, hostname)
        self._srem(hostname, ' ')

    @empty_list_on_error
    def get_publishers(self):
        return [tuple(hostname.split(',')) for hostname
                in self._smembers(_PUBLISHERS_KEY)]

    @no_reraise
    def register_router(self, hostname, expire=-1):
        self._sadd(_ROUTERS_KEY, hostname, expire)
        self._sadd(hostname, ' ', expire)

    @no_reraise
    def unregister_router(self, hostname):
        self._srem(_ROUTERS_KEY, hostname)
        self._srem(hostname, ' ')

    @empty_list_on_error
    def get_routers(self):
        return self._smembers(_ROUTERS_KEY)

    def get_hosts_by_key(self, key):
        return self._smembers(key)

    def register(self, target, hostname, listener_type, expire=-1):
        if target.server:
            key = zmq_address.target_to_key(target, listener_type)
            self._sadd(key, hostname, expire)
            self._sadd(hostname, ' ', expire)

        key = zmq_address.prefix_str(target.topic, listener_type)
        self._sadd(key, hostname, expire)
        self._sadd(hostname, ' ', expire)

    @no_reraise
    def unregister(self, target, hostname, listener_type):
        if target.server:
            key = zmq_address.target_to_key(target, listener_type)
            self._srem(key, hostname)
            self._srem(hostname, ' ')

        key = zmq_address.prefix_str(target.topic, listener_type)
        self._srem(key, hostname)
        self._srem(hostname, ' ')

    def get_hosts(self, target, listener_type):
        hosts = []

        if target.server:
            key = zmq_address.target_to_key(target, listener_type)
            hosts.extend(self._smembers(key))
        else:
            key = zmq_address.prefix_str(target.topic, listener_type)
            hosts.extend(self._smembers(key))

        LOG.debug("[Redis] get_hosts for target %(target)s: %(hosts)s",
                  {"target": target, "hosts": hosts})

        return hosts

    def get_hosts_retry(self, target, listener_type):
        return self._retry_method(target, listener_type, self.get_hosts)

    def get_hosts_fanout(self, target, listener_type):
        key = zmq_address.target_to_key(target, listener_type)
        hosts = list(self._smembers(key))

        LOG.debug("[Redis] get_hosts_fanout for target %(target)s: %(hosts)s",
                  {"target": target, "hosts": hosts})

        return hosts

    def get_hosts_fanout_retry(self, target, listener_type):
        return self._retry_method(target, listener_type, self.get_hosts_fanout)

    def _retry_method(self, target, listener_type, method):
        wait_timeout = self.conf.matchmaker_redis.wait_timeout / 1000.
        check_timeout = self.conf.matchmaker_redis.check_timeout / 1000.

        @tenacity.retry(retry=tenacity.retry_if_result(is_empty),
                        wait=tenacity.wait_fixed(wait_timeout),
                        stop=tenacity.stop_after_delay(check_timeout))
        def _get_hosts_retry(target, listener_type):
            return method(target, listener_type)

        return _get_hosts_retry(target, listener_type)


class MatchmakerRedis(MatchmakerRedisBase):

    def __init__(self, conf, *args, **kwargs):
        super(MatchmakerRedis, self).__init__(conf, *args, **kwargs)

        self._redis_hosts = self._extract_redis_hosts()

        self._redis_instances = [
            redis.StrictRedis(host=redis_host["host"],
                              port=redis_host["port"],
                              password=redis_host["password"])
            for redis_host in self._redis_hosts
        ]

        for redis_host, redis_instance \
                in six.moves.zip(self._redis_hosts, self._redis_instances):
            address = "{host}:{port}".format(host=redis_host["host"],
                                             port=redis_host["port"])
            redis_instance._address = address
            is_available = self._check_availability(redis_instance)
            if is_available:
                redis_instance._is_available = True
                redis_instance._ready_from = time.time()
            else:
                LOG.warning(_LW("Redis host %s is not available now."),
                            address)
                redis_instance._is_available = False
                redis_instance._ready_from = float("inf")

        # NOTE(gdavoian): store instances in a random order
        # (for the sake of load balancing)
        random.shuffle(self._redis_instances)

        self._availability_updater = \
            MatchmakerRedisAvailabilityUpdater(self.conf, self)

    def _extract_redis_hosts(self):
        if self.url and self.url.hosts:
            return [{"host": redis_host.hostname,
                     "port": redis_host.port,
                     "password": redis_host.password}
                    for redis_host in self.url.hosts]
        else:
            # FIXME(gdavoian): remove the code below along with the
            # corresponding deprecated options in the next release
            return [{"host": self.conf.matchmaker_redis.host,
                     "port": self.conf.matchmaker_redis.port,
                     "password": self.conf.matchmaker_redis.password}]

    @staticmethod
    def _check_availability(redis_instance):
        try:
            redis_instance.ping()
            return True
        except redis.ConnectionError:
            return False

    @write_to_redis_connection_warn
    def _sadd(self, redis_instance, key, value, expire):
        redis_instance.sadd(key, value)
        if expire > 0:
            redis_instance.expire(key, expire)

    @write_to_redis_connection_warn
    def _srem(self, redis_instance, key, value):
        redis_instance.srem(key, value)

    @read_from_redis_connection_warn
    def _ttl(self, redis_instance, key):
        # NOTE(ozamiatin): If the specialized key doesn't exist,
        # TTL fuction would return -2. If key exists,
        # but doesn't have expiration associated,
        # TTL func would return -1. For more information,
        # please visit http://redis.io/commands/ttl
        return redis_instance.ttl(key)

    @read_from_redis_connection_warn
    def _smembers(self, redis_instance, key):
        hosts = redis_instance.smembers(key)
        return [host for host in hosts if redis_instance.ttl(host) >= -1]


class MatchmakerRedisAvailabilityUpdater(zmq_updater.UpdaterBase):

    _MIN_SLEEP_FOR = 10

    def __init__(self, conf, matchmaker):
        super(MatchmakerRedisAvailabilityUpdater, self).__init__(
            conf, matchmaker, self._update_availability,
            sleep_for=conf.oslo_messaging_zmq.zmq_target_update
        )

    def _update_availability(self):
        fraction_of_available_instances = 0
        for redis_instance in self.matchmaker._redis_instances:
            if not redis_instance._is_available:
                is_available = \
                    self.matchmaker._check_availability(redis_instance)
                if is_available:
                    LOG.info(_LI("Redis host %s is available again."),
                             redis_instance._address)
                    fraction_of_available_instances += 1
                    # NOTE(gdavoian): mark an instance as available for
                    # writing to, but wait until all services register
                    # themselves in it for making the instance ready for
                    # reading from
                    redis_instance._is_available = True
                    redis_instance._ready_from = time.time() + \
                        self.conf.oslo_messaging_zmq.zmq_target_expire
            else:
                fraction_of_available_instances += 1
        fraction_of_available_instances /= \
            float(len(self.matchmaker._redis_instances))
        # NOTE(gdavoian): make the sleep time proportional to the number of
        # currently available instances
        self._sleep_for = max(self.conf.oslo_messaging_zmq.zmq_target_update *
                              fraction_of_available_instances,
                              self._MIN_SLEEP_FOR)


class MatchmakerSentinel(MatchmakerRedisBase):

    def __init__(self, conf, *args, **kwargs):
        super(MatchmakerSentinel, self).__init__(conf, *args, **kwargs)
        socket_timeout = self.conf.matchmaker_redis.socket_timeout / 1000.
        self._sentinel_hosts, self._password, self._master_group = \
            self._extract_sentinel_hosts()
        self._sentinel = redis_sentinel.Sentinel(
            sentinels=self._sentinel_hosts,
            socket_timeout=socket_timeout,
            password=self._password)
        self._slave = self._master = None

    @property
    def _redis_master(self):
        try:
            if not self._master:
                self._master = self._sentinel.master_for(self._master_group)
            return self._master
        except redis_sentinel.MasterNotFoundError:
            raise zmq_matchmaker_base.MatchmakerUnavailable()

    @property
    def _redis_slave(self):
        try:
            if not self._slave:
                self._slave = self._sentinel.slave_for(self._master_group)
        except redis_sentinel.SlaveNotFoundError:
            # use the master as slave (temporary)
            return self._redis_master
        return self._slave

    def _extract_sentinel_hosts(self):

        sentinels = []
        master_group = self.conf.matchmaker_redis.sentinel_group_name
        master_password = None

        if self.url and self.url.hosts:
            for host in self.url.hosts:
                target = host.hostname, host.port
                if host.password:
                    master_password = host.password
                sentinels.append(target)
            if self.url.virtual_host:
                # url://:pass@sentinel_a,:pass@sentinel_b/master_group_name
                master_group = self.url.virtual_host
        elif self.conf.matchmaker_redis.sentinel_hosts:
            s = self.conf.matchmaker_redis.sentinel_hosts
            sentinels.extend([tuple(target.split(":")) for target in s])
            master_password = self.conf.matchmaker_redis.password

        return sentinels, master_password, master_group

    def _sadd(self, key, value, expire):
        self._redis_master.sadd(key, value)
        if expire > 0:
            self._redis_master.expire(key, expire)

    def _srem(self, key, value):
        self._redis_master.srem(key, value)

    def _smembers(self, key):
        hosts = self._redis_slave.smembers(key)
        return [host for host in hosts if self._ttl(host) >= -1]

    def _ttl(self, key):
        return self._redis_slave.ttl(key)
