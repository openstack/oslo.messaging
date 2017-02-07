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
import collections
import logging

import six
import time

from oslo_messaging._drivers import common as rpc_common
from oslo_messaging._drivers.zmq_driver import zmq_address
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._i18n import _LE

LOG = logging.getLogger(__name__)


class MatchmakerUnavailable(rpc_common.RPCException):
    """Exception is raised on connection error to matchmaker service"""

    def __init__(self):
        super(MatchmakerUnavailable, self).__init__(
            message=_LE("Matchmaker is not currently available."))


@six.add_metaclass(abc.ABCMeta)
class MatchmakerBase(object):

    def __init__(self, conf, *args, **kwargs):
        super(MatchmakerBase, self).__init__()
        self.conf = conf
        self.url = kwargs.get('url')

    @abc.abstractmethod
    def register_publisher(self, hostname, expire=-1):
        """Register publisher on nameserver.

        This works for PUB-SUB only

       :param hostname: host for the topic in "host:port" format
                        host for back-chatter in "host:port" format
       :type hostname: tuple
       :param expire: record expiration timeout
       :type expire: int
       """

    @abc.abstractmethod
    def unregister_publisher(self, hostname):
        """Unregister publisher on nameserver.

        This works for PUB-SUB only

       :param hostname: host for the topic in "host:port" format
                        host for back-chatter in "host:port" format
       :type hostname: tuple
       """

    @abc.abstractmethod
    def get_publishers(self):
        """Get all publisher-hosts from nameserver.

       :returns: a list of tuples of strings "hostname:port" hosts
       """

    @abc.abstractmethod
    def register_router(self, hostname, expire=-1):
        """Register router on the nameserver.

        This works for ROUTER proxy only

       :param hostname: host for the topic in "host:port" format
       :type hostname: str
       :param expire: record expiration timeout
       :type expire: int
       """

    @abc.abstractmethod
    def unregister_router(self, hostname):
        """Unregister router on the nameserver.

        This works for ROUTER proxy only

       :param hostname: host for the topic in "host:port" format
       :type hostname: str
       """

    @abc.abstractmethod
    def get_routers(self):
        """Get all router-hosts from nameserver.

       :returns: a list of strings "hostname:port" hosts
       """

    @abc.abstractmethod
    def register(self, target, hostname, listener_type, expire=-1):
        """Register target on nameserver.
        If record already exists and has expiration timeout it will be
        updated. Existing records without timeout will stay untouched

       :param target: the target for host
       :type target: Target
       :param hostname: host for the topic in "host:port" format
       :type hostname: str
       :param listener_type: listener socket type ROUTER, SUB etc.
       :type listener_type: str
       :param expire: record expiration timeout
       :type expire: int
       """

    @abc.abstractmethod
    def unregister(self, target, hostname, listener_type):
        """Unregister target from nameserver.

       :param target: the target for host
       :type target: Target
       :param hostname: host for the topic in "host:port" format
       :type hostname: str
       :param listener_type: listener socket type ROUTER, SUB etc.
       :type listener_type: str
       """

    @abc.abstractmethod
    def get_hosts(self, target, listener_type):
        """Get all hosts from nameserver by target.

       :param target: the default target for invocations
       :type target: Target
       :param listener_type: listener socket type ROUTER, SUB etc.
       :type listener_type: str
       :returns: a list of "hostname:port" hosts
       """

    @abc.abstractmethod
    def get_hosts_retry(self, target, listener_type):
        """Retry if not hosts - used on client first time connection.

       :param target: the default target for invocations
       :type target: Target
       :param listener_type: listener socket type ROUTER, SUB etc.
       :type listener_type: str
       :returns: a list of "hostname:port" hosts
       """

    @abc.abstractmethod
    def get_hosts_fanout(self, target, listener_type):
        """Get all hosts for fanout from nameserver by target.

       :param target: the default target for invocations
       :type target: Target
       :param listener_type: listener socket type ROUTER, SUB etc.
       :type listener_type: str
       :returns: a list of "hostname:port" hosts
       """

    @abc.abstractmethod
    def get_hosts_fanout_retry(self, target, listener_type):
        """Retry if not host for fanout - used on client first time connection.

       :param target: the default target for invocations
       :type target: Target
       :param listener_type: listener socket type ROUTER, SUB etc.
       :type listener_type: str
       :returns: a list of "hostname:port" hosts
       """


class MatchmakerDummy(MatchmakerBase):

    def __init__(self, conf, *args, **kwargs):
        super(MatchmakerDummy, self).__init__(conf, *args, **kwargs)

        self._cache = collections.defaultdict(list)
        self._publishers = set()
        self._routers = set()
        self._address = {}
        self.executor = zmq_async.get_executor(method=self._loop)
        self.executor.execute()

    def register_publisher(self, hostname, expire=-1):
        if hostname not in self._publishers:
            self._publishers.add(hostname)
        self._address[hostname] = expire

    def unregister_publisher(self, hostname):
        if hostname in self._publishers:
            self._publishers.remove(hostname)
        if hostname in self._address:
            self._address.pop(hostname)

    def get_publishers(self):
        hosts = [host for host in self._publishers
                 if self._address[host] > 0]
        return hosts

    def register_router(self, hostname, expire=-1):
        if hostname not in self._routers:
            self._routers.add(hostname)
        self._address[hostname] = expire

    def unregister_router(self, hostname):
        if hostname in self._routers:
            self._routers.remove(hostname)
        if hostname in self._address:
            self._address.pop(hostname)

    def get_routers(self):
        hosts = [host for host in self._routers
                 if self._address[host] > 0]
        return hosts

    def _loop(self):
        for hostname in self._address:
            expire = self._address[hostname]
            if expire > 0:
                self._address[hostname] = expire - 1
        time.sleep(1)

    def register(self, target, hostname, listener_type, expire=-1):
        if target.server:
            key = zmq_address.target_to_key(target, listener_type)
            if hostname not in self._cache[key]:
                self._cache[key].append(hostname)

        key = zmq_address.prefix_str(target.topic, listener_type)
        if hostname not in self._cache[key]:
            self._cache[key].append(hostname)

        self._address[hostname] = expire

    def unregister(self, target, hostname, listener_type):
        if target.server:
            key = zmq_address.target_to_key(target, listener_type)
            if hostname in self._cache[key]:
                self._cache[key].remove(hostname)

        key = zmq_address.prefix_str(target.topic, listener_type)
        if hostname in self._cache[key]:
            self._cache[key].remove(hostname)

        if hostname in self._address:
            self._address.pop(hostname)

    def get_hosts(self, target, listener_type):
        hosts = []

        if target.server:
            key = zmq_address.target_to_key(target, listener_type)
            hosts.extend([host for host in self._cache[key]
                         if self._address[host] > 0])

        if not hosts:
            key = zmq_address.prefix_str(target.topic, listener_type)
            hosts.extend([host for host in self._cache[key]
                         if self._address[host] > 0])

        LOG.debug("[Dummy] get_hosts for target %(target)s: %(hosts)s",
                  {"target": target, "hosts": hosts})

        return hosts

    def get_hosts_retry(self, target, listener_type):
        # Do not complicate dummy matchmaker
        # This method will act smarter in real world matchmakers
        return self.get_hosts(target, listener_type)

    def get_hosts_fanout(self, target, listener_type):
        hosts = []
        key = zmq_address.target_to_key(target, listener_type)
        hosts.extend([host for host in self._cache[key]
                     if self._address[host] > 0])

        LOG.debug("[Dummy] get_hosts_fanout for target %(target)s: %(hosts)s",
                  {"target": target, "hosts": hosts})

        return hosts

    def get_hosts_fanout_retry(self, target, listener_type):
        # Do not complicate dummy matchmaker
        # This method will act smarter in real world matchmakers
        return self.get_hosts_fanout(target, listener_type)
