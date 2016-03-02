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

import six

from oslo_messaging._drivers.zmq_driver import zmq_address


@six.add_metaclass(abc.ABCMeta)
class MatchMakerBase(object):

    def __init__(self, conf, *args, **kwargs):
        super(MatchMakerBase, self).__init__()
        self.conf = conf
        self.url = kwargs.get('url')

    @abc.abstractmethod
    def register_publisher(self, hostname):
        """Register publisher on nameserver.

        This works for PUB-SUB only

       :param hostname: host for the topic in "host:port" format
                        host for back-chatter in "host:port" format
       :type hostname: tuple
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
    def register_router(self, hostname):
        """Register router on the nameserver.

        This works for ROUTER proxy only

       :param hostname: host for the topic in "host:port" format
       :type hostname: string
       """

    @abc.abstractmethod
    def unregister_router(self, hostname):
        """Unregister router on the nameserver.

        This works for ROUTER proxy only

       :param hostname: host for the topic in "host:port" format
       :type hostname: string
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
       :type hostname: String
       :param listener_type: Listener socket type ROUTER, SUB etc.
       :type listener_type: String
       :param expire: Record expiration timeout
       :type expire: int
       """

    @abc.abstractmethod
    def unregister(self, target, hostname, listener_type):
        """Unregister target from nameserver.

       :param target: the target for host
       :type target: Target
       :param hostname: host for the topic in "host:port" format
       :type hostname: String
       :param listener_type: Listener socket type ROUTER, SUB etc.
       :type listener_type: String
       """

    @abc.abstractmethod
    def get_hosts(self, target, listener_type):
        """Get all hosts from nameserver by target.

       :param target: the default target for invocations
       :type target: Target
       :returns: a list of "hostname:port" hosts
       """


class DummyMatchMaker(MatchMakerBase):

    def __init__(self, conf, *args, **kwargs):
        super(DummyMatchMaker, self).__init__(conf, *args, **kwargs)

        self._cache = collections.defaultdict(list)
        self._publishers = set()
        self._routers = set()

    def register_publisher(self, hostname):
        if hostname not in self._publishers:
            self._publishers.add(hostname)

    def unregister_publisher(self, hostname):
        if hostname in self._publishers:
            self._publishers.remove(hostname)

    def get_publishers(self):
        return list(self._publishers)

    def register_router(self, hostname):
        if hostname not in self._routers:
            self._routers.add(hostname)

    def unregister_router(self, hostname):
        if hostname in self._routers:
            self._routers.remove(hostname)

    def get_routers(self):
        return list(self._routers)

    def register(self, target, hostname, listener_type, expire=-1):
        key = zmq_address.target_to_key(target, listener_type)
        if hostname not in self._cache[key]:
            self._cache[key].append(hostname)

    def unregister(self, target, hostname, listener_type):
        key = zmq_address.target_to_key(target, listener_type)
        if hostname in self._cache[key]:
            self._cache[key].remove(hostname)

    def get_hosts(self, target, listener_type):
        key = zmq_address.target_to_key(target, listener_type)
        return self._cache[key]
