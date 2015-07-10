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

from oslo_messaging._i18n import _LI, _LW


LOG = logging.getLogger(__name__)


@six.add_metaclass(abc.ABCMeta)
class MatchMakerBase(object):

    def __init__(self, conf, *args, **kwargs):
        super(MatchMakerBase, self).__init__(*args, **kwargs)

        self.conf = conf

    @abc.abstractmethod
    def register(self, topic, hostname):
        """Register topic on nameserver"""

    @abc.abstractmethod
    def get_hosts(self, topic):
        """Get hosts from nameserver by topic"""

    def get_single_host(self, topic):
        """Get a single host by topic"""
        hosts = self.get_hosts(topic)
        if len(hosts) == 0:
            LOG.warning(_LW("No hosts were found for topic %s. Using "
                            "localhost") % topic)
            return "localhost"
        elif len(hosts) == 1:
            LOG.info(_LI("A single host found for topic %s.") % topic)
            return hosts[0]
        else:
            LOG.warning(_LW("Multiple hosts were found for topic %s. Using "
                            "the first one.") % topic)
            return hosts[0]


class DummyMatchMaker(MatchMakerBase):

    def __init__(self, conf, *args, **kwargs):
        super(DummyMatchMaker, self).__init__(conf, *args, **kwargs)

        self._cache = collections.defaultdict(list)

    def register(self, topic, hostname):
        if hostname not in self._cache[topic]:
            self._cache[topic].append(hostname)

    def get_hosts(self, topic):
        return self._cache[topic]
