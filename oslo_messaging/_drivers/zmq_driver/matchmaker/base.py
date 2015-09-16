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
import random

import six

import oslo_messaging
from oslo_messaging._i18n import _LI, _LW


LOG = logging.getLogger(__name__)


@six.add_metaclass(abc.ABCMeta)
class MatchMakerBase(object):

    def __init__(self, conf, *args, **kwargs):
        super(MatchMakerBase, self).__init__(*args, **kwargs)

        self.conf = conf

    @abc.abstractmethod
    def register(self, target, hostname):
        """Register target on nameserver.

       :param target: the target for host
       :type target: Target
       :param hostname: host for the topic in "host:port" format
       :type hostname: String
       """

    @abc.abstractmethod
    def get_hosts(self, target):
        """Get all hosts from nameserver by target.

       :param target: the default target for invocations
       :type target: Target
       :returns: a list of "hostname:port" hosts
       """

    def get_single_host(self, target):
        """Get a single host by target.

       :param target: the target for messages
       :type target: Target
       :returns: a "hostname:port" host
       """

        hosts = self.get_hosts(target)
        if not hosts:
            err_msg = "No hosts were found for target %s." % target
            LOG.error(err_msg)
            raise oslo_messaging.InvalidTarget(err_msg, target)

        if len(hosts) == 1:
            host = hosts[0]
            LOG.info(_LI("A single host %(host)s found for target %(target)s.")
                     % {"host": host, "target": target})
        else:
            host = random.choice(hosts)
            LOG.warning(_LW("Multiple hosts %(hosts)s were found for target "
                            " %(target)s. Using the random one - %(host)s.")
                        % {"hosts": hosts, "target": target, "host": host})
        return host


class DummyMatchMaker(MatchMakerBase):

    def __init__(self, conf, *args, **kwargs):
        super(DummyMatchMaker, self).__init__(conf, *args, **kwargs)

        self._cache = collections.defaultdict(list)

    def register(self, target, hostname):
        key = str(target)
        if hostname not in self._cache[key]:
            self._cache[key].append(hostname)

    def get_hosts(self, target):
        key = str(target)
        return self._cache[key]
