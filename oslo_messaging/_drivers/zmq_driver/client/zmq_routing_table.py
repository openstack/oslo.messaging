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

import logging
import retrying
import time

from oslo_messaging._drivers.zmq_driver.matchmaker import zmq_matchmaker_base
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_names
from oslo_messaging._i18n import _LW

zmq = zmq_async.import_zmq()

LOG = logging.getLogger(__name__)


class RoutingTable(object):
    """This class implements local routing-table cache
        taken from matchmaker. Its purpose is to give the next routable
        host id (remote DEALER's id) by request for specific target in
        round-robin fashion.
    """

    def __init__(self, conf, matchmaker):
        self.conf = conf
        self.matchmaker = matchmaker
        self.routing_table = {}
        self.routable_hosts = {}

    def get_all_hosts(self, target):
        self._update_routing_table(
            target,
            get_hosts=self.matchmaker.get_hosts_fanout,
            get_hosts_retry=self.matchmaker.get_hosts_fanout_retry)
        return self.routable_hosts.get(str(target), [])

    def get_routable_host(self, target):
        self._update_routing_table(
            target,
            get_hosts=self.matchmaker.get_hosts,
            get_hosts_retry=self.matchmaker.get_hosts_retry)
        hosts_for_target = self.routable_hosts.get(str(target))
        if not hosts_for_target:
            # Matchmaker doesn't contain any target
            return None
        host = hosts_for_target.pop(0)
        if not hosts_for_target:
            self._renew_routable_hosts(target)
        return host

    def _is_tm_expired(self, tm):
        return 0 <= self.conf.oslo_messaging_zmq.zmq_target_expire \
            <= time.time() - tm

    def _update_routing_table(self, target, get_hosts, get_hosts_retry):
        routing_record = self.routing_table.get(str(target))
        if routing_record is None:
            self._fetch_hosts(target, get_hosts, get_hosts_retry)
            self._renew_routable_hosts(target)
        elif self._is_tm_expired(routing_record[1]):
            self._fetch_hosts(target, get_hosts, get_hosts_retry)

    def _fetch_hosts(self, target, get_hosts, get_hosts_retry):
        key = str(target)
        if key not in self.routing_table:
            try:
                self.routing_table[key] = (get_hosts_retry(
                    target, zmq_names.socket_type_str(zmq.DEALER)),
                    time.time())
            except retrying.RetryError:
                LOG.warning(_LW("Matchmaker contains no hosts for target %s")
                            % key)
        else:
            try:
                hosts = get_hosts(
                    target, zmq_names.socket_type_str(zmq.DEALER))
                self.routing_table[key] = (hosts, time.time())
            except zmq_matchmaker_base.MatchmakerUnavailable:
                LOG.warning(_LW("Matchmaker contains no hosts for target %s")
                            % key)

    def _renew_routable_hosts(self, target):
        key = str(target)
        try:
            hosts, _ = self.routing_table[key]
            self.routable_hosts[key] = list(hosts)
        except KeyError:
            self.routable_hosts[key] = []
