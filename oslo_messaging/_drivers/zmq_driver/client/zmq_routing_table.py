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

import itertools
import logging
import threading
import time

from oslo_messaging._drivers.zmq_driver.matchmaker import zmq_matchmaker_base
from oslo_messaging._drivers.zmq_driver import zmq_address
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_names
from oslo_messaging._drivers.zmq_driver import zmq_updater
from oslo_messaging._i18n import _LW

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


class RoutingTableAdaptor(object):

    def __init__(self, conf, matchmaker, listener_type):
        self.conf = conf
        self.matchmaker = matchmaker
        self.listener_type = listener_type
        self.routing_table = RoutingTable(conf)
        self.routing_table_updater = RoutingTableUpdater(
            conf, matchmaker, self.routing_table)
        self.round_robin_targets = {}
        self._lock = threading.Lock()

    def get_round_robin_host(self, target):
        target_key = self._fetch_round_robin_hosts_from_matchmaker(target)
        rr_gen = self.round_robin_targets[target_key]
        host = next(rr_gen)
        LOG.debug("Host resolved for the current connection is %s" % host)
        return host

    def get_all_round_robin_hosts(self, target):
        target_key = self._fetch_round_robin_hosts_from_matchmaker(target)
        return self.routing_table.get_hosts_fanout(target_key)

    def _fetch_round_robin_hosts_from_matchmaker(self, target):
        target_key = zmq_address.target_to_key(
            target, zmq_names.socket_type_str(self.listener_type))

        LOG.debug("Processing target %s for round-robin." % target_key)

        if target_key not in self.round_robin_targets:
            with self._lock:
                if target_key not in self.round_robin_targets:
                    LOG.debug("Target %s is not in cache. Check matchmaker "
                              "server." % target_key)
                    hosts = self.matchmaker.get_hosts_retry(
                        target, zmq_names.socket_type_str(self.listener_type))
                    LOG.debug("Received hosts %s" % hosts)
                    self.routing_table.update_hosts(target_key, hosts)
                    self.round_robin_targets[target_key] = \
                        self.routing_table.get_hosts_round_robin(target_key)
        return target_key

    def get_fanout_hosts(self, target):
        target_key = zmq_address.target_to_key(
            target, zmq_names.socket_type_str(self.listener_type))

        LOG.debug("Processing target %s for fanout." % target_key)

        if not self.routing_table.contains(target_key):
            self._fetch_fanout_hosts_from_matchmaker(target, target_key)

        return self.routing_table.get_hosts_fanout(target_key)

    def _fetch_fanout_hosts_from_matchmaker(self, target, target_key):
        with self._lock:
            if not self.routing_table.contains(target_key):
                LOG.debug("Target %s is not in cache. Check matchmaker server."
                          % target_key)
                hosts = self.matchmaker.get_hosts_fanout(
                    target, zmq_names.socket_type_str(self.listener_type))
                LOG.debug("Received hosts %s" % hosts)
                self.routing_table.update_hosts(target_key, hosts)

    def cleanup(self):
        self.routing_table_updater.cleanup()


class RoutingTable(object):

    def __init__(self, conf):
        self.conf = conf
        self.targets = {}
        self._lock = threading.Lock()

    def register(self, target_key, host):
        with self._lock:
            if target_key in self.targets:
                hosts, tm = self.targets[target_key]
                if host not in hosts:
                    hosts.add(host)
                    self.targets[target_key] = (hosts, self._create_tm())
            else:
                self.targets[target_key] = ({host}, self._create_tm())

    def get_targets(self):
        with self._lock:
            return list(self.targets.keys())

    def unregister(self, target_key, host):
        with self._lock:
            hosts, tm = self.targets.get(target_key)
            if hosts and host in hosts:
                hosts.discard(host)
                self.targets[target_key] = (hosts, self._create_tm())

    def update_hosts(self, target_key, hosts_updated):
        with self._lock:
            if target_key in self.targets and not hosts_updated:
                self.targets.pop(target_key)
                return
            hosts_current, _ = self.targets.get(target_key, (set(), None))
            hosts_updated = set(hosts_updated)
            has_differences = hosts_updated ^ hosts_current
            if has_differences:
                self.targets[target_key] = (hosts_updated, self._create_tm())

    def get_hosts_round_robin(self, target_key):
        while self.contains(target_key):
            for host in self._get_hosts_rr(target_key):
                yield host

    def get_hosts_fanout(self, target_key):
        hosts, _ = self._get_hosts(target_key)
        return hosts

    def contains(self, target_key):
        with self._lock:
            return target_key in self.targets

    def _get_hosts(self, target_key):
        with self._lock:
            hosts, tm = self.targets.get(target_key, ([], None))
            hosts = list(hosts)
            return hosts, tm

    def _get_tm(self, target_key):
        with self._lock:
            _, tm = self.targets.get(target_key)
            return tm

    def _is_target_changed(self, target_key, tm_orig):
        return self._get_tm(target_key) != tm_orig

    @staticmethod
    def _create_tm():
        return time.time()

    def _get_hosts_rr(self, target_key):
        hosts, tm_original = self._get_hosts(target_key)
        for host in itertools.cycle(hosts):
            if self._is_target_changed(target_key, tm_original):
                raise StopIteration()
            yield host


class RoutingTableUpdater(zmq_updater.UpdaterBase):

    def __init__(self, conf, matchmaker, routing_table):
        self.routing_table = routing_table
        super(RoutingTableUpdater, self).__init__(
            conf, matchmaker, self._update_routing_table,
            conf.oslo_messaging_zmq.zmq_target_update)

    def _update_routing_table(self):
        target_keys = self.routing_table.get_targets()

        try:
            for target_key in target_keys:
                hosts = self.matchmaker.get_hosts_by_key(target_key)
                self.routing_table.update_hosts(target_key, hosts)
            LOG.debug("Updating routing table from the matchmaker. "
                      "%d target(s) updated %s." % (len(target_keys),
                                                    target_keys))
        except zmq_matchmaker_base.MatchmakerUnavailable:
            LOG.warning(_LW("Not updated. Matchmaker was not available."))
