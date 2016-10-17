#    Copyright 2016, Red Hat, Inc.
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

"""
Utilities that map from a  Target address to a proper AMQP 1.0 address.

This module defines a utility class that translates a high level oslo.messaging
 address (Target) into the message-level address used on the message bus.  This
 translation may be statically configured or determined when the connection to
 the message bus is made.

The Target members that are used to generate the address are:

 * exchange
 * topic
 * server flag
 * fanout flag

In addition a 'service' tag is associated with the address. This tag determines
the service associated with an address (e.g. rpc or notification) so
that traffic can be partitioned based on its use.
"""

import abc
import logging

from oslo_messaging._i18n import _LW
from oslo_messaging.target import Target

__all__ = [
    "keyify",
    "AddresserFactory",
    "SERVICE_RPC",
    "SERVICE_NOTIFY"
]

SERVICE_RPC = 0
SERVICE_NOTIFY = 1

LOG = logging.getLogger(__name__)


def keyify(address, service=SERVICE_RPC):
    """Create a hashable key from a Target and service that will uniquely
    identify the generated address. This key is used to map the abstract
    oslo.messaging address to its corresponding AMQP link(s). This mapping may
    be done before the connection is established.
    """
    if isinstance(address, Target):
        # service is important because the resolved address may be
        # different based on whether or not this Target is used for
        # notifications or RPC
        return ("Target:{t={%s} e={%s} s={%s} f={%s} service={%s}}" %
                (address.topic, address.exchange, address.server,
                 address.fanout, service))
    else:
        # absolute address can be used without modification
        return "String:{%s}" % address


class Addresser(object):
    """Base class message bus address generator. Used to convert an
    oslo.messaging address into an AMQP 1.0 address string used over the
    connection to the message bus.
    """
    def __init__(self, default_exchange):
        self._default_exchange = default_exchange

    def resolve(self, target, service):
        if not isinstance(target, Target):
            # an already resolved address
            return target
        # Return a link address for a given target
        if target.fanout:
            return self.multicast_address(target, service)
        elif target.server:
            return self.unicast_address(target, service)
        else:
            return self.anycast_address(target, service)

    @abc.abstractmethod
    def multicast_address(self, target, service):
        """Address used to broadcast to all subscribers
        """

    @abc.abstractmethod
    def unicast_address(self, target, service):
        """Address used to target a specific subscriber (direct)
        """

    @abc.abstractmethod
    def anycast_address(self, target, service):
        """Address used for shared subscribers (competing consumers)
        """


class LegacyAddresser(Addresser):
    """Legacy addresses are in the following format:

    multicast: '$broadcast_prefix.$exchange.$topic.all'
    unicast: '$server_prefix.$exchange.$topic.$server'
    anycast: '$group_prefix.$exchange.$topic'

    Legacy addresses do not distinguish RPC traffic from Notification traffic
    """
    def __init__(self, default_exchange, server_prefix, broadcast_prefix,
                 group_prefix):
        super(LegacyAddresser, self).__init__(default_exchange)
        self._server_prefix = server_prefix
        self._broadcast_prefix = broadcast_prefix
        self._group_prefix = group_prefix

    def multicast_address(self, target, service):
        return self._concatenate([self._broadcast_prefix,
                                  target.exchange or self._default_exchange,
                                  target.topic, "all"])

    def unicast_address(self, target, service=SERVICE_RPC):
        return self._concatenate([self._server_prefix,
                                  target.exchange or self._default_exchange,
                                  target.topic, target.server])

    def anycast_address(self, target, service=SERVICE_RPC):
        return self._concatenate([self._group_prefix,
                                  target.exchange or self._default_exchange,
                                  target.topic])

    def _concatenate(self, items):
        return ".".join(filter(bool, items))

    # for debug:
    def _is_multicast(self, address):
        return address.startswith(self._broadcast_prefix)

    def _is_unicast(self, address):
        return address.startswith(self._server_prefix)

    def _is_anycast(self, address):
        return address.startswith(self._group_prefix)

    def _is_service(self, address, service):
        # legacy addresses are the same for RPC or Notifications
        return True


class RoutableAddresser(Addresser):
    """Routable addresses have different formats based their use.  It starts
    with a prefix that is determined by the type of traffic (RPC or
    Notifications).  The prefix is followed by a description of messaging
    delivery semantics. The delivery may be one of: 'multicast', 'unicast', or
    'anycast'. The delivery semantics are followed by information pulled from
    the Target.  The template is:

    $prefix/$semantics/$exchange/$topic[/$server]

    Examples based on the default prefix and semantic values:

    rpc-unicast: "openstack.org/om/rpc/unicast/$exchange/$topic/$server"
    notify-anycast: "openstack.org/om/notify/anycast/$exchange/$topic"
    """

    def __init__(self, default_exchange, rpc_exchange, rpc_prefix,
                 notify_exchange, notify_prefix, unicast_tag, multicast_tag,
                 anycast_tag):
        super(RoutableAddresser, self).__init__(default_exchange)
        if not self._default_exchange:
            self._default_exchange = "openstack"
        # templates for address generation:
        _rpc = rpc_prefix + "/"
        self._rpc_prefix = _rpc
        self._rpc_unicast = _rpc + unicast_tag
        self._rpc_multicast = _rpc + multicast_tag
        self._rpc_anycast = _rpc + anycast_tag

        _notify = notify_prefix + "/"
        self._notify_prefix = _notify
        self._notify_unicast = _notify + unicast_tag
        self._notify_multicast = _notify + multicast_tag
        self._notify_anycast = _notify + anycast_tag

        self._exchange = [
            # SERVICE_RPC:
            rpc_exchange or self._default_exchange or 'rpc',
            # SERVICE_NOTIFY:
            notify_exchange or self._default_exchange or 'notify'
        ]

    def multicast_address(self, target, service=SERVICE_RPC):
        if service == SERVICE_RPC:
            prefix = self._rpc_multicast
        else:
            prefix = self._notify_multicast
        return "%s/%s/%s" % (prefix,
                             target.exchange or self._exchange[service],
                             target.topic)

    def unicast_address(self, target, service=SERVICE_RPC):
        if service == SERVICE_RPC:
            prefix = self._rpc_unicast
        else:
            prefix = self._notify_unicast
        if target.server:
            return "%s/%s/%s/%s" % (prefix,
                                    target.exchange or self._exchange[service],
                                    target.topic,
                                    target.server)
        return "%s/%s/%s" % (prefix,
                             target.exchange or self._exchange[service],
                             target.topic)

    def anycast_address(self, target, service=SERVICE_RPC):
        if service == SERVICE_RPC:
            prefix = self._rpc_anycast
        else:
            prefix = self._notify_anycast
        return "%s/%s/%s" % (prefix,
                             target.exchange or self._exchange[service],
                             target.topic)

    # for debug:
    def _is_multicast(self, address):
        return (address.startswith(self._rpc_multicast) or
                address.startswith(self._notify_multicast))

    def _is_unicast(self, address):
        return (address.startswith(self._rpc_unicast) or
                address.startswith(self._notify_unicast))

    def _is_anycast(self, address):
        return (address.startswith(self._rpc_anycast) or
                address.startswith(self._notify_anycast))

    def _is_service(self, address, service):
        return address.startswith(self._rpc_prefix if service == SERVICE_RPC
                                  else self._notify_prefix)


class AddresserFactory(object):
    """Generates the proper Addresser based on configuration and the type of
    message bus the driver is connected to.
    """
    def __init__(self, default_exchange, mode, **kwargs):
        self._default_exchange = default_exchange
        self._mode = mode
        self._kwargs = kwargs

    def __call__(self, remote_properties):
        # for backwards compatibility use legacy if dynamic and we're connected
        # to qpidd or we cannot identify the message bus.  This can be
        # overridden via the configuration.
        product = remote_properties.get('product', 'qpid-cpp')

        # TODO(kgiusti): Router support was added in Newton.  Remove this
        # warning post Newton, once the driver has stabilized.
        if product == "qpid-dispatch-router":
            w = _LW("This is the initial release of support for message"
                    " routing technology. Be aware that messages are not"
                    " queued and may be discarded if there are no consumers"
                    " present.")
            LOG.warning(w)

        if self._mode == 'legacy' or (self._mode == 'dynamic'
                                      and product == 'qpid-cpp'):
            return LegacyAddresser(self._default_exchange,
                                   self._kwargs['legacy_server_prefix'],
                                   self._kwargs['legacy_broadcast_prefix'],
                                   self._kwargs['legacy_group_prefix'])
        else:
            return RoutableAddresser(self._default_exchange,
                                     self._kwargs.get("rpc_exchange"),
                                     self._kwargs["rpc_prefix"],
                                     self._kwargs.get("notify_exchange"),
                                     self._kwargs["notify_prefix"],
                                     self._kwargs["unicast"],
                                     self._kwargs["multicast"],
                                     self._kwargs["anycast"])
