# Copyright 2010 United States Government as represented by the
# Administrator of the National Aeronautics and Space Administration.
# All Rights Reserved.
# Copyright 2013 Red Hat, Inc.
# Copyright (c) 2012 Rackspace Hosting
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

from debtcollector import removals
from oslo_config import cfg
from oslo_utils import netutils
from stevedore import driver
from urllib import parse

from oslo_messaging import exceptions

__all__ = [
    'DriverLoadFailure',
    'InvalidTransportURL',
    'Transport',
    'TransportHost',
    'TransportURL',
    'TransportOptions',
    'get_transport',
    'set_transport_defaults',
]

LOG = logging.getLogger(__name__)

_transport_opts = [
    cfg.StrOpt('transport_url',
               default="rabbit://",
               secret=True,
               help='The network address and optional user credentials for '
                    'connecting to the messaging backend, in URL format. The '
                    'expected format is:\n\n'
                    'driver://[user:pass@]host:port[,[userN:passN@]hostN:'
                    'portN]/virtual_host?query\n\n'
                    'Example: rabbit://rabbitmq:password@127.0.0.1:5672//\n\n'
                    'For full details on the fields in the URL see the '
                    'documentation of oslo_messaging.TransportURL at '
                    'https://docs.openstack.org/oslo.messaging/latest/'
                    'reference/transport.html'),

    cfg.StrOpt('control_exchange',
               default='openstack',
               help='The default exchange under which topics are scoped. May '
                    'be overridden by an exchange name specified in the '
                    'transport_url option.'),
]


def set_transport_defaults(control_exchange):
    """Set defaults for messaging transport configuration options.

    :param control_exchange: the default exchange under which topics are scoped
    :type control_exchange: str
    """
    cfg.set_defaults(_transport_opts,
                     control_exchange=control_exchange)


class Transport:

    """A messaging transport.

    This is a mostly opaque handle for an underlying messaging transport
    driver.

    RPCs and Notifications may use separate messaging systems that utilize
    different drivers, access permissions, message delivery, etc. To ensure
    the correct messaging functionality, the corresponding method should be
    used to construct a Transport object from transport configuration
    gleaned from the user's configuration and, optionally, a transport URL.

    The factory method for RPC Transport objects::

        def get_rpc_transport(conf, url=None,
                              allowed_remote_exmods=None)

    If a transport URL is supplied as a parameter, any transport configuration
    contained in it takes precedence. If no transport URL is supplied, but
    there is a transport URL supplied in the user's configuration then that
    URL will take the place of the URL parameter.

    The factory method for Notification Transport objects::

        def get_notification_transport(conf, url=None,
                                       allowed_remote_exmods=None)

    If no transport URL is provided, the URL in the notifications section of
    the config file will be used.  If that URL is also absent, the same
    transport as specified in the user's default section will be used.

    The Transport has a single 'conf' property which is the cfg.ConfigOpts
    instance used to construct the transport object.
    """

    def __init__(self, driver):
        self.conf = driver.conf
        self._driver = driver

    def _require_driver_features(self, requeue=False):
        self._driver.require_features(requeue=requeue)

    def _send(self, target, ctxt, message, wait_for_reply=None, timeout=None,
              call_monitor_timeout=None, retry=None, transport_options=None):
        if not target.topic:
            raise exceptions.InvalidTarget('A topic is required to send',
                                           target)
        return self._driver.send(target, ctxt, message,
                                 wait_for_reply=wait_for_reply,
                                 timeout=timeout,
                                 call_monitor_timeout=call_monitor_timeout,
                                 retry=retry,
                                 transport_options=transport_options)

    def _send_notification(self, target, ctxt, message, version, retry=None):
        if not target.topic:
            raise exceptions.InvalidTarget('A topic is required to send',
                                           target)
        self._driver.send_notification(target, ctxt, message, version,
                                       retry=retry)

    def _listen(self, target, batch_size, batch_timeout):
        if not (target.topic and target.server):
            raise exceptions.InvalidTarget('A server\'s target must have '
                                           'topic and server names specified',
                                           target)
        return self._driver.listen(target, batch_size,
                                   batch_timeout)

    def _listen_for_notifications(self, targets_and_priorities, pool,
                                  batch_size, batch_timeout):
        for target, priority in targets_and_priorities:
            if not target.topic:
                raise exceptions.InvalidTarget('A target must have '
                                               'topic specified',
                                               target)
        return self._driver.listen_for_notifications(
            targets_and_priorities, pool, batch_size, batch_timeout
        )

    def cleanup(self):
        """Release all resources associated with this transport."""
        self._driver.cleanup()


class RPCTransport(Transport):
    """Transport object for RPC."""

    def __init__(self, driver):
        super().__init__(driver)


class NotificationTransport(Transport):
    """Transport object for notifications."""

    def __init__(self, driver):
        super().__init__(driver)


class InvalidTransportURL(exceptions.MessagingException):
    """Raised if transport URL is invalid."""

    def __init__(self, url, msg):
        super().__init__(msg)
        self.url = url


class DriverLoadFailure(exceptions.MessagingException):
    """Raised if a transport driver can't be loaded."""

    def __init__(self, driver, ex):
        msg = 'Failed to load transport driver "{}": {}'.format(driver, ex)
        super().__init__(msg)
        self.driver = driver
        self.ex = ex


def _get_transport(conf, url=None, allowed_remote_exmods=None,
                   transport_cls=RPCTransport):
    allowed_remote_exmods = allowed_remote_exmods or []
    conf.register_opts(_transport_opts)

    if not isinstance(url, TransportURL):
        url = TransportURL.parse(conf, url)

    kwargs = dict(default_exchange=conf.control_exchange,
                  allowed_remote_exmods=allowed_remote_exmods)

    try:
        mgr = driver.DriverManager('oslo.messaging.drivers',
                                   url.transport.split('+')[0],
                                   invoke_on_load=True,
                                   invoke_args=[conf, url],
                                   invoke_kwds=kwargs)
    except RuntimeError as ex:
        raise DriverLoadFailure(url.transport, ex)

    return transport_cls(mgr.driver)


@removals.remove(
    message='use get_rpc_transport or get_notification_transport'
)
def get_transport(conf, url=None, allowed_remote_exmods=None):
    """A factory method for Transport objects.

    This method will construct a Transport object from transport configuration
    gleaned from the user's configuration and, optionally, a transport URL.

    If a transport URL is supplied as a parameter, any transport configuration
    contained in it takes precedence. If no transport URL is supplied, but
    there is a transport URL supplied in the user's configuration then that
    URL will take the place of the URL parameter. In both cases, any
    configuration not supplied in the transport URL may be taken from
    individual configuration parameters in the user's configuration.

    An example transport URL might be::

        rabbit://me:passwd@host:5672/virtual_host

    and can either be passed as a string or a TransportURL object.

    :param conf: the user configuration
    :type conf: cfg.ConfigOpts
    :param url: a transport URL, see :py:class:`transport.TransportURL`
    :type url: str or TransportURL
    :param allowed_remote_exmods: a list of modules which a client using this
                                  transport will deserialize remote exceptions
                                  from
    :type allowed_remote_exmods: list
    """
    return _get_transport(conf, url, allowed_remote_exmods,
                          transport_cls=RPCTransport)


class TransportHost:

    """A host element of a parsed transport URL."""

    def __init__(self, hostname=None, port=None, username=None, password=None):
        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password

    def __hash__(self):
        return hash((self.hostname, self.port, self.username, self.password))

    def __eq__(self, other):
        return vars(self) == vars(other)

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        attrs = []
        for a in ['hostname', 'port', 'username', 'password']:
            v = getattr(self, a)
            if v:
                attrs.append((a, repr(v)))
        values = ', '.join(['%s=%s' % i for i in attrs])
        return '<TransportHost ' + values + '>'


class TransportOptions:

    def __init__(self, at_least_once=False):
        self._at_least_once = at_least_once

    @property
    def at_least_once(self):
        return self._at_least_once


class TransportURL:

    """A parsed transport URL.

    Transport URLs take the form::

      driver://[user:pass@]host:port[,[userN:passN@]hostN:portN]/virtual_host?query

    where:

    driver
      Specifies the transport driver to use. Typically this is `rabbit` for the
      RabbitMQ broker. See the documentation for other available transport
      drivers.

    [user:pass@]host:port
      Specifies the network location of the broker. `user` and `pass` are the
      optional username and password used for authentication with the broker.

      `user` and `pass` may contain any of the following ASCII characters:
        * Alphabetic (a-z and A-Z)
        * Numeric (0-9)
        * Special characters: & = $ - _ . + ! * ( )

      `user` may include at most one `@` character for compatibility with some
      implementations of SASL.

      All other characters in `user` and `pass` must be encoded via '%nn'

      You may include multiple different network locations separated by commas.
      The client will connect to any of the available locations and will
      automatically fail over to another should the connection fail.

    virtual_host
      Specifies the "virtual host" within the broker. Support for virtual hosts
      is specific to the message bus used.

    query
      Permits passing driver-specific options which override the corresponding
      values from the configuration file.

    :param conf: a ConfigOpts instance
    :type conf: oslo.config.cfg.ConfigOpts
    :param transport: a transport name for example 'rabbit'
    :type transport: str
    :param virtual_host: a virtual host path for example '/'
    :type virtual_host: str
    :param hosts: a list of TransportHost objects
    :type hosts: list
    :param query: a dictionary of URL query parameters
    :type query: dict
    """

    def __init__(self, conf, transport=None, virtual_host=None, hosts=None,
                 query=None):
        self.conf = conf
        self.conf.register_opts(_transport_opts)
        self.transport = transport
        self.virtual_host = virtual_host
        if hosts is None:
            self.hosts = []
        else:
            self.hosts = hosts
        if query is None:
            self.query = {}
        else:
            self.query = query

    def __hash__(self):
        return hash((tuple(self.hosts), self.transport, self.virtual_host))

    def __eq__(self, other):
        return (self.transport == other.transport and
                self.virtual_host == other.virtual_host and
                self.hosts == other.hosts)

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        attrs = []
        for a in ['transport', 'virtual_host', 'hosts']:
            v = getattr(self, a)
            if v:
                attrs.append((a, repr(v)))
        values = ', '.join(['%s=%s' % i for i in attrs])
        return '<TransportURL ' + values + '>'

    def __str__(self):
        netlocs = []

        for host in self.hosts:
            username = host.username
            password = host.password
            hostname = host.hostname
            port = host.port

            # Starting place for the network location
            netloc = ''

            # Build the username and password portion of the transport URL
            if username is not None or password is not None:
                if username is not None:
                    netloc += parse.quote(username, '')
                if password is not None:
                    netloc += ':%s' % parse.quote(password, '')
                netloc += '@'

            # Build the network location portion of the transport URL
            if hostname:
                netloc += netutils.escape_ipv6(hostname)
            if port is not None:
                netloc += ':%d' % port

            netlocs.append(netloc)

        # Assemble the transport URL
        url = '{}://{}/'.format(self.transport, ','.join(netlocs))

        if self.virtual_host:
            url += parse.quote(self.virtual_host)

        if self.query:
            url += '?' + parse.urlencode(self.query, doseq=True)

        return url

    @classmethod
    def parse(cls, conf, url=None):
        """Parse a URL as defined by :py:class:`TransportURL` and return a
        TransportURL object.

        Assuming a URL takes the form of::

          transport://user:pass@host:port[,userN:passN@hostN:portN]/virtual_host?query

        then parse the URL and return a TransportURL object.

        Netloc is parsed following the sequence bellow:

        * It is first split by ',' in order to support multiple hosts
        * All hosts should be specified with username/password or not
          at the same time. In case of lack of specification, username and
          password will be omitted::

            user:pass@host1:port1,host2:port2

            [
              {"username": "user", "password": "pass", "host": "host1:port1"},
              {"host": "host2:port2"}
            ]

        If the url is not provided conf.transport_url is parsed instead.

        :param conf: a ConfigOpts instance
        :type conf: oslo.config.cfg.ConfigOpts
        :param url: The URL to parse
        :type url: str
        :returns: A TransportURL
        """

        if not url:
            conf.register_opts(_transport_opts)
        url = url or conf.transport_url

        if not isinstance(url, str):
            raise InvalidTransportURL(url, 'Wrong URL type')

        url = parse.urlparse(url)

        if not url.scheme:
            raise InvalidTransportURL(url.geturl(), 'No scheme specified')

        transport = url.scheme

        query = {}
        if url.query:
            for key, values in parse.parse_qs(url.query).items():
                query[key] = ','.join(values)

        virtual_host = None
        if url.path.startswith('/'):
            virtual_host = parse.unquote(url.path[1:])

        hosts_with_credentials = []
        hosts_without_credentials = []
        hosts = []

        for host in url.netloc.split(','):
            if not host:
                continue

            hostname = host
            username = password = port = None

            if '@' in host:
                username, hostname = host.rsplit('@', 1)
                if ':' in username:
                    username, password = username.split(':', 1)
                    password = parse.unquote(password)
                username = parse.unquote(username)

            hostname, port = netutils.parse_host_port(hostname)

            if username is None or password is None:
                hosts_without_credentials.append(hostname)
            else:
                hosts_with_credentials.append(hostname)

            hosts.append(TransportHost(hostname=hostname,
                                       port=port,
                                       username=username,
                                       password=password))

        if (len(hosts_with_credentials) > 0 and
                len(hosts_without_credentials) > 0):
            LOG.warning("All hosts must be set with username/password or "
                        "not at the same time. Hosts with credentials "
                        "are: %(hosts_with_credentials)s. Hosts without "
                        "credentials are %(hosts_without_credentials)s.",
                        {'hosts_with_credentials': hosts_with_credentials,
                         'hosts_without_credentials':
                         hosts_without_credentials})
        return cls(conf, transport, virtual_host, hosts, query)
