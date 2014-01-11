
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

__all__ = [
    'DriverLoadFailure',
    'InvalidTransportURL',
    'Transport',
    'TransportHost',
    'TransportURL',
    'get_transport',
    'set_transport_defaults',
]

from oslo.config import cfg
import six
from stevedore import driver

from oslo.messaging import exceptions
from oslo.messaging.openstack.common.py3kcompat import urlutils


_transport_opts = [
    cfg.StrOpt('transport_url',
               default=None,
               help='A URL representing the messaging driver to use and its '
                    'full configuration. If not set, we fall back to the '
                    'rpc_backend option and driver specific configuration.'),
    cfg.StrOpt('rpc_backend',
               default='rabbit',
               help='The messaging driver to use, defaults to rabbit. Other '
                    'drivers include qpid and zmq.'),
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


class Transport(object):

    """A messaging transport.

    This is a mostly opaque handle for an underlying messaging transport
    driver.

    It has a single 'conf' property which is the cfg.ConfigOpts instance used
    to construct the transport object.
    """

    def __init__(self, driver):
        self.conf = driver.conf
        self._driver = driver

    def _send(self, target, ctxt, message, wait_for_reply=None, timeout=None):
        if not target.topic:
            raise exceptions.InvalidTarget('A topic is required to send',
                                           target)
        return self._driver.send(target, ctxt, message,
                                 wait_for_reply=wait_for_reply,
                                 timeout=timeout)

    def _send_notification(self, target, ctxt, message, version):
        if not target.topic:
            raise exceptions.InvalidTarget('A topic is required to send',
                                           target)
        self._driver.send_notification(target, ctxt, message, version)

    def _listen(self, target):
        if not (target.topic and target.server):
            raise exceptions.InvalidTarget('A server\'s target must have '
                                           'topic and server names specified',
                                           target)
        return self._driver.listen(target)

    def cleanup(self):
        """Release all resources associated with this transport."""
        self._driver.cleanup()


class InvalidTransportURL(exceptions.MessagingException):
    """Raised if transport URL is invalid."""

    def __init__(self, url, msg):
        super(InvalidTransportURL, self).__init__(msg)
        self.url = url


class DriverLoadFailure(exceptions.MessagingException):
    """Raised if a transport driver can't be loaded."""

    def __init__(self, driver, ex):
        msg = 'Failed to load transport driver "%s": %s' % (driver, ex)
        super(DriverLoadFailure, self).__init__(msg)
        self.driver = driver
        self.ex = ex


def get_transport(conf, url=None, allowed_remote_exmods=[], aliases=None):
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
    :param url: a transport URL
    :type url: str or TransportURL
    :param allowed_remote_exmods: a list of modules which a client using this
                                  transport will deserialize remote exceptions
                                  from
    :type allowed_remote_exmods: list
    :param aliases: A map of transport alias to transport name
    :type aliases: dict
    """
    conf.register_opts(_transport_opts)

    if not isinstance(url, TransportURL):
        url = url or conf.transport_url
        parsed = TransportURL.parse(conf, url, aliases)
        if not parsed.transport:
            raise InvalidTransportURL(url, 'No scheme specified in "%s"' % url)
        url = parsed

    kwargs = dict(default_exchange=conf.control_exchange,
                  allowed_remote_exmods=allowed_remote_exmods)

    try:
        mgr = driver.DriverManager('oslo.messaging.drivers',
                                   url.transport,
                                   invoke_on_load=True,
                                   invoke_args=[conf, url],
                                   invoke_kwds=kwargs)
    except RuntimeError as ex:
        raise DriverLoadFailure(url.transport, ex)

    return Transport(mgr.driver)


class TransportHost(object):

    """A host element of a parsed transport URL."""

    def __init__(self, hostname=None, port=None, username=None, password=None):
        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password

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


class TransportURL(object):

    """A parsed transport URL.

    Transport URLs take the form::

      transport://user:pass@host1:port[,hostN:portN]/virtual_host

    i.e. the scheme selects the transport driver, you may include multiple
    hosts in netloc and the path part is a "virtual host" partition path.

    :param conf: a ConfigOpts instance
    :type conf: oslo.config.cfg.ConfigOpts
    :param transport: a transport name e.g. 'rabbit' or 'qpid'
    :type transport: str
    :param virtual_host: a virtual host path e.g. '/'
    :type virtual_host: str
    :param hosts: a list of TransportHost objects
    :type hosts: list
    :param aliases: A map of transport alias to transport name
    :type aliases: dict
    """

    def __init__(self, conf, transport=None, virtual_host=None, hosts=None,
                 aliases=None):
        self.conf = conf
        self.conf.register_opts(_transport_opts)
        self._transport = transport
        self.virtual_host = virtual_host
        if hosts is None:
            self.hosts = []
        else:
            self.hosts = hosts
        if aliases is None:
            self.aliases = {}
        else:
            self.aliases = aliases

    @property
    def transport(self):
        if self._transport is None:
            transport = self.conf.rpc_backend
        else:
            transport = self._transport
        return self.aliases.get(transport, transport)

    @transport.setter
    def transport(self, value):
        self._transport = value

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
                    netloc += urlutils.quote(username, '')
                if password is not None:
                    netloc += ':%s' % urlutils.quote(password, '')
                netloc += '@'

            # Build the network location portion of the transport URL
            if hostname:
                if ':' in hostname:
                    netloc += '[%s]' % hostname
                else:
                    netloc += hostname
            if port is not None:
                netloc += ':%d' % port

            netlocs.append(netloc)

        # Assemble the transport URL
        url = '%s://%s/' % (self.transport, ','.join(netlocs))

        if self.virtual_host:
            url += urlutils.quote(self.virtual_host)

        return url

    @classmethod
    def parse(cls, conf, url, aliases=None):
        """Parse an url.

        Assuming a URL takes the form of:

          transport://user:pass@host1:port[,hostN:portN]/virtual_host

        then parse the URL and return a TransportURL object.

        Netloc is parsed following the sequence bellow:

        * It is first split by ',' in order to support multiple hosts
        * The last parsed username and password will be propagated to the rest
          of hosts specified:

          user:passwd@host1:port1,host2:port2

          [
            {"username": "user", "password": "passwd", "host": "host1:port1"},
            {"username": "user", "password": "passwd", "host": "host2:port2"}
          ]

        * In order to avoid the above propagation, it is possible to alter the
          order in which the hosts are specified or specify a set of fake
          credentials using ",:@host2:port2"

          user:passwd@host1:port1,:@host2:port2

          [
            {"username": "user", "password": "passwd", "host": "host1:port1"},
            {"username": "", "password": "", "host": "host2:port2"}
          ]

        :param conf: a ConfigOpts instance
        :type conf: oslo.config.cfg.ConfigOpts
        :param url: The URL to parse
        :type url: str
        :param aliases: A map of transport alias to transport name
        :type aliases: dict
        :returns: A TransportURL
        """
        if not url:
            return cls(conf, aliases=aliases)

        if not isinstance(url, six.string_types):
            raise InvalidTransportURL(url, 'Wrong URL type')

        url = urlutils.urlparse(url)

        # Make sure there's not a query string; that could identify
        # requirements we can't comply with (e.g., ssl), so reject it if
        # it's present
        if '?' in url.path or url.query:
            raise InvalidTransportURL(url.geturl(),
                                      "Cannot comply with query string in "
                                      "transport URL")

        virtual_host = None
        if url.path.startswith('/'):
            virtual_host = url.path[1:]

        hosts = []

        username = password = ''
        for host in url.netloc.split(','):
            if not host:
                continue

            hostname = host
            username = password = port = None

            if '@' in host:
                username, hostname = host.split('@', 1)
                if ':' in username:
                    username, password = username.split(':', 1)

            if not hostname:
                hostname = None
            elif hostname.startswith('['):
                # Find the closing ']' and extract the hostname
                host_end = hostname.find(']')
                if host_end < 0:
                    # NOTE(Vek): Identical to what Python 2.7's
                    # urlparse.urlparse() raises in this case
                    raise ValueError("Invalid IPv6 URL")

                port_text = hostname[host_end:]
                hostname = hostname[1:host_end]

                # Now we need the port; this is compliant with how urlparse
                # parses the port data
                port = None
                if ':' in port_text:
                    port = int(port_text.split(':', 1)[1])
            elif ':' in hostname:
                hostname, port = hostname.split(':', 1)
                port = int(port)

            hosts.append(TransportHost(hostname=hostname,
                                       port=port,
                                       username=username,
                                       password=password))

        return cls(conf, url.scheme, virtual_host, hosts, aliases)
