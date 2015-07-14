#    Copyright 2015 Mirantis, Inc.
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
import logging

import six

from oslo_messaging._drivers.common import RPCException
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging._drivers.zmq_driver import zmq_topic
from oslo_messaging._i18n import _LE, _LI

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


@six.add_metaclass(abc.ABCMeta)
class BaseProxy(object):

    """Base TCP-proxy.

    TCP-proxy redirects messages received by TCP from clients to servers
    over IPC. Consists of TCP-frontend and IPC-backend objects. Runs
    in async executor.
    """

    def __init__(self, conf, context):
        super(BaseProxy, self).__init__()
        self.conf = conf
        self.context = context
        self.executor = zmq_async.get_executor(
            self.run, native_zmq=conf.rpc_zmq_native)

    @abc.abstractmethod
    def run(self):
        """Main execution point of the proxy"""

    def start(self):
        self.executor.execute()

    def stop(self):
        self.executor.stop()

    def wait(self):
        self.executor.wait()


@six.add_metaclass(abc.ABCMeta)
class BaseTcpFrontend(object):

    """Base frontend clause.

    TCP-frontend is a part of TCP-proxy which receives incoming
    messages from clients.
    """

    def __init__(self, conf, poller, context,
                 socket_type=None,
                 port_number=None,
                 receive_meth=None):

        """Construct a TCP-frontend.

        Its attributes are:

        :param conf: Driver configuration object.
        :type conf: ConfigOpts
        :param poller: Messages poller-object green or threading.
        :type poller: ZmqPoller
        :param context: ZeroMQ context object.
        :type context: zmq.Context
        :param socket_type: ZeroMQ socket type.
        :type socket_type: int
        :param port_number: Current messaging pipeline port.
        :type port_number: int
        """

        self.conf = conf
        self.poller = poller
        self.context = context
        try:
            self.frontend = self.context.socket(socket_type)
            bind_address = zmq_topic.get_tcp_bind_address(port_number)
            LOG.info(_LI("Binding to TCP %s") % bind_address)
            self.frontend.bind(bind_address)
            self.poller.register(self.frontend, receive_meth)
        except zmq.ZMQError as e:
            errmsg = _LE("Could not create ZeroMQ receiver daemon. "
                         "Socket may already be in use: %s") % str(e)
            LOG.error(errmsg)
            raise RPCException(errmsg)

    def receive_incoming(self):
        message, socket = self.poller.poll(1)
        LOG.info(_LI("Message %s received."), message)
        return message


@six.add_metaclass(abc.ABCMeta)
class BaseBackendMatcher(object):

    def __init__(self, conf, poller, context):
        self.conf = conf
        self.context = context
        self.backends = {}
        self.poller = poller

    @abc.abstractmethod
    def redirect_to_backend(self, message):
        """Redirect message"""


@six.add_metaclass(abc.ABCMeta)
class DirectBackendMatcher(BaseBackendMatcher):

    def redirect_to_backend(self, message):
        backend, topic = self._match_backend(message)
        self._send_message(backend, message, topic)

    def _match_backend(self, message):
        topic = self._get_topic(message)
        ipc_address = self._get_ipc_address(topic)
        backend = self._create_backend(ipc_address)
        return backend, topic

    @abc.abstractmethod
    def _get_topic(self, message):
        """Extract topic from message"""

    @abc.abstractmethod
    def _get_ipc_address(self, topic):
        """Get ipc backend address from topic"""

    @abc.abstractmethod
    def _send_message(self, backend, message, topic):
        """Backend specific sending logic"""

    @abc.abstractmethod
    def _create_backend(self, ipc_address):
        """Backend specific socket opening logic"""
