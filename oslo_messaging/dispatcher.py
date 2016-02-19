
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

from oslo_messaging._i18n import _


__all__ = [
    "DispatcherBase",
    "DispatcherExecutorContext"
]

LOG = logging.getLogger(__name__)


class DispatcherExecutorContext(object):
    """Dispatcher executor context helper

    A dispatcher can have work to do before and after the dispatch of the
    request in the main server thread while the dispatcher itself can be
    done in its own thread.

    The executor can use the helper like this:

        callback = dispatcher(incoming)
        callback.prepare()
        thread = MyWhateverThread()
        thread.on_done(callback.done)
        thread.run(callback.run)

    """
    def __init__(self, incoming, dispatch, post=None):
        self._result = None
        self._incoming = incoming
        self._dispatch = dispatch
        self._post = post

    def run(self):
        """The incoming message dispath itself

        Can be run in an other thread/greenlet/corotine if the executor is
        able to do it.
        """
        try:
            self._result = self._dispatch(self._incoming)
        except Exception:
            msg = _('The dispatcher method must catches all exceptions')
            LOG.exception(msg)
            raise RuntimeError(msg)

    def done(self):
        """Callback after the incoming message have been dispathed

        Should be ran in the main executor thread/greenlet/corotine
        """
        # FIXME(sileht): this is not currently true, this works only because
        # the driver connection used for polling write on the wire only to
        # ack/requeue message, but what if one day, the driver do something
        # else
        if self._post is not None:
            self._post(self._incoming, self._result)


@six.add_metaclass(abc.ABCMeta)
class DispatcherBase(object):
    "Base class for dispatcher"

    batch_size = 1
    "Number of messages to wait before calling endpoints callacks"

    batch_timeout = None
    "Number of seconds to wait before calling endpoints callacks"

    @abc.abstractmethod
    def _listen(self, transport):
        """Initiate the driver Listener

        Usually the driver Listener is start with the transport helper methods:

        * transport._listen()
        * transport._listen_for_notifications()

        :param transport: the transport object
        :type transport: oslo_messaging.transport.Transport
        :returns: a driver Listener object
        :rtype: oslo_messaging._drivers.base.Listener
        """

    @abc.abstractmethod
    def __call__(self, incoming):
        """Called by the executor to get the DispatcherExecutorContext

        :param incoming: list of messages
        :type incoming: oslo_messging._drivers.base.IncomingMessage
        :returns: DispatcherExecutorContext
        :rtype: DispatcherExecutorContext
        """
