# Copyright 2010 United States Government as represented by the
# Administrator of the National Aeronautics and Space Administration.
# All Rights Reserved.
# Copyright 2013 Red Hat, Inc.
# Copyright 2013 New Dream Network, LLC (DreamHost)
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

from openstack.common.gettextutils import _
from openstack.common import log as logging

_LOG = logging.getLogger(__name__)


class MessagingServerError(Exception):
    pass


class MessageHandlingServer(object):
    """Server for handling messages.

    Connect a transport to a dispatcher that knows how process the
    message using an executor that knows how the app wants to create
    new tasks.
    """

    def __init__(self, transport, target, dispatcher, executor_cls):
        self.conf = transport.conf

        self.transport = transport
        self.target = target
        self.dispatcher = dispatcher

        self._executor_cls = executor_cls
        self._executor = None

        super(MessageHandlingServer, self).__init__()

    def start(self):
        """Start handling incoming messages.

        This method causes the server to begin polling the transport for
        incoming messages and passing them to the dispatcher. Message
        processing will continue until the stop() method is called.

        The executor controls how the server integrates with the applications
        I/O handling strategy - it may choose to poll for messages in a new
        process, thread or co-operatively scheduled coroutine or simply by
        registering a callback with an event loop. Similarly, the executor may
        choose to dispatch messages in a new thread, coroutine or simply the
        current thread. An RPCServer subclass is available for each I/O
        strategy supported by the library, so choose the subclass appropraite
        for your program.
        """
        if self._executor is not None:
            return
        listener = self.transport._listen(self.target)
        self._executor = self._executor_cls(self.conf, listener,
                                            self.dispatcher)
        self._executor.start()

    def stop(self):
        """Stop handling incoming messages.

        Once this method returns, no new incoming messages will be handled by
        the server. However, the server may still be in the process of handling
        some messages.
        """
        if self._executor is not None:
            self._executor.stop()

    def wait(self):
        """Wait for message processing to complete.

        After calling stop(), there may still be some some existing messages
        which have not been completely processed. The wait() method blocks
        until all message processing has completed.
        """
        if self._executor is not None:
            self._executor.wait()
        self._executor = None
