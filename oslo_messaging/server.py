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

__all__ = [
    'ExecutorLoadFailure',
    'MessageHandlingServer',
    'MessagingServerError',
    'ServerListenError',
]

from stevedore import driver

from oslo_messaging._drivers import base as driver_base
from oslo_messaging import exceptions


class MessagingServerError(exceptions.MessagingException):
    """Base class for all MessageHandlingServer exceptions."""


class ExecutorLoadFailure(MessagingServerError):
    """Raised if an executor can't be loaded."""

    def __init__(self, executor, ex):
        msg = 'Failed to load executor "%s": %s' % (executor, ex)
        super(ExecutorLoadFailure, self).__init__(msg)
        self.executor = executor
        self.ex = ex


class ServerListenError(MessagingServerError):
    """Raised if we failed to listen on a target."""

    def __init__(self, target, ex):
        msg = 'Failed to listen on target "%s": %s' % (target, ex)
        super(ServerListenError, self).__init__(msg)
        self.target = target
        self.ex = ex


class MessageHandlingServer(object):
    """Server for handling messages.

    Connect a transport to a dispatcher that knows how to process the
    message using an executor that knows how the app wants to create
    new tasks.
    """

    def __init__(self, transport, dispatcher, executor='blocking'):
        """Construct a message handling server.

        The dispatcher parameter is a callable which is invoked with context
        and message dictionaries each time a message is received.

        The executor parameter controls how incoming messages will be received
        and dispatched. By default, the most simple executor is used - the
        blocking executor.

        :param transport: the messaging transport
        :type transport: Transport
        :param dispatcher: a callable which is invoked for each method
        :type dispatcher: callable
        :param executor: name of message executor - for example
                         'eventlet', 'blocking'
        :type executor: str
        """
        self.conf = transport.conf

        self.transport = transport
        self.dispatcher = dispatcher
        self.executor = executor

        try:
            mgr = driver.DriverManager('oslo.messaging.executors',
                                       self.executor)
        except RuntimeError as ex:
            raise ExecutorLoadFailure(self.executor, ex)
        else:
            self._executor_cls = mgr.driver
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
        current thread.
        """
        if self._executor is not None:
            return
        try:
            listener = self.dispatcher._listen(self.transport)
        except driver_base.TransportDriverError as ex:
            raise ServerListenError(self.target, ex)

        self._executor = self._executor_cls(self.conf, listener,
                                            self.dispatcher)
        self._executor.start()

    def stop(self):
        """Stop handling incoming messages.

        Once this method returns, no new incoming messages will be handled by
        the server. However, the server may still be in the process of handling
        some messages, and underlying driver resources associated to this
        server are still in use. See 'wait' for more details.
        """
        if self._executor is not None:
            self._executor.stop()

    def wait(self):
        """Wait for message processing to complete.

        After calling stop(), there may still be some some existing messages
        which have not been completely processed. The wait() method blocks
        until all message processing has completed.

        Once it's finished, the underlying driver resources associated to this
        server are released (like closing useless network connections).
        """
        if self._executor is not None:
            self._executor.wait()
            # Close listener connection after processing all messages
            self._executor.listener.cleanup()

        self._executor = None
