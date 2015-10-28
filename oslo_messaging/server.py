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

import functools
import inspect
import logging
import threading
import traceback

from oslo_service import service
from oslo_utils import timeutils
from stevedore import driver

from oslo_messaging._drivers import base as driver_base
from oslo_messaging import exceptions

LOG = logging.getLogger(__name__)


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


class _OrderedTask(object):
    """A task which must be executed in a particular order.

    A caller may wait for this task to complete by calling
    `wait_for_completion`.

    A caller may run this task with `run_once`, which will ensure that however
    many times the task is called it only runs once. Simultaneous callers will
    block until the running task completes, which means that any caller can be
    sure that the task has completed after run_once returns.
    """

    INIT = 0      # The task has not yet started
    RUNNING = 1   # The task is running somewhere
    COMPLETE = 2  # The task has run somewhere

    # We generate a log message if we wait for a lock longer than
    # LOG_AFTER_WAIT_SECS seconds
    LOG_AFTER_WAIT_SECS = 30

    def __init__(self, name):
        """Create a new _OrderedTask.

        :param name: The name of this task. Used in log messages.
        """

        super(_OrderedTask, self).__init__()

        self._name = name
        self._cond = threading.Condition()
        self._state = self.INIT

    def _wait(self, condition, warn_msg):
        """Wait while condition() is true. Write a log message if condition()
        has not become false within LOG_AFTER_WAIT_SECS.
        """
        with timeutils.StopWatch(duration=self.LOG_AFTER_WAIT_SECS) as w:
            logged = False
            while condition():
                wait = None if logged else w.leftover()
                self._cond.wait(wait)

                if not logged and w.expired():
                    LOG.warn(warn_msg)
                    LOG.debug(''.join(traceback.format_stack()))
                    # Only log once. After than we wait indefinitely without
                    # logging.
                    logged = True

    def wait_for_completion(self, caller):
        """Wait until this task has completed.

        :param caller: The name of the task which is waiting.
        """
        with self._cond:
            self._wait(lambda: self._state != self.COMPLETE,
                       '%s has been waiting for %s to complete for longer '
                       'than %i seconds'
                       % (caller, self._name, self.LOG_AFTER_WAIT_SECS))

    def run_once(self, fn):
        """Run a task exactly once. If it is currently running in another
        thread, wait for it to complete. If it has already run, return
        immediately without running it again.

        :param fn: The task to run. It must be a callable taking no arguments.
                   It may optionally return another callable, which also takes
                   no arguments, which will be executed after completion has
                   been signaled to other threads.
        """
        with self._cond:
            if self._state == self.INIT:
                self._state = self.RUNNING
                # Note that nothing waits on RUNNING, so no need to notify

                # We need to release the condition lock before calling out to
                # prevent deadlocks. Reacquire it immediately afterwards.
                self._cond.release()
                try:
                    post_fn = fn()
                finally:
                    self._cond.acquire()
                    self._state = self.COMPLETE
                    self._cond.notify_all()

                if post_fn is not None:
                    # Release the condition lock before calling out to prevent
                    # deadlocks. Reacquire it immediately afterwards.
                    self._cond.release()
                    try:
                        post_fn()
                    finally:
                        self._cond.acquire()
            elif self._state == self.RUNNING:
                self._wait(lambda: self._state == self.RUNNING,
                           '%s has been waiting on another thread to complete '
                           'for longer than %i seconds'
                           % (self._name, self.LOG_AFTER_WAIT_SECS))


class _OrderedTaskRunner(object):
    """Mixin for a class which executes ordered tasks."""

    def __init__(self, *args, **kwargs):
        super(_OrderedTaskRunner, self).__init__(*args, **kwargs)

        # Get a list of methods on this object which have the _ordered
        # attribute
        self._tasks = [name
                       for (name, member) in inspect.getmembers(self)
                       if inspect.ismethod(member) and
                       getattr(member, '_ordered', False)]
        self.init_task_states()

    def init_task_states(self):
        # Note that we don't need to lock this. Once created, the _states dict
        # is immutable. Get and set are (individually) atomic operations in
        # Python, and we only set after the dict is fully created.
        self._states = {task: _OrderedTask(task) for task in self._tasks}

    @staticmethod
    def decorate_ordered(fn, state, after):

        @functools.wraps(fn)
        def wrapper(self, *args, **kwargs):
            # Store the states we started with in case the state wraps on us
            # while we're sleeping. We must wait and run_once in the same
            # epoch. If the epoch ended while we were sleeping, run_once will
            # safely do nothing.
            states = self._states

            # Wait for the given preceding state to complete
            if after is not None:
                states[after].wait_for_completion(state)

            # Run this state
            states[state].run_once(lambda: fn(self, *args, **kwargs))
        return wrapper


def ordered(after=None):
    """A method which will be executed as an ordered task. The method will be
    called exactly once, however many times it is called. If it is called
    multiple times simultaneously it will only be called once, but all callers
    will wait until execution is complete.

    If `after` is given, this method will not run until `after` has completed.

    :param after: Optionally, another method decorated with `ordered`. Wait for
                  the completion of `after` before executing this method.
    """
    if after is not None:
        after = after.__name__

    def _ordered(fn):
        # Set an attribute on the method so we can find it later
        setattr(fn, '_ordered', True)
        state = fn.__name__

        return _OrderedTaskRunner.decorate_ordered(fn, state, after)
    return _ordered


class MessageHandlingServer(service.ServiceBase, _OrderedTaskRunner):
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

        self._executor_cls = mgr.driver
        self._executor_obj = None

        super(MessageHandlingServer, self).__init__()

    @ordered()
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
        try:
            listener = self.dispatcher._listen(self.transport)
        except driver_base.TransportDriverError as ex:
            raise ServerListenError(self.target, ex)
        executor = self._executor_cls(self.conf, listener, self.dispatcher)
        executor.start()
        self._executor_obj = executor

        if self.executor == 'blocking':
            # N.B. This will be executed unlocked and unordered, so
            # we can't rely on the value of self._executor_obj when this runs.
            # We explicitly pass the local variable.
            return lambda: executor.execute()

    @ordered(after=start)
    def stop(self):
        """Stop handling incoming messages.

        Once this method returns, no new incoming messages will be handled by
        the server. However, the server may still be in the process of handling
        some messages, and underlying driver resources associated to this
        server are still in use. See 'wait' for more details.
        """
        self._executor_obj.stop()

    @ordered(after=stop)
    def wait(self):
        """Wait for message processing to complete.

        After calling stop(), there may still be some existing messages
        which have not been completely processed. The wait() method blocks
        until all message processing has completed.

        Once it's finished, the underlying driver resources associated to this
        server are released (like closing useless network connections).
        """
        try:
            self._executor_obj.wait()
        finally:
            # Close listener connection after processing all messages
            self._executor_obj.listener.cleanup()
            self._executor_obj = None

            self.init_task_states()

    def reset(self):
        """Reset service.

        Called in case service running in daemon mode receives SIGHUP.
        """
        # TODO(sergey.vilgelm): implement this method
        pass
