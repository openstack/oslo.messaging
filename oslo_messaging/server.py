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

import abc
import functools
import inspect
import logging
import threading
import traceback

import debtcollector
from oslo_config import cfg
from oslo_service import service
from oslo_utils import eventletutils
from oslo_utils import timeutils
from stevedore import driver

from oslo_messaging._drivers import base as driver_base
from oslo_messaging import _utils as utils
from oslo_messaging import exceptions

__all__ = [
    'ExecutorLoadFailure',
    'MessageHandlingServer',
    'MessagingServerError',
    'ServerListenError',
]

LOG = logging.getLogger(__name__)

# The default number of seconds of waiting after which we will emit a log
# message
DEFAULT_LOG_AFTER = 30


_pool_opts = [
    cfg.IntOpt('executor_thread_pool_size',
               default=64,
               deprecated_name="rpc_thread_pool_size",
               help='Size of executor thread pool when'
               ' executor is threading or eventlet.'),
]


class MessagingServerError(exceptions.MessagingException):
    """Base class for all MessageHandlingServer exceptions."""


class ExecutorLoadFailure(MessagingServerError):
    """Raised if an executor can't be loaded."""

    def __init__(self, executor, ex):
        msg = 'Failed to load executor "{}": {}'.format(executor, ex)
        super().__init__(msg)
        self.executor = executor
        self.ex = ex


class ServerListenError(MessagingServerError):
    """Raised if we failed to listen on a target."""

    def __init__(self, target, ex):
        msg = 'Failed to listen on target "{}": {}'.format(target, ex)
        super().__init__(msg)
        self.target = target
        self.ex = ex


class TaskTimeout(MessagingServerError):
    """Raised if we timed out waiting for a task to complete."""


class _OrderedTask:
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

    def __init__(self, name):
        """Create a new _OrderedTask.

        :param name: The name of this task. Used in log messages.
        """
        super().__init__()

        self._name = name
        self._cond = threading.Condition()
        self._state = self.INIT

    def _wait(self, condition, msg, log_after, timeout_timer):
        """Wait while condition() is true. Write a log message if condition()
        has not become false within `log_after` seconds. Raise TaskTimeout if
        timeout_timer expires while waiting.
        """

        log_timer = None
        if log_after != 0:
            log_timer = timeutils.StopWatch(duration=log_after)
            log_timer.start()

        while condition():
            if log_timer is not None and log_timer.expired():
                LOG.warning('Possible hang: %s', msg)
                LOG.debug(''.join(traceback.format_stack()))
                # Only log once. After than we wait indefinitely without
                # logging.
                log_timer = None

            if timeout_timer is not None and timeout_timer.expired():
                raise TaskTimeout(msg)

            timeouts = []
            if log_timer is not None:
                timeouts.append(log_timer.leftover())
            if timeout_timer is not None:
                timeouts.append(timeout_timer.leftover())

            wait = None
            if timeouts:
                wait = min(timeouts)
            self._cond.wait(wait)

    @property
    def complete(self):
        return self._state == self.COMPLETE

    def wait_for_completion(self, caller, log_after, timeout_timer):
        """Wait until this task has completed.

        :param caller: The name of the task which is waiting.
        :param log_after: Emit a log message if waiting longer than `log_after`
                          seconds.
        :param timeout_timer: Raise TaskTimeout if StopWatch object
                              `timeout_timer` expires while waiting.
        """
        with self._cond:
            msg = '{} is waiting for {} to complete'.format(caller, self._name)
            self._wait(lambda: not self.complete,
                       msg, log_after, timeout_timer)

    def run_once(self, fn, log_after, timeout_timer):
        """Run a task exactly once. If it is currently running in another
        thread, wait for it to complete. If it has already run, return
        immediately without running it again.

        :param fn: The task to run. It must be a callable taking no arguments.
                   It may optionally return another callable, which also takes
                   no arguments, which will be executed after completion has
                   been signaled to other threads.
        :param log_after: Emit a log message if waiting longer than `log_after`
                          seconds.
        :param timeout_timer: Raise TaskTimeout if StopWatch object
                              `timeout_timer` expires while waiting.
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
                msg = ('%s is waiting for another thread to complete'
                       % self._name)
                self._wait(lambda: self._state == self.RUNNING,
                           msg, log_after, timeout_timer)


class _OrderedTaskRunner:
    """Mixin for a class which executes ordered tasks."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Get a list of methods on this object which have the _ordered
        # attribute
        self._tasks = [name
                       for (name, member) in inspect.getmembers(self)
                       if inspect.ismethod(member) and
                       getattr(member, '_ordered', False)]
        self.reset_states()

        self._reset_lock = threading.Lock()

    def reset_states(self):
        # Create new task states for tasks in reset
        self._states = {task: _OrderedTask(task) for task in self._tasks}

    @staticmethod
    def decorate_ordered(fn, state, after, reset_after):

        @functools.wraps(fn)
        def wrapper(self, *args, **kwargs):
            # If the reset_after state has already completed, reset state so
            # we can run again.
            # NOTE(mdbooth): This is ugly and requires external locking to be
            # deterministic when using multiple threads. Consider a thread that
            # does: server.stop(), server.wait(). If another thread causes a
            # reset between stop() and wait(), this will not have the intended
            # behaviour. It is safe without external locking, if the caller
            # instantiates a new object.
            with self._reset_lock:
                if (reset_after is not None and
                        self._states[reset_after].complete):
                    self.reset_states()

            # Store the states we started with in case the state wraps on us
            # while we're sleeping. We must wait and run_once in the same
            # epoch. If the epoch ended while we were sleeping, run_once will
            # safely do nothing.
            states = self._states

            log_after = kwargs.pop('log_after', DEFAULT_LOG_AFTER)
            timeout = kwargs.pop('timeout', None)

            timeout_timer = None
            if timeout is not None:
                timeout_timer = timeutils.StopWatch(duration=timeout)
                timeout_timer.start()

            # Wait for the given preceding state to complete
            if after is not None:
                states[after].wait_for_completion(state,
                                                  log_after, timeout_timer)

            # Run this state
            states[state].run_once(lambda: fn(self, *args, **kwargs),
                                   log_after, timeout_timer)
        return wrapper


def ordered(after=None, reset_after=None):
    """A method which will be executed as an ordered task. The method will be
    called exactly once, however many times it is called. If it is called
    multiple times simultaneously it will only be called once, but all callers
    will wait until execution is complete.

    If `after` is given, this method will not run until `after` has completed.

    If `reset_after` is given and the target method has completed, allow this
    task to run again by resetting all task states.

    :param after: Optionally, the name of another `ordered` method. Wait for
                  the completion of `after` before executing this method.
    :param reset_after: Optionally, the name of another `ordered` method. Reset
                        all states when calling this method if `reset_after`
                        has completed.
    """
    def _ordered(fn):
        # Set an attribute on the method so we can find it later
        setattr(fn, '_ordered', True)
        state = fn.__name__

        return _OrderedTaskRunner.decorate_ordered(fn, state, after,
                                                   reset_after)
    return _ordered


class MessageHandlingServer(service.ServiceBase, _OrderedTaskRunner,
                            metaclass=abc.ABCMeta):
    """Server for handling messages.

    Connect a transport to a dispatcher that knows how to process the
    message using an executor that knows how the app wants to create
    new tasks.
    """

    @debtcollector.removals.removed_kwarg(
        'executor',
        message="the eventlet executor is now deprecated. Threading "
                "will be the only execution model available.")
    def __init__(self, transport, dispatcher, executor=None):
        """Construct a message handling server.

        The dispatcher parameter is a DispatcherBase instance which is used
        for routing request to endpoint for processing.

        The executor parameter controls how incoming messages will be received
        and dispatched. Executor is automatically detected from
        execution environment.
        It handles many message in parallel. If your application need
        asynchronism then you need to consider to use the eventlet executor.

        :param transport: the messaging transport
        :type transport: Transport
        :param dispatcher: has a dispatch() method which is invoked for each
                           incoming request
        :type dispatcher: DispatcherBase
        :param executor: (DEPRECATED) name of message executor -
            available values are 'eventlet' and 'threading'.
            The Eventlet executor is also deprecated.
        :type executor: str
        """
        if executor and executor not in ("threading", "eventlet"):
            raise ExecutorLoadFailure(
                executor,
                "Executor should be None or 'eventlet' and 'threading'")
        if not executor:
            executor = utils.get_executor_with_context()

        self.conf = transport.conf
        self.conf.register_opts(_pool_opts)

        self.transport = transport
        self.dispatcher = dispatcher
        self.executor_type = executor

        if self.executor_type == "eventlet":
            eventletutils.warn_eventlet_not_patched(
                expected_patched_modules=['thread'],
                what="the 'oslo.messaging eventlet executor'")

            debtcollector.deprecate(
                'Eventlet usages are deprecated and the removal '
                'of Eventlet from OpenStack is planned, for this reason '
                'the Eventlet executor is deprecated. '
                'Start migrating your stack to the '
                'threading executor. Please also start considering '
                'removing your internal Eventlet usages.',
                version="2025.1", removal_version="2026.1",
                category=DeprecationWarning
            )

        self.listener = None

        try:
            mgr = driver.DriverManager('oslo.messaging.executors',
                                       self.executor_type)
        except RuntimeError as ex:
            raise ExecutorLoadFailure(self.executor_type, ex)

        self._executor_cls = mgr.driver

        self._work_executor = None

        self._started = False

        super().__init__()

    def _on_incoming(self, incoming):
        """Handles on_incoming event

        :param incoming: incoming request.
        """
        self._work_executor.submit(self._process_incoming, incoming)

    @abc.abstractmethod
    def _process_incoming(self, incoming):
        """Perform processing incoming request

        :param incoming: incoming request.
        """

    @abc.abstractmethod
    def _create_listener(self):
        """Creates listener object for polling requests
        :return: MessageListenerAdapter
        """

    @ordered(reset_after='stop')
    def start(self, override_pool_size=None):
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
        if self._started:
            LOG.warning('The server has already been started. Ignoring '
                        'the redundant call to start().')
            return

        self._started = True

        executor_opts = {}

        executor_opts["max_workers"] = (
            override_pool_size or self.conf.executor_thread_pool_size
        )
        self._work_executor = self._executor_cls(**executor_opts)

        try:
            self.listener = self._create_listener()
        except driver_base.TransportDriverError as ex:
            raise ServerListenError(self.target, ex)

        self.listener.start(self._on_incoming)

    @ordered(after='start')
    def stop(self):
        """Stop handling incoming messages.

        Once this method returns, no new incoming messages will be handled by
        the server. However, the server may still be in the process of handling
        some messages, and underlying driver resources associated to this
        server are still in use. See 'wait' for more details.
        """
        if self.listener:
            self.listener.stop()
        self._started = False

    @ordered(after='stop')
    def wait(self):
        """Wait for message processing to complete.

        After calling stop(), there may still be some existing messages
        which have not been completely processed. The wait() method blocks
        until all message processing has completed.

        Once it's finished, the underlying driver resources associated to this
        server are released (like closing useless network connections).
        """
        self._work_executor.shutdown(wait=True)

        # Close listener connection after processing all messages
        if self.listener:
            self.listener.cleanup()

    def reset(self):
        """Reset service.

        Called in case service running in daemon mode receives SIGHUP.
        """
        # TODO(sergey.vilgelm): implement this method
        pass
