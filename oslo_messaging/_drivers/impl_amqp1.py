#    Copyright 2014, Red Hat, Inc.
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
Driver for the 'amqp' transport.

This module provides a transport driver that speaks version 1.0 of the AMQP
messaging protocol.  The driver sends messages and creates subscriptions via
'tasks' that are performed on its behalf via the controller module.
"""

import collections
import logging
import os
import threading
import uuid

from oslo_config import cfg
from oslo_messaging.target import Target
from oslo_serialization import jsonutils
from oslo_utils import importutils
from oslo_utils import timeutils

from oslo_messaging._drivers.amqp1_driver.eventloop import compute_timeout
from oslo_messaging._drivers.amqp1_driver import opts
from oslo_messaging._drivers import base
from oslo_messaging._drivers import common
from oslo_messaging._i18n import _LI, _LW


proton = importutils.try_import('proton')
controller = importutils.try_import(
    'oslo_messaging._drivers.amqp1_driver.controller'
)
LOG = logging.getLogger(__name__)

# Build/Decode RPC Response messages
# Body Format - json string containing a map with keys:
# 'failure' - (optional) serialized exception from remote
# 'response' - (if no failure provided) data returned by call


def marshal_response(reply, failure):
    # TODO(grs): do replies have a context?
    # NOTE(flaper87): Set inferred to True since rabbitmq-amqp-1.0 doesn't
    # have support for vbin8.
    msg = proton.Message(inferred=True)
    if failure:
        failure = common.serialize_remote_exception(failure)
        data = {"failure": failure}
    else:
        data = {"response": reply}
    msg.body = jsonutils.dumps(data)
    return msg


def unmarshal_response(message, allowed):
    # TODO(kgiusti) This may fail to unpack and raise an exception. Need to
    # communicate this to the caller!
    data = jsonutils.loads(message.body)
    failure = data.get('failure')
    if failure is not None:
        raise common.deserialize_remote_exception(failure, allowed)
    return data.get("response")


# Build/Decode RPC Request and Notification messages
# Body Format: json string containing a map with keys:
# 'request' - possibly serialized application data
# 'context' - context provided by the application
# 'call_monitor_timeout' - optional time in seconds for RPC call monitoring

def marshal_request(request, context, envelope=False,
                    call_monitor_timeout=None):
    # NOTE(flaper87): Set inferred to True since rabbitmq-amqp-1.0 doesn't
    # have support for vbin8.
    msg = proton.Message(inferred=True)
    if envelope:
        request = common.serialize_msg(request)
    data = {
        "request": request,
        "context": context
    }
    if call_monitor_timeout is not None:
        data["call_monitor_timeout"] = call_monitor_timeout
    msg.body = jsonutils.dumps(data)
    return msg


def unmarshal_request(message):
    data = jsonutils.loads(message.body)
    msg = common.deserialize_msg(data.get("request"))
    return (msg, data.get("context"), data.get("call_monitor_timeout"))


class ProtonIncomingMessage(base.RpcIncomingMessage):
    def __init__(self, listener, message, disposition):
        request, ctxt, client_timeout = unmarshal_request(message)
        super(ProtonIncomingMessage, self).__init__(ctxt, request)
        self.listener = listener
        self.client_timeout = client_timeout
        self._reply_to = message.reply_to
        self._correlation_id = message.id
        self._disposition = disposition

    def heartbeat(self):
        # heartbeats are sent "worst effort": non-blocking, no retries,
        # pre-settled (no blocking for acks). We don't want the server thread
        # being blocked because it is unable to send a heartbeat.
        if not self._reply_to:
            LOG.warning("Cannot send RPC heartbeat: no reply-to provided")
            return
        # send a null msg (no body). This will cause the client to simply reset
        # its timeout (the null message is dropped).  Use time-to-live to
        # prevent stale heartbeats from building up on the message bus
        msg = proton.Message()
        msg.correlation_id = self._correlation_id
        msg.ttl = self.client_timeout
        task = controller.SendTask("RPC KeepAlive", msg, self._reply_to,
                                   deadline=None, retry=0, wait_for_ack=False)
        self.listener.driver._ctrl.add_task(task)
        task.wait()

    def reply(self, reply=None, failure=None):
        """Schedule an RPCReplyTask to send the reply."""
        if self._reply_to:
            response = marshal_response(reply, failure)
            response.correlation_id = self._correlation_id
            driver = self.listener.driver
            deadline = compute_timeout(driver._default_reply_timeout)
            ack = not driver._pre_settle_reply
            task = controller.SendTask("RPC Reply", response, self._reply_to,
                                       # analogous to kombu missing dest t/o:
                                       deadline,
                                       retry=driver._default_reply_retry,
                                       wait_for_ack=ack)
            driver._ctrl.add_task(task)
            rc = task.wait()
            if rc:
                # something failed.  Not much we can do at this point but log
                LOG.debug("RPC Reply failed to send: %s", str(rc))
        else:
            LOG.debug("Ignoring reply as no reply address available")

    def acknowledge(self):
        """Schedule a MessageDispositionTask to send the settlement."""
        task = controller.MessageDispositionTask(self._disposition,
                                                 released=False)
        self.listener.driver._ctrl.add_task(task)

    def requeue(self):
        """Schedule a MessageDispositionTask to release the message"""
        task = controller.MessageDispositionTask(self._disposition,
                                                 released=True)
        self.listener.driver._ctrl.add_task(task)


class Queue(object):
    def __init__(self):
        self._queue = collections.deque()
        self._lock = threading.Lock()
        self._pop_wake_condition = threading.Condition(self._lock)
        self._started = True

    def put(self, item):
        with self._lock:
            self._queue.appendleft(item)
            self._pop_wake_condition.notify()

    def pop(self, timeout):
        with timeutils.StopWatch(timeout) as stop_watcher:
            with self._lock:
                while len(self._queue) == 0:
                    if stop_watcher.expired() or not self._started:
                        return None
                    self._pop_wake_condition.wait(
                        stop_watcher.leftover(return_none=True)
                    )
                return self._queue.pop()

    def stop(self):
        with self._lock:
            self._started = False
            self._pop_wake_condition.notify_all()


class ProtonListener(base.PollStyleListener):
    def __init__(self, driver):
        super(ProtonListener, self).__init__(driver.prefetch_size)
        self.driver = driver
        self.incoming = Queue()
        self.id = uuid.uuid4().hex

    def stop(self):
        self.incoming.stop()

    @base.batch_poll_helper
    def poll(self, timeout=None):
        qentry = self.incoming.pop(timeout)
        if qentry is None:
            return None
        return ProtonIncomingMessage(self,
                                     qentry['message'],
                                     qentry['disposition'])


class ProtonDriver(base.BaseDriver):
    """AMQP 1.0 Driver

    See :doc:`AMQP1.0` for details.
    """

    def __init__(self, conf, url,
                 default_exchange=None, allowed_remote_exmods=[]):
        if proton is None or controller is None:
            raise NotImplementedError("Proton AMQP C libraries not installed")

        super(ProtonDriver, self).__init__(conf, url, default_exchange,
                                           allowed_remote_exmods)

        opt_group = cfg.OptGroup(name='oslo_messaging_amqp',
                                 title='AMQP 1.0 driver options')
        conf.register_group(opt_group)
        conf.register_opts(opts.amqp1_opts, group=opt_group)
        conf = common.ConfigOptsProxy(conf, url, opt_group.name)

        self._conf = conf
        self._default_exchange = default_exchange

        # lazy connection setup - don't create the controller until
        # after the first messaging request:
        self._ctrl = None
        self._pid = None
        self._lock = threading.Lock()

        # timeout for message acknowledgement
        opt_name = conf.oslo_messaging_amqp
        self._default_reply_timeout = opt_name.default_reply_timeout
        self._default_send_timeout = opt_name.default_send_timeout
        self._default_notify_timeout = opt_name.default_notify_timeout
        self._default_reply_retry = opt_name.default_reply_retry

        # which message types should be sent pre-settled?
        ps = [s.lower() for s in opt_name.pre_settled]
        self._pre_settle_call = 'rpc-call' in ps
        self._pre_settle_reply = 'rpc-reply' in ps
        self._pre_settle_cast = 'rpc-cast' in ps
        self._pre_settle_notify = 'notify' in ps
        bad_opts = set(ps).difference(['rpc-call', 'rpc-reply',
                                      'rpc-cast', 'notify'])
        if bad_opts:
            LOG.warning(_LW("Ignoring unrecognized pre_settle value(s): %s"),
                        " ".join(bad_opts))

    def _ensure_connect_called(func):
        """Causes a new controller to be created when the messaging service is
        first used by the current process. It is safe to push tasks to it
        whether connected or not, but those tasks won't be processed until
        connection completes.
        """
        def wrap(self, *args, **kws):
            with self._lock:
                # check to see if a fork was done after the Controller and its
                # I/O thread was spawned.  old_pid will be None the first time
                # this is called which will cause the Controller to be created.
                old_pid = self._pid
                self._pid = os.getpid()

                if old_pid != self._pid:
                    if self._ctrl is not None:
                        # fork was called after the Controller was created, and
                        # we are now executing as the child process.  Do not
                        # touch the existing Controller - it is owned by the
                        # parent.  Best we can do here is simply drop it and
                        # hope we get lucky.
                        LOG.warning(_LW("Process forked after connection "
                                        "established!"))
                        self._ctrl = None
                    # Create a Controller that connects to the messaging
                    # service:
                    self._ctrl = controller.Controller(self._url,
                                                       self._default_exchange,
                                                       self._conf)
                    self._ctrl.connect()
            return func(self, *args, **kws)
        return wrap

    @_ensure_connect_called
    def send(self, target, ctxt, message,
             wait_for_reply=False,
             timeout=None, call_monitor_timeout=None,
             retry=None):
        """Send a message to the given target.

        :param target: destination for message
        :type target: oslo_messaging.Target
        :param ctxt: message context
        :type ctxt: dict
        :param message: message payload
        :type message: dict
        :param wait_for_reply: expects a reply message, wait for it
        :type wait_for_reply: bool
        :param timeout: raise exception if send does not complete within
                        timeout seconds. None == no timeout.
        :type timeout: float
        :param call_monitor_timeout: Maximum time the client will wait for the
            call to complete or receive a message heartbeat indicating the
            remote side is still executing.
        :type call_monitor_timeout: float
        :param retry: (optional) maximum re-send attempts on recoverable error
                      None or -1 means to retry forever
                      0 means no retry
                      N means N retries
        :type retry: int
        """
        request = marshal_request(message, ctxt, None,
                                  call_monitor_timeout)
        if timeout:
            expire = compute_timeout(timeout)
            request.ttl = timeout
            request.expiry_time = compute_timeout(timeout)
        else:
            # no timeout provided by application.  If the backend is queueless
            # this could lead to a hang - provide a default to prevent this
            # TODO(kgiusti) only do this if brokerless backend
            expire = compute_timeout(self._default_send_timeout)
        if wait_for_reply:
            ack = not self._pre_settle_call
            if call_monitor_timeout is None:
                task = controller.RPCCallTask(target, request, expire, retry,
                                              wait_for_ack=ack)
            else:
                task = controller.RPCMonitoredCallTask(target, request, expire,
                                                       call_monitor_timeout,
                                                       retry, wait_for_ack=ack)
        else:
            ack = not self._pre_settle_cast
            task = controller.SendTask("RPC Cast", request, target, expire,
                                       retry, wait_for_ack=ack)
        self._ctrl.add_task(task)

        reply = task.wait()
        if isinstance(reply, Exception):
            raise reply
        if reply:
            # TODO(kgiusti) how to handle failure to un-marshal?
            # Must log, and determine best way to communicate this failure
            # back up to the caller
            reply = unmarshal_response(reply, self._allowed_remote_exmods)
        return reply

    @_ensure_connect_called
    def send_notification(self, target, ctxt, message, version,
                          retry=None):
        """Send a notification message to the given target.

        :param target: destination for message
        :type target: oslo_messaging.Target
        :param ctxt: message context
        :type ctxt: dict
        :param message: message payload
        :type message: dict
        :param version: message envelope version
        :type version: float
        :param retry: (optional) maximum re-send attempts on recoverable error
                      None or -1 means to retry forever
                      0 means no retry
                      N means N retries
        :type retry: int
        """
        request = marshal_request(message, ctxt, envelope=(version == 2.0))
        # no timeout is applied to notifications, however if the backend is
        # queueless this could lead to a hang - provide a default to prevent
        # this
        # TODO(kgiusti) should raise NotImplemented if not broker backend
        deadline = compute_timeout(self._default_notify_timeout)
        ack = not self._pre_settle_notify
        task = controller.SendTask("Notify", request, target,
                                   deadline, retry, wait_for_ack=ack,
                                   notification=True)
        self._ctrl.add_task(task)
        rc = task.wait()
        if isinstance(rc, Exception):
            raise rc

    @_ensure_connect_called
    def listen(self, target, batch_size, batch_timeout):
        """Construct a Listener for the given target."""
        LOG.debug("Listen to %s", target)
        listener = ProtonListener(self)
        task = controller.SubscribeTask(target, listener)
        self._ctrl.add_task(task)
        task.wait()
        return base.PollStyleListenerAdapter(listener, batch_size,
                                             batch_timeout)

    @_ensure_connect_called
    def listen_for_notifications(self, targets_and_priorities, pool,
                                 batch_size, batch_timeout):
        """Construct a Listener for notifications on the given target and
        priority.
        """
        # TODO(kgiusti) should raise NotImplemented if not broker backend
        LOG.debug("Listen for notifications %s", targets_and_priorities)
        if pool:
            raise NotImplementedError('"pool" not implemented by '
                                      'this transport driver')
        listener = ProtonListener(self)
        # this is how the destination target is created by the notifier,
        # see MessagingDriver.notify in oslo_messaging/notify/messaging.py
        for target, priority in targets_and_priorities:
            topic = '%s.%s' % (target.topic, priority)
            # Sooo... the exchange is simply discarded? (see above comment)
            task = controller.SubscribeTask(Target(topic=topic),
                                            listener, notifications=True)
            self._ctrl.add_task(task)
            task.wait()
        return base.PollStyleListenerAdapter(listener, batch_size,
                                             batch_timeout)

    def cleanup(self):
        """Release all resources."""
        if self._ctrl:
            self._ctrl.shutdown()
            self._ctrl = None
        LOG.info(_LI("AMQP 1.0 messaging driver shutdown"))

    def require_features(self, requeue=True):
        pass
