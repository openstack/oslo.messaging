
# Copyright 2013 Red Hat, Inc.
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
import os
import queue
import threading
import time
import uuid

import cachetools
from oslo_concurrency import lockutils
from oslo_utils import eventletutils
from oslo_utils import timeutils

import oslo_messaging
from oslo_messaging._drivers import amqp as rpc_amqp
from oslo_messaging._drivers import base
from oslo_messaging._drivers import common as rpc_common
from oslo_messaging import MessageDeliveryFailure

__all__ = ['AMQPDriverBase']

LOG = logging.getLogger(__name__)

# Minimum/Maximum sleep between a poll and ack/requeue
# Maximum should be small enough to not get rejected ack,
# minimum should be big enough to not burn the CPU.
ACK_REQUEUE_EVERY_SECONDS_MIN = 0.001
ACK_REQUEUE_EVERY_SECONDS_MAX = 5.0


class QManager(object):
    """Queue Manager to build queue name for reply (and fanout) type.
    This class is used only when use_queue_manager is set to True in config
    file.
    It rely on a shared memory accross processes (reading/writing data to
    /dev/shm/xyz) and oslo_concurrency.lockutils to avoid assigning the same
    queue name twice (or more) to different processes.
    The original idea of this queue manager was to avoid random queue names,
    so on service restart, the previously created queues can be reused,
    avoiding deletion/creation of queues on rabbitmq side (which cost a lot
    at scale).
    """
    def __init__(self, hostname, processname):
        # We will use hostname and processname in queue names to help identify
        # them easily.
        # This is also ensuring consistency between service restart.
        self.hostname = hostname
        self.processname = processname
        # This is where the counter is kept
        self.file_name = '/dev/shm/%s_%s_qmanager' % (self.hostname,  # nosec
                                                      self.processname)
        # We use the process group to restart the counter on service restart
        self.pg = os.getpgrp()

        # We need to also handle containerized deployments, so let's
        # parse start time (in jiffies) since system boot
        #
        # https://www.man7.org/linux/man-pages//man5/proc_pid_stat.5.html
        with open(f'/proc/{self.pg}/stat', 'r') as f:
            self.start_time = int(f.read().split()[21])

    def get(self):
        lock_name = 'oslo_read_shm_%s_%s' % (self.hostname, self.processname)

        @lockutils.synchronized(lock_name, external=True)
        def read_from_shm():
            # Grab the counter from shm
            # This function is thread and process safe thanks to lockutils
            try:
                with open(self.file_name, 'r') as f:
                    pg, counter, start_time = f.readline().split(':')
                    pg = int(pg)
                    counter = int(counter)
                    start_time = int(start_time)
            except (FileNotFoundError, ValueError):
                pg = self.pg
                counter = 0
                start_time = self.start_time

            # Increment the counter
            if pg == self.pg and start_time == self.start_time:
                counter += 1
            else:
                # The process group is changed, or start time since system boot
                # differs. Maybe service restarted ?
                # Start over the counter
                counter = 1

            # Write the new counter
            with open(self.file_name, 'w') as f:
                f.write(str(self.pg) + ':' + str(counter) + ':' +
                        str(self.start_time))
            return counter

        counter = read_from_shm()
        return self.hostname + ":" + self.processname + ":" + str(counter)


class MessageOperationsHandler(object):
    """Queue used by message operations to ensure that all tasks are
    serialized and run in the same thread, since underlying drivers like kombu
    are not thread safe.
    """

    def __init__(self, name):
        self.name = "%s (%s)" % (name, hex(id(self)))
        self._tasks = queue.Queue()

        self._shutdown = eventletutils.Event()
        self._shutdown_thread = threading.Thread(
            target=self._process_in_background)
        self._shutdown_thread.daemon = True

    def stop(self):
        self._shutdown.set()

    def process_in_background(self):
        """Run all pending tasks queued by do() in an thread during the
        shutdown process.
        """
        self._shutdown_thread.start()

    def _process_in_background(self):
        while not self._shutdown.is_set():
            self.process()
            time.sleep(ACK_REQUEUE_EVERY_SECONDS_MIN)

    def process(self):
        "Run all pending tasks queued by do()."

        while True:
            try:
                task = self._tasks.get(block=False)
            except queue.Empty:
                break
            task()

    def do(self, task):
        "Put the task in the queue."
        self._tasks.put(task)


class AMQPIncomingMessage(base.RpcIncomingMessage):

    def __init__(self, listener, ctxt, message, unique_id, msg_id, reply_q,
                 client_timeout, obsolete_reply_queues,
                 message_operations_handler):
        super(AMQPIncomingMessage, self).__init__(ctxt, message, msg_id)
        self.orig_msg_id = msg_id
        self.listener = listener

        self.unique_id = unique_id
        self.reply_q = reply_q
        self.client_timeout = client_timeout
        self._obsolete_reply_queues = obsolete_reply_queues
        self._message_operations_handler = message_operations_handler
        self.stopwatch = timeutils.StopWatch()
        self.stopwatch.start()

    def _send_reply(self, conn, reply=None, failure=None, ending=True):
        if not self._obsolete_reply_queues.reply_q_valid(self.reply_q,
                                                         self.msg_id):
            return

        if failure:
            failure = rpc_common.serialize_remote_exception(failure)
        # NOTE(sileht): ending can be removed in N*, see Listener.wait()
        # for more detail.
        msg = {'result': reply, 'failure': failure, 'ending': ending,
               '_msg_id': self.msg_id}
        rpc_amqp._add_unique_id(msg)
        unique_id = msg[rpc_amqp.UNIQUE_ID]

        LOG.debug("sending reply msg_id: %(msg_id)s "
                  "reply queue: %(reply_q)s "
                  "time elapsed: %(elapsed)ss", {
                      'msg_id': self.msg_id,
                      'unique_id': unique_id,
                      'reply_q': self.reply_q,
                      'elapsed': self.stopwatch.elapsed()})
        conn.direct_send(self.reply_q, rpc_common.serialize_msg(msg))

    def reply(self, reply=None, failure=None):
        if not self.orig_msg_id:
            # NOTE(Alexei_987) not sending reply, if msg_id is empty
            #    because reply should not be expected by caller side
            return

        # NOTE(sileht): return without hold the a connection if possible
        if not self._obsolete_reply_queues.reply_q_valid(self.reply_q,
                                                         self.msg_id):
            return

        # NOTE(sileht): we read the configuration value from the driver
        # to be able to backport this change in previous version that
        # still have the qpid driver
        duration = self.listener.driver.missing_destination_retry_timeout
        timer = rpc_common.DecayingTimer(duration=duration)
        timer.start()

        while True:
            try:
                with self.listener.driver._get_connection(
                    rpc_common.PURPOSE_SEND,
                ) as conn:
                    self._send_reply(conn, reply, failure)

                return
            except oslo_messaging.MessageUndeliverable:
                # queue not found
                if timer.check_return() <= 0:
                    self._obsolete_reply_queues.add(self.reply_q, self.msg_id)
                    LOG.error(
                        'The reply %(msg_id)s failed to send after '
                        '%(duration)d seconds due to a missing queue '
                        '(%(reply_q)s). Abandoning...', {
                            'msg_id': self.msg_id,
                            'duration': duration,
                            'reply_q': self.reply_q})
                    return

                LOG.debug(
                    'The reply %(msg_id)s could not be sent due to a missing '
                    'queue (%(reply_q)s). Retrying...', {
                        'msg_id': self.msg_id,
                        'reply_q': self.reply_q})
                time.sleep(0.25)
            except rpc_amqp.AMQPDestinationNotFound as exc:
                # exchange not found/down
                if timer.check_return() <= 0:
                    self._obsolete_reply_queues.add(self.reply_q, self.msg_id)
                    LOG.error(
                        'The reply %(msg_id)s failed to send after '
                        '%(duration)d seconds due to a broker issue '
                        '(%(exc)s). Abandoning...', {
                            'msg_id': self.msg_id,
                            'duration': duration,
                            'exc': exc})
                    return

                LOG.debug(
                    'The reply %(msg_id)s could not be sent due to a broker '
                    'issue (%(exc)s). Retrying...', {
                        'msg_id': self.msg_id,
                        'exc': exc})
                time.sleep(0.25)

    def heartbeat(self):
        # generate a keep alive for RPC call monitoring
        with self.listener.driver._get_connection(
            rpc_common.PURPOSE_SEND,
        ) as conn:
            try:
                self._send_reply(conn, None, None, ending=False)
            except oslo_messaging.MessageUndeliverable:
                # internal exception that indicates queue gone -
                # broker unreachable.
                raise MessageDeliveryFailure(
                    "Heartbeat send failed. Missing queue")
            except rpc_amqp.AMQPDestinationNotFound:
                # internal exception that indicates exchange gone -
                # broker unreachable.
                raise MessageDeliveryFailure(
                    "Heartbeat send failed. Missing exchange")

    # NOTE(sileht): Those have already be ack in RpcListener IO thread
    # We keep them as noop until all drivers do the same
    def acknowledge(self):
        pass

    def requeue(self):
        pass


class NotificationAMQPIncomingMessage(AMQPIncomingMessage):
    def acknowledge(self):
        def _do_ack():
            try:
                self.message.acknowledge()
            except Exception as exc:
                # NOTE(kgiusti): this failure is likely due to a loss of the
                # connection to the broker.  Not much we can do in this case,
                # especially considering the Notification has already been
                # dispatched. This *could* result in message duplication
                # (unacked msg is returned to the queue by the broker), but the
                # driver tries to catch that using the msg_id_cache.
                LOG.warning("Failed to acknowledge received message: %s", exc)
        self._message_operations_handler.do(_do_ack)
        self.listener.msg_id_cache.add(self.unique_id)

    def requeue(self):
        # NOTE(sileht): In case of the connection is lost between receiving the
        # message and requeing it, this requeue call fail
        # but because the message is not acknowledged and not added to the
        # msg_id_cache, the message will be reconsumed, the only difference is
        # the message stay at the beginning of the queue instead of moving to
        # the end.
        def _do_requeue():
            try:
                self.message.requeue()
            except Exception as exc:
                LOG.warning("Failed to requeue received message: %s", exc)
        self._message_operations_handler.do(_do_requeue)


class ObsoleteReplyQueuesCache(object):
    """Cache of reply queue id that doesn't exist anymore.

    NOTE(sileht): In case of a broker restart/failover
    a reply queue can be unreachable for short period
    the IncomingMessage.send_reply will block for 60 seconds
    in this case or until rabbit recovers.

    But in case of the reply queue is unreachable because the
    rpc client is really gone, we can have a ton of reply to send
    waiting 60 seconds.
    This leads to a starvation of connection of the pool
    The rpc server take to much time to send reply, other rpc client will
    raise TimeoutError because their don't receive their replies in time.

    This object cache stores already known gone client to not wait 60 seconds
    and hold a connection of the pool.
    Keeping 200 last gone rpc client for 1 minute is enough
    and doesn't hold to much memory.
    """

    SIZE = 200
    TTL = 60

    def __init__(self):
        self._lock = threading.RLock()
        self._cache = cachetools.TTLCache(self.SIZE, self.TTL)

    def reply_q_valid(self, reply_q, msg_id):
        if reply_q in self._cache:
            self._no_reply_log(reply_q, msg_id)
            return False
        return True

    def add(self, reply_q, msg_id):
        with self._lock:
            self._cache.update({reply_q: msg_id})
        self._no_reply_log(reply_q, msg_id)

    def _no_reply_log(self, reply_q, msg_id):
        LOG.warning("%(reply_queue)s doesn't exist, drop reply to "
                    "%(msg_id)s", {'reply_queue': reply_q, "msg_id": msg_id})


class AMQPListener(base.PollStyleListener):
    use_cache = False

    def __init__(self, driver, conn):
        super(AMQPListener, self).__init__(driver.prefetch_size)
        self.driver = driver
        self.conn = conn
        self.msg_id_cache = rpc_amqp._MsgIdCache()
        self.incoming = []
        self._shutdown = eventletutils.Event()
        self._shutoff = eventletutils.Event()
        self._obsolete_reply_queues = ObsoleteReplyQueuesCache()
        self._message_operations_handler = MessageOperationsHandler(
            "AMQPListener")
        self._current_timeout = ACK_REQUEUE_EVERY_SECONDS_MIN

    def __call__(self, message):
        ctxt = rpc_amqp.unpack_context(message)
        try:
            unique_id = self.msg_id_cache.check_duplicate_message(message)
        except rpc_common.DuplicateMessageError:
            LOG.exception("ignoring duplicate message %s", ctxt.msg_id)
            return
        if self.use_cache:
            self.msg_id_cache.add(unique_id)
        if ctxt.msg_id:
            LOG.debug("received message msg_id: %(msg_id)s reply to "
                      "%(queue)s", {'queue': ctxt.reply_q,
                                    'msg_id': ctxt.msg_id})
        else:
            LOG.debug("received message with unique_id: %s", unique_id)

        self.incoming.append(self.message_cls(
            self,
            ctxt.to_dict(),
            message,
            unique_id,
            ctxt.msg_id,
            ctxt.reply_q,
            ctxt.client_timeout,
            self._obsolete_reply_queues,
            self._message_operations_handler))

    @base.batch_poll_helper
    def poll(self, timeout=None):
        stopwatch = timeutils.StopWatch(duration=timeout).start()

        while not self._shutdown.is_set():
            self._message_operations_handler.process()
            LOG.debug("Listener is running")

            if self.incoming:
                LOG.debug("Poll the incoming message with unique_id: %s",
                          self.incoming[0].unique_id)
                return self.incoming.pop(0)

            left = stopwatch.leftover(return_none=True)
            if left is None:
                left = self._current_timeout
            if left <= 0:
                return None

            try:
                LOG.debug("AMQPListener connection consume")
                self.conn.consume(timeout=min(self._current_timeout, left))
            except rpc_common.Timeout:
                LOG.debug("AMQPListener connection timeout")
                self._current_timeout = min(self._current_timeout * 2,
                                            ACK_REQUEUE_EVERY_SECONDS_MAX)
            else:
                self._current_timeout = ACK_REQUEUE_EVERY_SECONDS_MIN

        # NOTE(sileht): listener is stopped, just processes remaining messages
        # and operations
        LOG.debug("Listener is stopped")
        self._message_operations_handler.process()
        if self.incoming:
            LOG.debug("Poll the incoming message with unique_id: %s",
                      self.incoming[0].unique_id)
            return self.incoming.pop(0)

        self._shutoff.set()

    def stop(self):
        self._shutdown.set()
        self.conn.stop_consuming()
        self._shutoff.wait()

        # NOTE(sileht): Here, the listener is stopped, but some incoming
        # messages may still live on server side, because callback is still
        # running and message is not yet ack/requeue. It's safe to do the ack
        # into another thread, side the polling thread is now terminated.
        self._message_operations_handler.process_in_background()

    def cleanup(self):
        # NOTE(sileht): server executor is now stopped, we are sure that no
        # more incoming messages in live, we can acknowledge
        # remaining messages and stop the thread
        self._message_operations_handler.stop()
        # Closes listener connection
        self.conn.close()


class RpcAMQPListener(AMQPListener):
    message_cls = AMQPIncomingMessage
    use_cache = True

    def __call__(self, message):
        # NOTE(kgiusti): In the original RPC implementation the RPC server
        # would acknowledge the request THEN process it.  The goal of this was
        # to prevent duplication if the ack failed.  Should the ack fail the
        # request would be discarded since the broker would not remove the
        # request from the queue since no ack was received.  That would lead to
        # the request being redelivered at some point. However this approach
        # meant that the ack was issued from the dispatch thread, not the
        # consumer thread, which is bad since kombu is not thread safe.  So a
        # change was made to schedule the ack to be sent on the consumer thread
        # - breaking the ability to catch ack errors before dispatching the
        # request.  To fix this we do the actual ack here in the consumer
        # callback and avoid the upcall if the ack fails.  See
        # https://bugs.launchpad.net/oslo.messaging/+bug/1695746
        # for all the gory details...
        try:
            message.acknowledge()
        except Exception as exc:
            LOG.warning("Discarding RPC request due to failed acknowledge: %s",
                        exc)
        else:
            # NOTE(kgiusti): be aware that even if the acknowledge call
            # succeeds there is no guarantee the broker actually gets the ACK
            # since acknowledge() simply writes the ACK to the socket (there is
            # no ACK confirmation coming back from the broker)
            super(RpcAMQPListener, self).__call__(message)


class NotificationAMQPListener(AMQPListener):
    message_cls = NotificationAMQPIncomingMessage


class ReplyWaiters(object):

    def __init__(self):
        self._queues = {}
        self._wrn_threshold = 10

    def get(self, msg_id, timeout):
        watch = timeutils.StopWatch(duration=timeout)
        watch.start()
        while not watch.expired():
            try:
                # NOTE(amorin) we can't use block=True
                # See lp-2035113
                return self._queues[msg_id].get(block=False)
            except queue.Empty:
                time.sleep(0.5)
        raise oslo_messaging.MessagingTimeout(
            'Timed out waiting for a reply '
            'to message ID %s' % msg_id)

    def put(self, msg_id, message_data):
        LOG.debug('Received RPC response for msg %s', msg_id)
        queue = self._queues.get(msg_id)
        if not queue:
            LOG.info('No calling threads waiting for msg_id : %s', msg_id)
            LOG.debug(' queues: %(queues)s, message: %(message)s',
                      {'queues': len(self._queues), 'message': message_data})
        else:
            queue.put(message_data)

    def add(self, msg_id):
        self._queues[msg_id] = queue.Queue()
        queues_length = len(self._queues)
        if queues_length > self._wrn_threshold:
            LOG.warning('Number of call queues is %(queues_length)s, '
                        'greater than warning threshold: %(old_threshold)s. '
                        'There could be a leak. Increasing threshold to: '
                        '%(threshold)s',
                        {'queues_length': queues_length,
                         'old_threshold': self._wrn_threshold,
                         'threshold': self._wrn_threshold * 2})
            self._wrn_threshold *= 2

    def remove(self, msg_id):
        del self._queues[msg_id]


class ReplyWaiter(object):
    def __init__(self, reply_q, conn, allowed_remote_exmods):
        self.conn = conn
        self.allowed_remote_exmods = allowed_remote_exmods
        self.msg_id_cache = rpc_amqp._MsgIdCache()
        self.waiters = ReplyWaiters()

        self.conn.declare_direct_consumer(reply_q, self)

        self._thread_exit_event = eventletutils.Event()
        self._thread = threading.Thread(target=self.poll)
        self._thread.daemon = True
        self._thread.start()

    def stop(self):
        if self._thread:
            self._thread_exit_event.set()
            self.conn.stop_consuming()
            self._thread.join()
            self._thread = None

    def poll(self):
        current_timeout = ACK_REQUEUE_EVERY_SECONDS_MIN
        while not self._thread_exit_event.is_set():
            try:
                # ack every ACK_REQUEUE_EVERY_SECONDS_MAX seconds
                self.conn.consume(timeout=current_timeout)
            except rpc_common.Timeout:
                current_timeout = min(current_timeout * 2,
                                      ACK_REQUEUE_EVERY_SECONDS_MAX)
            except Exception:
                LOG.exception("Failed to process incoming message, retrying..")
            else:
                current_timeout = ACK_REQUEUE_EVERY_SECONDS_MIN

    def __call__(self, message):
        # NOTE(sileht): __call__ is running within the polling thread,
        # (conn.consume -> conn.conn.drain_events() -> __call__ callback)
        # it's threadsafe to acknowledge the message here, no need to wait
        # the next polling
        message.acknowledge()
        incoming_msg_id = message.pop('_msg_id', None)
        if message.get('ending'):
            LOG.debug("received reply msg_id: %s", incoming_msg_id)
        self.waiters.put(incoming_msg_id, message)

    def listen(self, msg_id):
        self.waiters.add(msg_id)

    def unlisten(self, msg_id):
        self.waiters.remove(msg_id)

    @staticmethod
    def _raise_timeout_exception(msg_id, reply_q):
        raise oslo_messaging.MessagingTimeout(
            'Timed out waiting for a reply %(reply_q)s '
            'to message ID %(msg_id)s.',
            {'msg_id': msg_id, 'reply_q': reply_q})

    def _process_reply(self, data):
        self.msg_id_cache.check_duplicate_message(data)
        if data['failure']:
            failure = data['failure']
            result = rpc_common.deserialize_remote_exception(
                failure, self.allowed_remote_exmods)
        else:
            result = data.get('result', None)

        ending = data.get('ending', False)
        return result, ending

    def wait(self, msg_id, timeout, call_monitor_timeout, reply_q):
        # NOTE(sileht): for each msg_id we receive two amqp message
        # first one with the payload, a second one to ensure the other
        # have finish to send the payload
        # NOTE(viktors): We are going to remove this behavior in the N
        # release, but we need to keep backward compatibility, so we should
        # support both cases for now.
        timer = rpc_common.DecayingTimer(duration=timeout)
        timer.start()
        if call_monitor_timeout:
            call_monitor_timer = rpc_common.DecayingTimer(
                duration=call_monitor_timeout)
            call_monitor_timer.start()
        else:
            call_monitor_timer = None
        final_reply = None
        ending = False
        while not ending:
            timeout = timer.check_return(
                self._raise_timeout_exception,
                msg_id,
                reply_q
            )
            if call_monitor_timer and timeout > 0:
                cm_timeout = call_monitor_timer.check_return(
                    self._raise_timeout_exception,
                    msg_id,
                    reply_q
                )
                if cm_timeout < timeout:
                    timeout = cm_timeout
            try:
                message = self.waiters.get(msg_id, timeout=timeout)
            except queue.Empty:
                self._raise_timeout_exception(
                    msg_id,
                    reply_q
                )

            reply, ending = self._process_reply(message)
            if reply is not None:
                # NOTE(viktors): This can be either first _send_reply() with an
                # empty `result` field or a second _send_reply() with
                # ending=True and no `result` field.
                final_reply = reply
            elif ending is False:
                LOG.debug('Call monitor heartbeat received; '
                          'renewing timeout timer')
                call_monitor_timer.restart()
        return final_reply


class AMQPDriverBase(base.BaseDriver):
    missing_destination_retry_timeout = 0

    def __init__(self, conf, url, connection_pool,
                 default_exchange=None, allowed_remote_exmods=None):
        super(AMQPDriverBase, self).__init__(conf, url, default_exchange,
                                             allowed_remote_exmods)

        self._default_exchange = default_exchange

        self._connection_pool = connection_pool

        self._reply_q_lock = threading.Lock()
        self._reply_q = None
        self._reply_q_conn = None
        self._waiter = None
        if conf.oslo_messaging_rabbit.use_queue_manager:
            self._q_manager = QManager(
                hostname=conf.oslo_messaging_rabbit.hostname,
                processname=conf.oslo_messaging_rabbit.processname)
        else:
            self._q_manager = None

    def _get_exchange(self, target):
        return target.exchange or self._default_exchange

    def _get_connection(self, purpose=rpc_common.PURPOSE_SEND, retry=None):
        return rpc_common.ConnectionContext(self._connection_pool,
                                            purpose=purpose,
                                            retry=retry)

    def _get_reply_q(self):
        with self._reply_q_lock:
            # NOTE(amorin) Re-use reply_q when it already exists
            # This avoid creating too many queues on AMQP server (rabbit)
            if self._reply_q is not None:
                return self._reply_q

            if self._q_manager:
                reply_q = 'reply_' + self._q_manager.get()
            else:
                reply_q = 'reply_' + uuid.uuid4().hex
            LOG.debug('Creating reply queue: %s', reply_q)

            conn = self._get_connection(rpc_common.PURPOSE_LISTEN)

            self._waiter = ReplyWaiter(reply_q, conn,
                                       self._allowed_remote_exmods)

            self._reply_q = reply_q
            self._reply_q_conn = conn

        return self._reply_q

    def _send(self, target, ctxt, message,
              wait_for_reply=None, timeout=None, call_monitor_timeout=None,
              envelope=True, notify=False, retry=None, transport_options=None):

        msg = message
        reply_q = None
        if 'method' in msg:
            LOG.debug('Calling RPC method %s on target %s', msg.get('method'),
                      target.topic)
        else:
            LOG.debug('Sending message to topic %s', target.topic)

        if wait_for_reply:
            reply_q = self._get_reply_q()
            msg_id = uuid.uuid4().hex
            msg.update({'_msg_id': msg_id})
            msg.update({'_reply_q': reply_q})
            msg.update({'_timeout': call_monitor_timeout})
            LOG.debug('Expecting reply to msg %s in queue %s', msg_id,
                      reply_q)

        rpc_amqp._add_unique_id(msg)
        unique_id = msg[rpc_amqp.UNIQUE_ID]

        rpc_amqp.pack_context(msg, ctxt)

        if envelope:
            msg = rpc_common.serialize_msg(msg)

        if wait_for_reply:
            self._waiter.listen(msg_id)
            log_msg = "CALL msg_id: %s " % msg_id
        else:
            log_msg = "CAST unique_id: %s " % unique_id

        try:
            with self._get_connection(rpc_common.PURPOSE_SEND, retry) as conn:
                if notify:
                    exchange = self._get_exchange(target)
                    LOG.debug(log_msg + "NOTIFY exchange '%(exchange)s'"
                              " topic '%(topic)s'", {'exchange': exchange,
                                                     'topic': target.topic})
                    conn.notify_send(exchange, target.topic, msg, retry=retry)
                elif target.fanout:
                    log_msg += "FANOUT topic '%(topic)s'" % {
                        'topic': target.topic}
                    LOG.debug(log_msg)
                    conn.fanout_send(target.topic, msg, retry=retry)
                else:
                    topic = target.topic
                    exchange = self._get_exchange(target)
                    if target.server:
                        topic = '%s.%s' % (target.topic, target.server)
                    LOG.debug(log_msg + "exchange '%(exchange)s'"
                              " topic '%(topic)s'", {'exchange': exchange,
                                                     'topic': topic})
                    conn.topic_send(exchange_name=exchange, topic=topic,
                                    msg=msg, timeout=timeout, retry=retry,
                                    transport_options=transport_options)

            if wait_for_reply:
                result = self._waiter.wait(msg_id, timeout,
                                           call_monitor_timeout, reply_q)
                if isinstance(result, Exception):
                    raise result
                return result
        finally:
            if wait_for_reply:
                self._waiter.unlisten(msg_id)

    def send(self, target, ctxt, message, wait_for_reply=None, timeout=None,
             call_monitor_timeout=None, retry=None, transport_options=None):
        return self._send(target, ctxt, message, wait_for_reply, timeout,
                          call_monitor_timeout, retry=retry,
                          transport_options=transport_options)

    def send_notification(self, target, ctxt, message, version, retry=None):
        return self._send(target, ctxt, message,
                          envelope=(version == 2.0), notify=True, retry=retry)

    def listen(self, target, batch_size, batch_timeout):
        conn = self._get_connection(rpc_common.PURPOSE_LISTEN)

        listener = RpcAMQPListener(self, conn)

        conn.declare_topic_consumer(exchange_name=self._get_exchange(target),
                                    topic=target.topic,
                                    callback=listener)
        conn.declare_topic_consumer(exchange_name=self._get_exchange(target),
                                    topic='%s.%s' % (target.topic,
                                                     target.server),
                                    callback=listener)
        conn.declare_fanout_consumer(target.topic, listener)

        return base.PollStyleListenerAdapter(listener, batch_size,
                                             batch_timeout)

    def listen_for_notifications(self, targets_and_priorities, pool,
                                 batch_size, batch_timeout):
        conn = self._get_connection(rpc_common.PURPOSE_LISTEN)

        listener = NotificationAMQPListener(self, conn)
        for target, priority in targets_and_priorities:
            conn.declare_topic_consumer(
                exchange_name=self._get_exchange(target),
                topic='%s.%s' % (target.topic, priority),
                callback=listener, queue_name=pool)
        return base.PollStyleListenerAdapter(listener, batch_size,
                                             batch_timeout)

    def cleanup(self):
        if self._connection_pool:
            self._connection_pool.empty()
        self._connection_pool = None

        with self._reply_q_lock:
            if self._reply_q is not None:
                self._waiter.stop()
                self._reply_q_conn.close()
                self._reply_q_conn = None
                self._reply_q = None
                self._waiter = None
