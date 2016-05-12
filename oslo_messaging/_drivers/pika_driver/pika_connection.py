#    Copyright 2016 Mirantis, Inc.
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

import collections
import logging
import os
import threading

import futurist
from pika.adapters import select_connection
from pika import exceptions as pika_exceptions
from pika import spec as pika_spec

from oslo_utils import eventletutils

current_thread = eventletutils.fetch_current_thread_functor()

LOG = logging.getLogger(__name__)


class ThreadSafePikaConnection(object):
    def __init__(self, parameters=None,
                 _impl_class=select_connection.SelectConnection):
        self.params = parameters
        self._connection_lock = threading.Lock()
        self._evt_closed = threading.Event()
        self._task_queue = collections.deque()
        self._pending_connection_futures = set()

        create_connection_future = self._register_pending_future()

        def on_open_error(conn, err):
            create_connection_future.set_exception(
                pika_exceptions.AMQPConnectionError(err)
            )

        self._impl = _impl_class(
            parameters=parameters,
            on_open_callback=create_connection_future.set_result,
            on_open_error_callback=on_open_error,
            on_close_callback=self._on_connection_close,
            stop_ioloop_on_close=False,
        )
        self._interrupt_pipein, self._interrupt_pipeout = os.pipe()
        self._impl.ioloop.add_handler(self._interrupt_pipein,
                                      self._impl.ioloop.read_interrupt,
                                      select_connection.READ)

        self._thread = threading.Thread(target=self._process_io)
        self._thread.daemon = True
        self._thread_id = None
        self._thread.start()

        create_connection_future.result()

    def _check_called_not_from_event_loop(self):
        if current_thread() == self._thread_id:
            raise RuntimeError("This call is not allowed from ioloop thread")

    def _execute_task(self, func, *args, **kwargs):
        if current_thread() == self._thread_id:
            return func(*args, **kwargs)

        future = futurist.Future()
        self._task_queue.append((func, args, kwargs, future))

        if self._evt_closed.is_set():
            self._notify_all_futures_connection_close()
        elif self._interrupt_pipeout is not None:
            os.write(self._interrupt_pipeout, b'X')

        return future.result()

    def _register_pending_future(self):
        future = futurist.Future()
        self._pending_connection_futures.add(future)

        def on_done_callback(fut):
            try:
                self._pending_connection_futures.remove(fut)
            except KeyError:
                pass

        future.add_done_callback(on_done_callback)

        if self._evt_closed.is_set():
            self._notify_all_futures_connection_close()
        return future

    def _notify_all_futures_connection_close(self):
        while self._task_queue:
            try:
                method_res_future = self._task_queue.pop()[3]
            except KeyError:
                break
            else:
                method_res_future.set_exception(
                    pika_exceptions.ConnectionClosed()
                )

        while self._pending_connection_futures:
            try:
                pending_connection_future = (
                    self._pending_connection_futures.pop()
                )
            except KeyError:
                break
            else:
                pending_connection_future.set_exception(
                    pika_exceptions.ConnectionClosed()
                )

    def _on_connection_close(self, conn, reply_code, reply_text):
        self._evt_closed.set()
        self._notify_all_futures_connection_close()
        if self._interrupt_pipeout:
            os.close(self._interrupt_pipeout)
            os.close(self._interrupt_pipein)

    def add_on_close_callback(self, callback):
        return self._execute_task(self._impl.add_on_close_callback, callback)

    def _do_process_io(self):
        while self._task_queue:
            func, args, kwargs, future = self._task_queue.pop()
            try:
                res = func(*args, **kwargs)
            except BaseException as e:
                LOG.exception(e)
                future.set_exception(e)
            else:
                future.set_result(res)

        self._impl.ioloop.poll()
        self._impl.ioloop.process_timeouts()

    def _process_io(self):
        self._thread_id = current_thread()
        while not self._evt_closed.is_set():
            try:
                self._do_process_io()
            except BaseException:
                LOG.exception("Error during processing connection's IO")

    def close(self, *args, **kwargs):
        self._check_called_not_from_event_loop()

        res = self._execute_task(self._impl.close, *args, **kwargs)

        self._evt_closed.wait()
        self._thread.join()
        return res

    def channel(self, channel_number=None):
        self._check_called_not_from_event_loop()

        channel_opened_future = self._register_pending_future()

        impl_channel = self._execute_task(
            self._impl.channel,
            on_open_callback=channel_opened_future.set_result,
            channel_number=channel_number
        )

        # Create our proxy channel
        channel = ThreadSafePikaChannel(impl_channel, self)

        # Link implementation channel with our proxy channel
        impl_channel._set_cookie(channel)

        channel_opened_future.result()
        return channel

    def add_timeout(self, timeout, callback):
        return self._execute_task(self._impl.add_timeout, timeout, callback)

    def remove_timeout(self, timeout_id):
        return self._execute_task(self._impl.remove_timeout, timeout_id)

    @property
    def is_closed(self):
        return self._impl.is_closed

    @property
    def is_closing(self):
        return self._impl.is_closing

    @property
    def is_open(self):
        return self._impl.is_open


class ThreadSafePikaChannel(object):  # pylint: disable=R0904,R0902

    def __init__(self, channel_impl, connection):
        self._impl = channel_impl
        self._connection = connection

        self._delivery_confirmation = False

        self._message_returned = False
        self._current_future = None

        self._evt_closed = threading.Event()

        self.add_on_close_callback(self._on_channel_close)

    def _execute_task(self, func, *args, **kwargs):
        return self._connection._execute_task(func, *args, **kwargs)

    def _on_channel_close(self, channel, reply_code, reply_text):
        self._evt_closed.set()

        if self._current_future:
            self._current_future.set_exception(
                pika_exceptions.ChannelClosed(reply_code, reply_text))

    def _on_message_confirmation(self, frame):
        self._current_future.set_result(frame)

    def add_on_close_callback(self, callback):
        self._execute_task(self._impl.add_on_close_callback, callback)

    def add_on_cancel_callback(self, callback):
        self._execute_task(self._impl.add_on_cancel_callback, callback)

    def __int__(self):
        return self.channel_number

    @property
    def channel_number(self):
        return self._impl.channel_number

    @property
    def is_closed(self):
        return self._impl.is_closed

    @property
    def is_closing(self):
        return self._impl.is_closing

    @property
    def is_open(self):
        return self._impl.is_open

    def close(self, reply_code=0, reply_text="Normal Shutdown"):
        self._impl.close(reply_code=reply_code, reply_text=reply_text)
        self._evt_closed.wait()

    def _check_called_not_from_event_loop(self):
        self._connection._check_called_not_from_event_loop()

    def flow(self, active):
        self._check_called_not_from_event_loop()

        self._current_future = futurist.Future()
        self._execute_task(
            self._impl.flow, callback=self._current_future.set_result,
            active=active
        )

        return self._current_future.result()

    def basic_consume(self,  # pylint: disable=R0913
                      consumer_callback,
                      queue,
                      no_ack=False,
                      exclusive=False,
                      consumer_tag=None,
                      arguments=None):

        self._check_called_not_from_event_loop()

        self._current_future = futurist.Future()
        self._execute_task(
            self._impl.add_callback, self._current_future.set_result,
            replies=[pika_spec.Basic.ConsumeOk], one_shot=True
        )

        self._impl.add_callback(self._current_future.set_result,
                                replies=[pika_spec.Basic.ConsumeOk],
                                one_shot=True)
        tag = self._execute_task(
            self._impl.basic_consume,
            consumer_callback=consumer_callback,
            queue=queue,
            no_ack=no_ack,
            exclusive=exclusive,
            consumer_tag=consumer_tag,
            arguments=arguments
        )

        self._current_future.result()
        return tag

    def basic_cancel(self, consumer_tag):
        self._check_called_not_from_event_loop()

        self._current_future = futurist.Future()
        self._execute_task(
            self._impl.basic_cancel,
            callback=self._current_future.set_result,
            consumer_tag=consumer_tag,
            nowait=False)
        self._current_future.result()

    def basic_ack(self, delivery_tag=0, multiple=False):
        return self._execute_task(
            self._impl.basic_ack, delivery_tag=delivery_tag, multiple=multiple)

    def basic_nack(self, delivery_tag=None, multiple=False, requeue=True):
        return self._execute_task(
            self._impl.basic_nack, delivery_tag=delivery_tag,
            multiple=multiple, requeue=requeue
        )

    def publish(self, exchange, routing_key, body,  # pylint: disable=R0913
                properties=None, mandatory=False, immediate=False):

        if self._delivery_confirmation:
            self._check_called_not_from_event_loop()

            # In publisher-acknowledgments mode
            self._message_returned = False
            self._current_future = futurist.Future()

            self._execute_task(self._impl.basic_publish,
                               exchange=exchange,
                               routing_key=routing_key,
                               body=body,
                               properties=properties,
                               mandatory=mandatory,
                               immediate=immediate)

            conf_method = self._current_future.result().method

            if isinstance(conf_method, pika_spec.Basic.Nack):
                raise pika_exceptions.NackError((None,))
            else:
                assert isinstance(conf_method, pika_spec.Basic.Ack), (
                    conf_method)

                if self._message_returned:
                    raise pika_exceptions.UnroutableError((None,))
        else:
            # In non-publisher-acknowledgments mode
            self._execute_task(self._impl.basic_publish,
                               exchange=exchange,
                               routing_key=routing_key,
                               body=body,
                               properties=properties,
                               mandatory=mandatory,
                               immediate=immediate)

    def basic_qos(self, prefetch_size=0, prefetch_count=0, all_channels=False):
        self._check_called_not_from_event_loop()

        self._current_future = futurist.Future()
        self._execute_task(self._impl.basic_qos,
                           callback=self._current_future.set_result,
                           prefetch_size=prefetch_size,
                           prefetch_count=prefetch_count,
                           all_channels=all_channels)
        self._current_future.result()

    def basic_recover(self, requeue=False):
        self._check_called_not_from_event_loop()

        self._current_future = futurist.Future()
        self._execute_task(
            self._impl.basic_recover,
            callback=lambda: self._current_future.set_result(None),
            requeue=requeue
        )
        self._current_future.result()

    def basic_reject(self, delivery_tag=None, requeue=True):
        self._execute_task(self._impl.basic_reject,
                           delivery_tag=delivery_tag,
                           requeue=requeue)

    def _on_message_returned(self, *args, **kwargs):
        self._message_returned = True

    def confirm_delivery(self):
        self._check_called_not_from_event_loop()

        self._current_future = futurist.Future()
        self._execute_task(self._impl.add_callback,
                           callback=self._current_future.set_result,
                           replies=[pika_spec.Confirm.SelectOk],
                           one_shot=True)
        self._execute_task(self._impl.confirm_delivery,
                           callback=self._on_message_confirmation,
                           nowait=False)
        self._current_future.result()

        self._delivery_confirmation = True
        self._execute_task(self._impl.add_on_return_callback,
                           self._on_message_returned)

    def exchange_declare(self, exchange=None,  # pylint: disable=R0913
                         exchange_type='direct', passive=False, durable=False,
                         auto_delete=False, internal=False,
                         arguments=None, **kwargs):
        self._check_called_not_from_event_loop()

        self._current_future = futurist.Future()
        self._execute_task(self._impl.exchange_declare,
                           callback=self._current_future.set_result,
                           exchange=exchange,
                           exchange_type=exchange_type,
                           passive=passive,
                           durable=durable,
                           auto_delete=auto_delete,
                           internal=internal,
                           nowait=False,
                           arguments=arguments,
                           type=kwargs["type"] if kwargs else None)

        return self._current_future.result()

    def exchange_delete(self, exchange=None, if_unused=False):
        self._check_called_not_from_event_loop()

        self._current_future = futurist.Future()
        self._execute_task(self._impl.exchange_delete,
                           callback=self._current_future.set_result,
                           exchange=exchange,
                           if_unused=if_unused,
                           nowait=False)

        return self._current_future.result()

    def exchange_bind(self, destination=None, source=None, routing_key='',
                      arguments=None):
        self._check_called_not_from_event_loop()

        self._current_future = futurist.Future()
        self._execute_task(self._impl.exchange_bind,
                           callback=self._current_future.set_result,
                           destination=destination,
                           source=source,
                           routing_key=routing_key,
                           nowait=False,
                           arguments=arguments)

        return self._current_future.result()

    def exchange_unbind(self, destination=None, source=None, routing_key='',
                        arguments=None):
        self._check_called_not_from_event_loop()

        self._current_future = futurist.Future()
        self._execute_task(self._impl.exchange_unbind,
                           callback=self._current_future.set_result,
                           destination=destination,
                           source=source,
                           routing_key=routing_key,
                           nowait=False,
                           arguments=arguments)

        return self._current_future.result()

    def queue_declare(self, queue='', passive=False, durable=False,
                      exclusive=False, auto_delete=False,
                      arguments=None):
        self._check_called_not_from_event_loop()

        self._current_future = futurist.Future()
        self._execute_task(self._impl.queue_declare,
                           callback=self._current_future.set_result,
                           queue=queue,
                           passive=passive,
                           durable=durable,
                           exclusive=exclusive,
                           auto_delete=auto_delete,
                           nowait=False,
                           arguments=arguments)

        return self._current_future.result()

    def queue_delete(self, queue='', if_unused=False, if_empty=False):
        self._check_called_not_from_event_loop()

        self._current_future = futurist.Future()
        self._execute_task(self._impl.queue_delete,
                           callback=self._current_future.set_result,
                           queue=queue,
                           if_unused=if_unused,
                           if_empty=if_empty,
                           nowait=False)

        return self._current_future.result()

    def queue_purge(self, queue=''):
        self._check_called_not_from_event_loop()

        self._current_future = futurist.Future()
        self._execute_task(self._impl.queue_purge,
                           callback=self._current_future.set_result,
                           queue=queue,
                           nowait=False)
        return self._current_future.result()

    def queue_bind(self, queue, exchange, routing_key=None,
                   arguments=None):
        self._check_called_not_from_event_loop()

        self._current_future = futurist.Future()
        self._execute_task(self._impl.queue_bind,
                           callback=self._current_future.set_result,
                           queue=queue,
                           exchange=exchange,
                           routing_key=routing_key,
                           nowait=False,
                           arguments=arguments)
        return self._current_future.result()

    def queue_unbind(self, queue='', exchange=None, routing_key=None,
                     arguments=None):
        self._check_called_not_from_event_loop()

        self._current_future = futurist.Future()
        self._execute_task(self._impl.queue_unbind,
                           callback=self._current_future.set_result,
                           queue=queue,
                           exchange=exchange,
                           routing_key=routing_key,
                           arguments=arguments)
        return self._current_future.result()
