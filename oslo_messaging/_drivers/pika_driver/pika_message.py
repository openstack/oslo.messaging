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
from concurrent import futures

from oslo_log import log as logging

from oslo_messaging._drivers import common
from oslo_messaging import exceptions

from oslo_messaging._drivers.pika_driver import pika_exceptions as pika_drv_exc

from oslo_serialization import jsonutils

from pika import spec as pika_spec

import retrying
import six
import time
import uuid

LOG = logging.getLogger(__name__)


class PikaIncomingMessage(object):

    def __init__(self, pika_engine, channel, method, properties, body, no_ack):
        self._pika_engine = pika_engine
        self._no_ack = no_ack
        self._channel = channel
        self.delivery_tag = method.delivery_tag

        self.content_type = getattr(properties, "content_type",
                                    "application/json")
        self.content_encoding = getattr(properties, "content_encoding",
                                        "utf-8")

        self.expiration_time = (
            None if properties.expiration is None else
            time.time() + int(properties.expiration) / 1000
        )

        if self.content_type != "application/json":
            raise NotImplementedError("Content-type['{}'] is not valid, "
                                      "'application/json' only is supported.")

        message_dict = common.deserialize_msg(
            jsonutils.loads(body, encoding=self.content_encoding)
        )

        self.unique_id = message_dict.pop('_unique_id')
        self.msg_id = message_dict.pop('_msg_id', None)
        self.reply_q = message_dict.pop('_reply_q', None)

        context_dict = {}

        for key in list(message_dict.keys()):
            key = six.text_type(key)
            if key.startswith('_context_'):
                value = message_dict.pop(key)
                context_dict[key[9:]] = value

        self.message = message_dict
        self.ctxt = context_dict

    def reply(self, reply=None, failure=None, log_failure=True):
        if not (self.msg_id and self.reply_q):
            return

        if failure:
            failure = common.serialize_remote_exception(failure, log_failure)

        msg = {
            'result': reply,
            'failure': failure,
            '_unique_id': uuid.uuid4().hex,
            '_msg_id': self.msg_id,
            'ending': True
        }

        def on_exception(ex):
            if isinstance(ex, pika_drv_exc.ConnectionException):
                LOG.warn(str(ex))
                return True
            else:
                return False

        retrier = retrying.retry(
            stop_max_attempt_number=(
                None if self._pika_engine.rpc_reply_retry_attempts == -1
                else self._pika_engine.rpc_reply_retry_attempts
            ),
            retry_on_exception=on_exception,
            wait_fixed=self._pika_engine.rpc_reply_retry_delay * 1000,
        )

        try:
            self._pika_engine.publish(
                exchange=self._pika_engine.rpc_reply_exchange,
                routing_key=self.reply_q,
                body=jsonutils.dumps(
                    common.serialize_msg(msg),
                    encoding=self.content_encoding
                ),
                properties=pika_spec.BasicProperties(
                    content_encoding=self.content_encoding,
                    content_type=self.content_type,
                ),
                confirm=True,
                mandatory=False,
                expiration_time=self.expiration_time,
                retrier=retrier
            )
            LOG.debug(
                "Message [id:'{}'] replied to '{}'.".format(
                    self.msg_id, self.reply_q
                )
            )
        except Exception:
            LOG.exception(
                "Message [id:'{}'] wasn't replied to : {}".format(
                    self.msg_id, self.reply_q
                )
            )

    def acknowledge(self):
        if not self._no_ack:
            self._channel.basic_ack(delivery_tag=self.delivery_tag)

    def requeue(self):
        if not self._no_ack:
            return self._channel.basic_nack(delivery_tag=self.delivery_tag,
                                            requeue=True)


class PikaOutgoingMessage(object):

    def __init__(self, pika_engine, message, context,
                 content_type="application/json", content_encoding="utf-8"):
        self._pika_engine = pika_engine

        self.content_type = content_type
        self.content_encoding = content_encoding

        if self.content_type != "application/json":
            raise NotImplementedError("Content-type['{}'] is not valid, "
                                      "'application/json' only is supported.")

        self.message = message
        self.context = context

        self.unique_id = uuid.uuid4().hex
        self.msg_id = None

    def send(self, exchange, routing_key='', confirm=True,
             wait_for_reply=False, mandatory=True, persistent=False,
             timeout=None, retrier=None):
        msg = self.message.copy()

        msg['_unique_id'] = self.unique_id

        for key, value in self.context.iteritems():
            key = six.text_type(key)
            msg['_context_' + key] = value

        properties = pika_spec.BasicProperties(
            content_encoding=self.content_encoding,
            content_type=self.content_type,
            delivery_mode=2 if persistent else 1
        )

        expiration_time = (
            None if timeout is None else (timeout + time.time())
        )

        if wait_for_reply:
            self.msg_id = uuid.uuid4().hex
            msg['_msg_id'] = self.msg_id
            LOG.debug('MSG_ID is %s', self.msg_id)

            msg['_reply_q'] = self._pika_engine.get_reply_q(timeout)

            future = futures.Future()

            self._pika_engine.register_reply_waiter(
                msg_id=self.msg_id, future=future,
                expiration_time=expiration_time
            )

        self._pika_engine.publish(
            exchange=exchange, routing_key=routing_key,
            body=jsonutils.dumps(
                common.serialize_msg(msg),
                encoding=self.content_encoding
            ),
            properties=properties,
            confirm=confirm,
            mandatory=mandatory,
            expiration_time=expiration_time,
            retrier=retrier
        )

        if wait_for_reply:
            try:
                return future.result(timeout)
            except futures.TimeoutError:
                raise exceptions.MessagingTimeout()
