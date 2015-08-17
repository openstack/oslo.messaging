# Copyright 2010 United States Government as represented by the
# Administrator of the National Aeronautics and Space Administration.
# All Rights Reserved.
# Copyright 2011 - 2012, Red Hat, Inc.
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
Shared code between AMQP based openstack.common.rpc implementations.

The code in this module is shared between the rpc implementations based on
AMQP. Specifically, this includes impl_kombu and impl_qpid.  impl_carrot also
uses AMQP, but is deprecated and predates this code.
"""

import collections
import logging
import uuid

from oslo_config import cfg
import six

from oslo_messaging._drivers import common as rpc_common
from oslo_messaging._drivers import pool

deprecated_durable_opts = [
    cfg.DeprecatedOpt('amqp_durable_queues',
                      group='DEFAULT'),
    cfg.DeprecatedOpt('rabbit_durable_queues',
                      group='DEFAULT')
]

amqp_opts = [
    cfg.BoolOpt('amqp_durable_queues',
                default=False,
                deprecated_opts=deprecated_durable_opts,
                help='Use durable queues in AMQP.'),
    cfg.BoolOpt('amqp_auto_delete',
                default=False,
                deprecated_group='DEFAULT',
                help='Auto-delete queues in AMQP.'),
    cfg.BoolOpt('send_single_reply',
                default=False,
                help='Send a single AMQP reply to call message. The current '
                     'behaviour since oslo-incubator is to send two AMQP '
                     'replies - first one with the payload, a second one to '
                     'ensure the other have finish to send the payload. We '
                     'are going to remove it in the N release, but we must '
                     'keep backward compatible at the same time. This option '
                     'provides such compatibility - it defaults to False in '
                     'Liberty and can be turned on for early adopters with a '
                     'new installations or for testing. Please note, that '
                     'this option will be removed in the Mitaka release.')
]

UNIQUE_ID = '_unique_id'
LOG = logging.getLogger(__name__)

# NOTE(sileht): Even if rabbit/qpid have only one Connection class,
# this connection can be used for two purposes:
# * wait and receive amqp messages (only do read stuffs on the socket)
# * send messages to the broker (only do write stuffs on the socket)
# The code inside a connection class is not concurrency safe.
# Using one Connection class instance for doing both, will result
# of eventlet complaining of multiple greenthreads that read/write the
# same fd concurrently... because 'send' and 'listen' run in different
# greenthread.
# So, a connection cannot be shared between thread/greenthread and
# this two variables permit to define the purpose of the connection
# to allow drivers to add special handling if needed (like heatbeat).
# amqp drivers create 3 kind of connections:
# * driver.listen*(): each call create a new 'PURPOSE_LISTEN' connection
# * driver.send*(): a pool of 'PURPOSE_SEND' connections is used
# * driver internally have another 'PURPOSE_LISTEN' connection dedicated
#   to wait replies of rpc call
PURPOSE_LISTEN = 'listen'
PURPOSE_SEND = 'send'


class ConnectionPool(pool.Pool):
    """Class that implements a Pool of Connections."""
    def __init__(self, conf, rpc_conn_pool_size, url, connection_cls):
        self.connection_cls = connection_cls
        self.conf = conf
        self.url = url
        super(ConnectionPool, self).__init__(rpc_conn_pool_size)
        self.reply_proxy = None

    # TODO(comstud): Timeout connections not used in a while
    def create(self, purpose=None):
        if purpose is None:
            purpose = PURPOSE_SEND
        LOG.debug('Pool creating new connection')
        return self.connection_cls(self.conf, self.url, purpose)

    def empty(self):
        for item in self.iter_free():
            item.close()


class ConnectionContext(rpc_common.Connection):
    """The class that is actually returned to the create_connection() caller.

    This is essentially a wrapper around Connection that supports 'with'.
    It can also return a new Connection, or one from a pool.

    The function will also catch when an instance of this class is to be
    deleted.  With that we can return Connections to the pool on exceptions
    and so forth without making the caller be responsible for catching them.
    If possible the function makes sure to return a connection to the pool.
    """

    def __init__(self, connection_pool, purpose):
        """Create a new connection, or get one from the pool."""
        self.connection = None
        self.connection_pool = connection_pool
        pooled = purpose == PURPOSE_SEND
        if pooled:
            self.connection = connection_pool.get()
        else:
            # a non-pooled connection is requested, so create a new connection
            self.connection = connection_pool.create(purpose)
        self.pooled = pooled
        self.connection.pooled = pooled

    def __enter__(self):
        """When with ConnectionContext() is used, return self."""
        return self

    def _done(self):
        """If the connection came from a pool, clean it up and put it back.
        If it did not come from a pool, close it.
        """
        if self.connection:
            if self.pooled:
                # Reset the connection so it's ready for the next caller
                # to grab from the pool
                try:
                    self.connection.reset()
                except Exception:
                    LOG.exception("Fail to reset the connection, drop it")
                    try:
                        self.connection.close()
                    except Exception:
                        pass
                    self.connection = self.connection_pool.create()
                finally:
                    self.connection_pool.put(self.connection)
            else:
                try:
                    self.connection.close()
                except Exception:
                    pass
            self.connection = None

    def __exit__(self, exc_type, exc_value, tb):
        """End of 'with' statement.  We're done here."""
        self._done()

    def __del__(self):
        """Caller is done with this connection.  Make sure we cleaned up."""
        self._done()

    def close(self):
        """Caller is done with this connection."""
        self._done()

    def __getattr__(self, key):
        """Proxy all other calls to the Connection instance."""
        if self.connection:
            return getattr(self.connection, key)
        else:
            raise rpc_common.InvalidRPCConnectionReuse()


class RpcContext(rpc_common.CommonRpcContext):
    """Context that supports replying to a rpc.call."""
    def __init__(self, **kwargs):
        self.msg_id = kwargs.pop('msg_id', None)
        self.reply_q = kwargs.pop('reply_q', None)
        self.conf = kwargs.pop('conf')
        super(RpcContext, self).__init__(**kwargs)

    def deepcopy(self):
        values = self.to_dict()
        values['conf'] = self.conf
        values['msg_id'] = self.msg_id
        values['reply_q'] = self.reply_q
        return self.__class__(**values)


def unpack_context(conf, msg):
    """Unpack context from msg."""
    context_dict = {}
    for key in list(msg.keys()):
        key = six.text_type(key)
        if key.startswith('_context_'):
            value = msg.pop(key)
            context_dict[key[9:]] = value
    context_dict['msg_id'] = msg.pop('_msg_id', None)
    context_dict['reply_q'] = msg.pop('_reply_q', None)
    context_dict['conf'] = conf
    return RpcContext.from_dict(context_dict)


def pack_context(msg, context):
    """Pack context into msg.

    Values for message keys need to be less than 255 chars, so we pull
    context out into a bunch of separate keys. If we want to support
    more arguments in rabbit messages, we may want to do the same
    for args at some point.

    """
    if isinstance(context, dict):
        context_d = six.iteritems(context)
    else:
        context_d = six.iteritems(context.to_dict())

    msg.update(('_context_%s' % key, value)
               for (key, value) in context_d)


class _MsgIdCache(object):
    """This class checks any duplicate messages."""

    # NOTE: This value is considered can be a configuration item, but
    #       it is not necessary to change its value in most cases,
    #       so let this value as static for now.
    DUP_MSG_CHECK_SIZE = 16

    def __init__(self, **kwargs):
        self.prev_msgids = collections.deque([],
                                             maxlen=self.DUP_MSG_CHECK_SIZE)

    def check_duplicate_message(self, message_data):
        """AMQP consumers may read same message twice when exceptions occur
           before ack is returned. This method prevents doing it.
        """
        try:
            msg_id = message_data.pop(UNIQUE_ID)
        except KeyError:
            return
        if msg_id in self.prev_msgids:
            raise rpc_common.DuplicateMessageError(msg_id=msg_id)
        return msg_id

    def add(self, msg_id):
        if msg_id and msg_id not in self.prev_msgids:
            self.prev_msgids.append(msg_id)


def _add_unique_id(msg):
    """Add unique_id for checking duplicate messages."""
    unique_id = uuid.uuid4().hex
    msg.update({UNIQUE_ID: unique_id})


class AMQPDestinationNotFound(Exception):
    pass
