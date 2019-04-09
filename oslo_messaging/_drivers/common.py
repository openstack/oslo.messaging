# Copyright 2010 United States Government as represented by the
# Administrator of the National Aeronautics and Space Administration.
# All Rights Reserved.
# Copyright 2011 Red Hat, Inc.
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

# TODO(smcginnis) update this once six has support for collections.abc
# (https://github.com/benjaminp/six/pull/241) or clean up once we drop py2.7.
try:
    from collections.abc import Mapping
except ImportError:
    from collections import Mapping

import copy
import logging
import sys
import traceback

from oslo_serialization import jsonutils
from oslo_utils import timeutils
import six

import oslo_messaging
from oslo_messaging._i18n import _
from oslo_messaging._i18n import _LE, _LW
from oslo_messaging import _utils as utils

LOG = logging.getLogger(__name__)

_EXCEPTIONS_MODULE = 'exceptions' if six.PY2 else 'builtins'
_EXCEPTIONS_MODULES = ['exceptions', 'builtins']


'''RPC Envelope Version.

This version number applies to the top level structure of messages sent out.
It does *not* apply to the message payload, which must be versioned
independently.  For example, when using rpc APIs, a version number is applied
for changes to the API being exposed over rpc.  This version number is handled
in the rpc proxy and dispatcher modules.

This version number applies to the message envelope that is used in the
serialization done inside the rpc layer.  See serialize_msg() and
deserialize_msg().

The current message format (version 2.0) is very simple.  It is:

    {
        'oslo.version': <RPC Envelope Version as a String>,
        'oslo.message': <Application Message Payload, JSON encoded>
    }

Message format version '1.0' is just considered to be the messages we sent
without a message envelope.

So, the current message envelope just includes the envelope version.  It may
eventually contain additional information, such as a signature for the message
payload.

We will JSON encode the application message payload.  The message envelope,
which includes the JSON encoded application message body, will be passed down
to the messaging libraries as a dict.
'''
_RPC_ENVELOPE_VERSION = '2.0'

_VERSION_KEY = 'oslo.version'
_MESSAGE_KEY = 'oslo.message'

_REMOTE_POSTFIX = '_Remote'


class RPCException(Exception):
    msg_fmt = _("An unknown RPC related exception occurred.")

    def __init__(self, message=None, **kwargs):
        self.kwargs = kwargs

        if not message:
            try:
                message = self.msg_fmt % kwargs

            except Exception:
                # kwargs doesn't match a variable in the message
                # log the issue and the kwargs
                LOG.exception(_LE('Exception in string format operation, '
                                  'kwargs are:'))
                for name, value in kwargs.items():
                    LOG.error("%s: %s", name, value)
                # at least get the core message out if something happened
                message = self.msg_fmt

        super(RPCException, self).__init__(message)


class Timeout(RPCException):
    """Signifies that a timeout has occurred.

    This exception is raised if the rpc_response_timeout is reached while
    waiting for a response from the remote side.
    """
    msg_fmt = _('Timeout while waiting on RPC response - '
                'topic: "%(topic)s", RPC method: "%(method)s" '
                'info: "%(info)s"')

    def __init__(self, info=None, topic=None, method=None):
        """Initiates Timeout object.

        :param info: Extra info to convey to the user
        :param topic: The topic that the rpc call was sent to
        :param method: The name of the rpc method being
                                called
        """
        self.info = info
        self.topic = topic
        self.method = method
        super(Timeout, self).__init__(
            None,
            info=info or _('<unknown>'),
            topic=topic or _('<unknown>'),
            method=method or _('<unknown>'))


class DuplicateMessageError(RPCException):
    msg_fmt = _("Found duplicate message(%(msg_id)s). Skipping it.")


class InvalidRPCConnectionReuse(RPCException):
    msg_fmt = _("Invalid reuse of an RPC connection.")


class UnsupportedRpcVersion(RPCException):
    msg_fmt = _("Specified RPC version, %(version)s, not supported by "
                "this endpoint.")


class UnsupportedRpcEnvelopeVersion(RPCException):
    msg_fmt = _("Specified RPC envelope version, %(version)s, "
                "not supported by this endpoint.")


class RpcVersionCapError(RPCException):
    msg_fmt = _("Specified RPC version cap, %(version_cap)s, is too low")


class Connection(object):
    """A connection, returned by rpc.create_connection().

    This class represents a connection to the message bus used for rpc.
    An instance of this class should never be created by users of the rpc API.
    Use rpc.create_connection() instead.
    """
    def close(self):
        """Close the connection.

        This method must be called when the connection will no longer be used.
        It will ensure that any resources associated with the connection, such
        as a network connection, and cleaned up.
        """
        raise NotImplementedError()


def serialize_remote_exception(failure_info):
    """Prepares exception data to be sent over rpc.

    Failure_info should be a sys.exc_info() tuple.

    """
    tb = traceback.format_exception(*failure_info)

    failure = failure_info[1]

    kwargs = {}
    if hasattr(failure, 'kwargs'):
        kwargs = failure.kwargs

    # NOTE(matiu): With cells, it's possible to re-raise remote, remote
    # exceptions. Lets turn it back into the original exception type.
    cls_name = six.text_type(failure.__class__.__name__)
    mod_name = six.text_type(failure.__class__.__module__)
    if (cls_name.endswith(_REMOTE_POSTFIX) and
            mod_name.endswith(_REMOTE_POSTFIX)):
        cls_name = cls_name[:-len(_REMOTE_POSTFIX)]
        mod_name = mod_name[:-len(_REMOTE_POSTFIX)]

    data = {
        'class': cls_name,
        'module': mod_name,
        'message': six.text_type(failure),
        'tb': tb,
        'args': failure.args,
        'kwargs': kwargs
    }

    json_data = jsonutils.dumps(data)

    return json_data


def deserialize_remote_exception(data, allowed_remote_exmods):
    failure = jsonutils.loads(six.text_type(data))

    trace = failure.get('tb', [])
    message = failure.get('message', "") + "\n" + "\n".join(trace)
    name = failure.get('class')
    module = failure.get('module')

    # the remote service which raised the given exception might have a
    # different python version than the caller. For example, the caller might
    # run python 2.7, while the remote service might run python 3.4. Thus,
    # the exception module will be "builtins" instead of "exceptions".
    if module in _EXCEPTIONS_MODULES:
        module = _EXCEPTIONS_MODULE

    # NOTE(ameade): We DO NOT want to allow just any module to be imported, in
    # order to prevent arbitrary code execution.
    if module != _EXCEPTIONS_MODULE and module not in allowed_remote_exmods:
        return oslo_messaging.RemoteError(name, failure.get('message'), trace)

    try:
        __import__(module)
        mod = sys.modules[module]
        klass = getattr(mod, name)
        if not issubclass(klass, Exception):
            raise TypeError("Can only deserialize Exceptions")

        failure = klass(*failure.get('args', []), **failure.get('kwargs', {}))
    except (AttributeError, TypeError, ImportError) as error:
        LOG.warning(_LW("Failed to rebuild remote exception due to error: %s"),
                    six.text_type(error))
        return oslo_messaging.RemoteError(name, failure.get('message'), trace)

    ex_type = type(failure)
    str_override = lambda self: message
    new_ex_type = type(ex_type.__name__ + _REMOTE_POSTFIX, (ex_type,),
                       {'__str__': str_override, '__unicode__': str_override})
    new_ex_type.__module__ = '%s%s' % (module, _REMOTE_POSTFIX)
    try:
        # NOTE(ameade): Dynamically create a new exception type and swap it in
        # as the new type for the exception. This only works on user defined
        # Exceptions and not core Python exceptions. This is important because
        # we cannot necessarily change an exception message so we must override
        # the __str__ method.
        failure.__class__ = new_ex_type
    except TypeError:
        # NOTE(ameade): If a core exception then just add the traceback to the
        # first exception argument.
        failure.args = (message,) + failure.args[1:]
    return failure


class CommonRpcContext(object):
    def __init__(self, **kwargs):
        self.values = kwargs

    def __getattr__(self, key):
        try:
            return self.values[key]
        except KeyError:
            raise AttributeError(key)

    def to_dict(self):
        return copy.deepcopy(self.values)

    @classmethod
    def from_dict(cls, values):
        return cls(**values)

    def deepcopy(self):
        return self.from_dict(self.to_dict())

    def update_store(self):
        # local.store.context = self
        pass


class ClientException(Exception):
    """Encapsulates actual exception expected to be hit by a RPC proxy object.

    Merely instantiating it records the current exception information, which
    will be passed back to the RPC client without exceptional logging.
    """
    def __init__(self):
        self._exc_info = sys.exc_info()


def serialize_msg(raw_msg):
    # NOTE(russellb) See the docstring for _RPC_ENVELOPE_VERSION for more
    # information about this format.
    msg = {_VERSION_KEY: _RPC_ENVELOPE_VERSION,
           _MESSAGE_KEY: jsonutils.dumps(raw_msg)}

    return msg


def deserialize_msg(msg):
    # NOTE(russellb): Hang on to your hats, this road is about to
    # get a little bumpy.
    #
    # Robustness Principle:
    #    "Be strict in what you send, liberal in what you accept."
    #
    # At this point we have to do a bit of guessing about what it
    # is we just received.  Here is the set of possibilities:
    #
    # 1) We received a dict.  This could be 2 things:
    #
    #   a) Inspect it to see if it looks like a standard message envelope.
    #      If so, great!
    #
    #   b) If it doesn't look like a standard message envelope, it could either
    #      be a notification, or a message from before we added a message
    #      envelope (referred to as version 1.0).
    #      Just return the message as-is.
    #
    # 2) It's any other non-dict type.  Just return it and hope for the best.
    #    This case covers return values from rpc.call() from before message
    #    envelopes were used.  (messages to call a method were always a dict)

    if not isinstance(msg, dict):
        # See #2 above.
        return msg

    base_envelope_keys = (_VERSION_KEY, _MESSAGE_KEY)
    if not all(map(lambda key: key in msg, base_envelope_keys)):
        #  See #1.b above.
        return msg

    # At this point we think we have the message envelope
    # format we were expecting. (#1.a above)

    if not utils.version_is_compatible(_RPC_ENVELOPE_VERSION,
                                       msg[_VERSION_KEY]):
        raise UnsupportedRpcEnvelopeVersion(version=msg[_VERSION_KEY])

    raw_msg = jsonutils.loads(msg[_MESSAGE_KEY])

    return raw_msg


class DecayingTimer(object):
    def __init__(self, duration=None):
        self._watch = timeutils.StopWatch(duration=duration)

    def start(self):
        self._watch.start()

    def restart(self):
        self._watch.restart()

    def check_return(self, timeout_callback=None, *args, **kwargs):
        maximum = kwargs.pop('maximum', None)
        left = self._watch.leftover(return_none=True)
        if left is None:
            return maximum
        if left <= 0 and timeout_callback is not None:
            timeout_callback(*args, **kwargs)
        return left if maximum is None else min(left, maximum)


# NOTE(sileht): Even if rabbit has only one Connection class,
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


class ConnectionContext(Connection):
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

        # NOTE(sileht): Even if rabbit only has one Connection class this
        # connection can be used for only one of two purposes:
        #
        # * receiving messages from the broker (read actions on the socket)
        # * sending messages to the broker (write actions on the socket)
        #
        # Using one Connection class instance for both purposes will result in
        # eventlet complaining about multiple greenthreads that read/write the
        # same fd concurrently. This is because 'send' and 'listen' run in
        # different greenthreads and the code inside a connection class is not
        # concurrency safe. The 'purpose' parameter ensures that no connection
        # is used for both sending and receiving messages.
        #
        # The rabbitmq driver allocates connections in the following manner:
        # * driver.listen*(): each call creates a new dedicated
        #   'PURPOSE_LISTEN' connection for the listener
        # * driver.send*(): senders are assigned a connection from a pool of
        #   'PURPOSE_SEND' connections maintained by the driver.
        # * One 'PURPOSE_LISTEN' connection is dedicated to waiting for replies
        #   to rpc calls.

        pooled = purpose == PURPOSE_SEND
        if pooled:
            self.connection = connection_pool.get()
        else:
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
                    LOG.exception(_LE("Fail to reset the connection, drop it"))
                    try:
                        self.connection.close()
                    except Exception as exc:
                        LOG.debug("pooled conn close failure (ignored): %s",
                                  str(exc))
                    self.connection = self.connection_pool.create()
                finally:
                    self.connection_pool.put(self.connection)
            else:
                try:
                    self.connection.close()
                except Exception as exc:
                    LOG.debug("pooled conn close failure (ignored): %s",
                              str(exc))
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
            raise InvalidRPCConnectionReuse()


class ConfigOptsProxy(Mapping):
    """Proxy for oslo_config.cfg.ConfigOpts.

    Values from the query part of the transport url (if they are both present
    and valid) override corresponding values from the configuration.
    """

    def __init__(self, conf, url, group):
        self._conf = conf
        self._url = url
        self._group = group
        self._validate_query()

    def _validate_query(self):
        for name in self._url.query:
            self.GroupAttrProxy(self._conf, self._group,
                                self._conf[self._group],
                                self._url)[name]

    def __getattr__(self, name):
        value = getattr(self._conf, name)
        if isinstance(value, self._conf.GroupAttr) and name == self._group:
            return self.GroupAttrProxy(self._conf, name, value, self._url)
        return value

    def __getitem__(self, name):
        return self.__getattr__(name)

    def __contains__(self, name):
        return name in self._conf

    def __iter__(self):
        return iter(self._conf)

    def __len__(self):
        return len(self._conf)

    class GroupAttrProxy(Mapping):
        """Internal helper proxy for oslo_config.cfg.ConfigOpts.GroupAttr."""

        _VOID_MARKER = object()

        def __init__(self, conf, group_name, group, url):
            self._conf = conf
            self._group_name = group_name
            self._group = group
            self._url = url

        def __getattr__(self, opt_name):
            # Make sure that the group has this specific option
            opt_value_conf = getattr(self._group, opt_name)
            # If the option is also present in the url and has a valid
            # (i.e. convertible) value type, then try to override it
            opt_value_url = self._url.query.get(opt_name, self._VOID_MARKER)
            if opt_value_url is self._VOID_MARKER:
                return opt_value_conf
            opt_info = self._conf._get_opt_info(opt_name, self._group_name)
            return opt_info['opt'].type(opt_value_url)

        def __getitem__(self, opt_name):
            return self.__getattr__(opt_name)

        def __contains__(self, opt_name):
            return opt_name in self._group

        def __iter__(self):
            return iter(self._group)

        def __len__(self):
            return len(self._group)
