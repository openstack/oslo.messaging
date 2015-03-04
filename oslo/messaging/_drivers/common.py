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

import copy
import logging
import sys
import time
import traceback

import six

from oslo import messaging
from oslo.messaging import _utils as utils
from oslo.messaging.openstack.common.gettextutils import _
from oslo.messaging.openstack.common import jsonutils

LOG = logging.getLogger(__name__)

_EXCEPTIONS_MODULE = 'exceptions' if six.PY2 else 'builtins'


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
                LOG.exception(_('Exception in string format operation'))
                for name, value in six.iteritems(kwargs):
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
        :param rpc_method_name: The name of the rpc method being
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


def _safe_log(log_func, msg, msg_data):
    """Sanitizes the msg_data field before logging."""
    SANITIZE = ['_context_auth_token', 'auth_token', 'new_pass']

    def _fix_passwords(d):
        """Sanitizes the password fields in the dictionary."""
        for k in six.iterkeys(d):
            if k.lower().find('password') != -1:
                d[k] = '<SANITIZED>'
            elif k.lower() in SANITIZE:
                d[k] = '<SANITIZED>'
            elif isinstance(d[k], dict):
                _fix_passwords(d[k])
        return d

    return log_func(msg, _fix_passwords(copy.deepcopy(msg_data)))


def serialize_remote_exception(failure_info, log_failure=True):
    """Prepares exception data to be sent over rpc.

    Failure_info should be a sys.exc_info() tuple.

    """
    tb = traceback.format_exception(*failure_info)
    failure = failure_info[1]
    if log_failure:
        LOG.error(_("Returning exception %s to caller"),
                  six.text_type(failure))
        LOG.error(tb)

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

    # NOTE(ameade): We DO NOT want to allow just any module to be imported, in
    # order to prevent arbitrary code execution.
    if module != _EXCEPTIONS_MODULE and module not in allowed_remote_exmods:
        return messaging.RemoteError(name, failure.get('message'), trace)

    try:
        __import__(module)
        mod = sys.modules[module]
        klass = getattr(mod, name)
        if not issubclass(klass, Exception):
            raise TypeError("Can only deserialize Exceptions")

        failure = klass(*failure.get('args', []), **failure.get('kwargs', {}))
    except (AttributeError, TypeError, ImportError):
        return messaging.RemoteError(name, failure.get('message'), trace)

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
        self._duration = duration
        self._ends_at = None

    def start(self):
        if self._duration is not None:
            self._ends_at = time.time() + max(0, self._duration)

    def check_return(self, timeout_callback, *args, **kwargs):
        maximum = kwargs.pop('maximum', None)
        if self._duration is None:
            return None if maximum is None else maximum
        if self._ends_at is None:
            raise RuntimeError(_("Can not check/return a timeout from a timer"
                               " that has not been started."))

        left = self._ends_at - time.time()
        if left <= 0:
            timeout_callback(*args, **kwargs)

        return left if maximum is None else min(left, maximum)
