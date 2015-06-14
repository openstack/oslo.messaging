
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

__all__ = [
    'get_local_context',
    'set_local_context',
    'clear_local_context',
    '_set_local_context',
    '_clear_local_context',
]

from functools import wraps
import threading
import uuid
import warnings


def _deprecated(as_of=None, in_favor_of=None):
    def __inner(func):
        @wraps(func)
        def __wrapper(*args, **kwargs):
            replaced_by = (' Please use %s instead.'
                           % in_favor_of) if in_favor_of else ''
            warnings.warn('%s has been deprecated in %s and will be '
                          'removed in the next release.%s' %
                          (func.__name__, as_of, replaced_by))
            return func(*args, **kwargs)
        return __wrapper
    return __inner
_VERSION = '1.8.0'

_KEY = '_%s_%s' % (__name__.replace('.', '_'), uuid.uuid4().hex)
_STORE = threading.local()


@_deprecated(as_of=_VERSION, in_favor_of='oslo_context.context.get_current')
def get_local_context(ctxt):
    """Retrieve the RPC endpoint request context for the current thread.

    This method allows any code running in the context of a dispatched RPC
    endpoint method to retrieve the context for this request.

    This is commonly used for logging so that, for example, you can include the
    request ID, user and tenant in every message logged from a RPC endpoint
    method.

    :returns: the context for the request dispatched in the current thread
    """
    return getattr(_STORE, _KEY, None)


@_deprecated(as_of=_VERSION)
def set_local_context(ctxt):
    _set_local_context(ctxt)


def _set_local_context(ctxt):
    """Set the request context for the current thread.

    :param ctxt: a deserialized request context
    :type ctxt: dict
    """
    setattr(_STORE, _KEY, ctxt)


@_deprecated(as_of=_VERSION)
def clear_local_context():
    _clear_local_context()


def _clear_local_context():
    """Clear the request context for the current thread."""
    delattr(_STORE, _KEY)
