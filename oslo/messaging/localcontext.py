
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
]

import threading
import uuid

_KEY = '_%s_%s' % (__name__.replace('.', '_'), uuid.uuid4().hex)
_STORE = threading.local()


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


def set_local_context(ctxt):
    """Set the request context for the current thread.

    :param ctxt: a deserialized request context
    :type ctxt: dict
    """
    setattr(_STORE, _KEY, ctxt)


def clear_local_context():
    """Clear the request context for the current thread."""
    delattr(_STORE, _KEY)
