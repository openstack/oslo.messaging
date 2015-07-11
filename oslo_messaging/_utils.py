
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

LOG = logging.getLogger(__name__)


def version_is_compatible(imp_version, version):
    """Determine whether versions are compatible.

    :param imp_version: The version implemented
    :param version: The version requested by an incoming message.
    """
    version_parts = version.split('.')
    imp_version_parts = imp_version.split('.')
    try:
        rev = version_parts[2]
    except IndexError:
        rev = 0
    try:
        imp_rev = imp_version_parts[2]
    except IndexError:
        imp_rev = 0

    if int(version_parts[0]) != int(imp_version_parts[0]):  # Major
        return False
    if int(version_parts[1]) > int(imp_version_parts[1]):  # Minor
        return False
    if (int(version_parts[1]) == int(imp_version_parts[1]) and
            int(rev) > int(imp_rev)):  # Revision
        return False
    return True


class DispatcherExecutorContext(object):
    """Dispatcher executor context helper

    A dispatcher can have work to do before and after the dispatch of the
    request in the main server thread while the dispatcher itself can be
    done in its own thread.

    The executor can use the helper like this:

        callback = dispatcher(incoming)
        callback.prepare()
        thread = MyWhateverThread()
        thread.on_done(callback.done)
        thread.run(callback.run)

    """
    def __init__(self, incoming, dispatch, executor_callback=None,
                 post=None):
        self._result = None
        self._incoming = incoming
        self._dispatch = dispatch
        self._post = post
        self._executor_callback = executor_callback

    def run(self):
        """The incoming message dispath itself

        Can be run in an other thread/greenlet/corotine if the executor is
        able to do it.
        """
        try:
            self._result = self._dispatch(self._incoming,
                                          self._executor_callback)
        except Exception:
            msg = 'The dispatcher method must catches all exceptions'
            LOG.exception(msg)
            raise RuntimeError(msg)

    def done(self):
        """Callback after the incoming message have been dispathed

        Should be runned in the main executor thread/greenlet/corotine
        """
        # FIXME(sileht): this is not currently true, this works only because
        # the driver connection used for polling write on the wire only to
        # ack/requeue message, but what if one day, the driver do something
        # else
        if self._post is not None:
            self._post(self._incoming, self._result)
