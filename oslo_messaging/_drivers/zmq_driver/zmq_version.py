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

import re

from oslo_messaging._drivers import common as rpc_common
from oslo_messaging._i18n import _


# current driver's version for representing internal message format
MESSAGE_VERSION = '1.0'


class UnsupportedMessageVersionError(rpc_common.RPCException):
    msg_fmt = _("Message version %(version)s is not supported.")

    def __init__(self, version):
        super(UnsupportedMessageVersionError, self).__init__(version=version)


def get_method_versions(obj, method_name):
    """Useful function for initializing versioned senders/receivers.

    Returns a dictionary of different internal versions of the given method.

    Assumes that the object has the particular versioned method and this method
    is public. Thus versions are private implementations of the method.

    For example, for a method 'func' methods '_func_v_1_0', '_func_v_1_5',
    '_func_v_2_0', etc. are assumed as its respective 1.0, 1.5, 2.0 versions.
    """

    assert callable(getattr(obj, method_name, None)), \
        "Object must have specified method!"
    assert not method_name.startswith('_'), "Method must be public!"

    method_versions = {}
    for attr_name in dir(obj):
        if attr_name == method_name:
            continue
        attr = getattr(obj, attr_name, None)
        if not callable(attr):
            continue
        match_obj = re.match(r'^_%s_v_(\d)_(\d)$' % method_name, attr_name)
        if match_obj is not None:
            version = '.'.join([match_obj.group(1), match_obj.group(2)])
            method_versions[version] = attr

    return method_versions
