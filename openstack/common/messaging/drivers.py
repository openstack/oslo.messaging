
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

NAMESPACE = 'openstack.common.messaging.drivers'


def _driver(module, name):
    return 'openstack.common.messaging._drivers.impl_%s:%s' % (module, name)

_RABBIT_DRIVER = _driver('rabbit', 'RabbitDriver')
_QPID_DRIVER = _driver('qpid', 'QpidDriver')
_ZMQ_DRIVER = _driver('zmq', 'ZmqDriver')
_FAKE_DRIVER = _driver('fake', 'FakeDriver')


TRANSPORT_DRIVERS = [
    'rabbit = ' + _RABBIT_DRIVER,
    'qpid = ' + _QPID_DRIVER,
    'zmq = ' + _ZMQ_DRIVER,

    # To avoid confusion
    'kombu = ' + _RABBIT_DRIVER,

    # For backwards compat
    'openstack.common.rpc.impl_kombu = ' + _RABBIT_DRIVER,
    'openstack.common.rpc.impl_qpid = ' + _QPID_DRIVER,
    'openstack.common.rpc.impl_zmq = ' + _ZMQ_DRIVER,

    # This is just for internal testing
    'fake = ' + _FAKE_DRIVER,
]
