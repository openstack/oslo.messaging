#!/usr/bin/env python

#    Copyright 2011 OpenStack Foundation
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

import eventlet
eventlet.monkey_patch()

import contextlib
import logging
import sys

from oslo.config import cfg
from oslo.messaging._drivers import impl_zmq
from oslo.messaging._executors import impl_eventlet  # FIXME(markmc)

CONF = cfg.CONF
CONF.register_opts(impl_zmq.zmq_opts)
CONF.register_opts(impl_eventlet._eventlet_opts)


def main():
    CONF(sys.argv[1:], project='oslo')
    logging.basicConfig(level=logging.DEBUG)

    with contextlib.closing(impl_zmq.ZmqProxy(CONF)) as reactor:
        reactor.consume_in_thread()
        reactor.wait()
