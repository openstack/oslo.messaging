# -*- coding: utf-8 -*-

#    Copyright (C) 2014 Yahoo! Inc. All Rights Reserved.
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

import futurist

from oslo_messaging._executors import impl_pooledexecutor


class ThreadExecutor(impl_pooledexecutor.PooledExecutor):
    """A message executor which integrates with threads.

    A message process that polls for messages from a dispatching thread and
    on reception of an incoming message places the message to be processed in
    a thread pool to be executed at a later time.
    """

    _executor_cls = futurist.ThreadPoolExecutor
