
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

import threading
import uuid

import testscenarios

from oslo_messaging._drivers import pool
from oslo_messaging.tests import utils as test_utils

load_tests = testscenarios.load_tests_apply_scenarios


class PoolTestCase(test_utils.BaseTestCase):

    _max_size = [
        ('default_size', dict(max_size=None, n_iters=4)),
        ('set_max_size', dict(max_size=10, n_iters=10)),
    ]

    _create_error = [
        ('no_create_error', dict(create_error=False)),
        ('create_error', dict(create_error=True)),
    ]

    @classmethod
    def generate_scenarios(cls):
        cls.scenarios = testscenarios.multiply_scenarios(cls._max_size,
                                                         cls._create_error)

    class TestPool(pool.Pool):

        def create(self):
            return uuid.uuid4()

    class ThreadWaitWaiter(object):

        """A gross hack.

        Stub out the condition variable's wait() method and spin until it
        has been called by each thread.
        """

        def __init__(self, cond, n_threads, stubs):
            self.cond = cond
            self.stubs = stubs
            self.n_threads = n_threads
            self.n_waits = 0
            self.orig_wait = cond.wait

            def count_waits(**kwargs):
                self.n_waits += 1
                self.orig_wait(**kwargs)
            self.stubs.Set(self.cond, 'wait', count_waits)

        def wait(self):
            while self.n_waits < self.n_threads:
                pass
            self.stubs.Set(self.cond, 'wait', self.orig_wait)

    def test_pool(self):
        kwargs = {}
        if self.max_size is not None:
            kwargs['max_size'] = self.max_size

        p = self.TestPool(**kwargs)

        if self.create_error:
            def create_error():
                raise RuntimeError
            orig_create = p.create
            self.stubs.Set(p, 'create', create_error)
            self.assertRaises(RuntimeError, p.get)
            self.stubs.Set(p, 'create', orig_create)

        objs = []
        for i in range(self.n_iters):
            objs.append(p.get())
            self.assertIsInstance(objs[i], uuid.UUID)

        def wait_for_obj():
            o = p.get()
            self.assertIn(o, objs)

        waiter = self.ThreadWaitWaiter(p._cond, self.n_iters, self.stubs)

        threads = []
        for i in range(self.n_iters):
            t = threading.Thread(target=wait_for_obj)
            t.start()
            threads.append(t)

        waiter.wait()

        for o in objs:
            p.put(o)

        for t in threads:
            t.join()

        for o in objs:
            p.put(o)

        for o in p.iter_free():
            self.assertIn(o, objs)
            objs.remove(o)

        self.assertEqual([], objs)


PoolTestCase.generate_scenarios()
