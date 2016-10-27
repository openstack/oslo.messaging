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

import logging
import logging.handlers
import multiprocessing
import os
import sys
import threading
import time
import uuid

from oslo_config import cfg

import oslo_messaging
from oslo_messaging._drivers.zmq_driver import zmq_async
from oslo_messaging.tests.functional import utils

LOG = logging.getLogger(__name__)

zmq = zmq_async.import_zmq()


class QueueHandler(logging.Handler):
    """This is a logging handler which sends events to a multiprocessing queue.

    The plan is to add it to Python 3.2, but this can be copy pasted into
    user code for use with earlier Python versions.
    """

    def __init__(self, queue):
        """Initialise an instance, using the passed queue."""
        logging.Handler.__init__(self)
        self.queue = queue

    def emit(self, record):
        """Emit a record.

        Writes the LogRecord to the queue.
        """
        try:
            ei = record.exc_info
            if ei:
                # just to get traceback text into record.exc_text
                dummy = self.format(record)  # noqa
                record.exc_info = None  # not needed any more
            self.queue.put_nowait(record)
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            self.handleError(record)


def listener_configurer(conf):
    root = logging.getLogger()
    h = logging.StreamHandler(sys.stdout)
    f = logging.Formatter('%(asctime)s %(processName)-10s %(name)s '
                          '%(levelname)-8s %(message)s')
    h.setFormatter(f)
    root.addHandler(h)
    log_path = conf.oslo_messaging_zmq.rpc_zmq_ipc_dir + \
        "/" + "zmq_multiproc.log"
    file_handler = logging.StreamHandler(open(log_path, 'w'))
    file_handler.setFormatter(f)
    root.addHandler(file_handler)


def server_configurer(queue):
    h = QueueHandler(queue)
    root = logging.getLogger()
    root.addHandler(h)
    root.setLevel(logging.DEBUG)


def listener_thread(queue, configurer, conf):
    configurer(conf)
    while True:
        time.sleep(0.3)
        try:
            record = queue.get()
            if record is None:
                break
            logger = logging.getLogger(record.name)
            logger.handle(record)
        except (KeyboardInterrupt, SystemExit):
            raise


class Client(oslo_messaging.RPCClient):

    def __init__(self, transport, topic):
        super(Client, self).__init__(
            transport=transport, target=oslo_messaging.Target(topic=topic))
        self.replies = []

    def call_a(self):
        LOG.warning("call_a - client side")
        rep = self.call({}, 'call_a')
        LOG.warning("after call_a - client side")
        self.replies.append(rep)
        return rep


class ReplyServerEndpoint(object):

    def call_a(self, *args, **kwargs):
        LOG.warning("call_a - Server endpoint reached!")
        return "OK"


class Server(object):

    def __init__(self, conf, log_queue, transport_url, name, topic=None):
        self.conf = conf
        self.log_queue = log_queue
        self.transport_url = transport_url
        self.name = name
        self.topic = topic or str(uuid.uuid4())
        self.ready = multiprocessing.Value('b', False)
        self._stop = multiprocessing.Event()

    def start(self):
        self.process = multiprocessing.Process(target=self._run_server,
                                               name=self.name,
                                               args=(self.conf,
                                                     self.transport_url,
                                                     self.log_queue,
                                                     self.ready))
        self.process.start()
        LOG.debug("Server process started: pid: %d", self.process.pid)

    def _run_server(self, conf, url, log_queue, ready):
        server_configurer(log_queue)
        LOG.debug("Starting RPC server")

        transport = oslo_messaging.get_transport(conf, url=url)
        target = oslo_messaging.Target(topic=self.topic, server=self.name)
        self.rpc_server = oslo_messaging.get_rpc_server(
            transport=transport, target=target,
            endpoints=[ReplyServerEndpoint()],
            executor='eventlet')
        self.rpc_server.start()
        ready.value = True
        LOG.debug("RPC server being started")
        while not self._stop.is_set():
            LOG.debug("Waiting for the stop signal ...")
            time.sleep(1)
        self.rpc_server.stop()
        self.rpc_server.wait()
        LOG.debug("Leaving process T:%s Pid:%d", str(target), os.getpid())

    def cleanup(self):
        LOG.debug("Stopping server")
        self.shutdown()

    def shutdown(self):
        self._stop.set()

    def restart(self, time_for_restart=1):
        pass

    def hang(self):
        pass

    def crash(self):
        pass

    def ping(self):
        pass


class MultiprocTestCase(utils.SkipIfNoTransportURL):

    def setUp(self):
        super(MultiprocTestCase, self).setUp(conf=cfg.ConfigOpts())

        if not self.url.startswith("zmq"):
            self.skipTest("ZeroMQ specific skipped...")

        self.transport = oslo_messaging.get_transport(self.conf, url=self.url)

        LOG.debug("Start log queue")

        self.log_queue = multiprocessing.Queue()
        self.log_listener = threading.Thread(target=listener_thread,
                                             args=(self.log_queue,
                                                   listener_configurer,
                                                   self.conf))
        self.log_listener.start()
        self.spawned = []

        self.conf.prog = "test_prog"
        self.conf.project = "test_project"

    def tearDown(self):
        for process in self.spawned:
            process.cleanup()
        super(MultiprocTestCase, self).tearDown()

    def get_client(self, topic):
        return Client(self.transport, topic)

    def spawn_server(self, wait_for_server=False, topic=None):
        name = "server_%d_%s" % (len(self.spawned), str(uuid.uuid4())[:8])
        server = Server(self.conf, self.log_queue, self.url, name, topic)
        LOG.debug("[SPAWN] %s (starting)...", server.name)
        server.start()
        if wait_for_server:
            while not server.ready.value:
                LOG.debug("[SPAWN] %s (waiting for server ready)...",
                          server.name)
                time.sleep(1)
        LOG.debug("[SPAWN] Server %s:%d started.",
                  server.name, server.process.pid)
        self.spawned.append(server)
        return server

    def spawn_servers(self, number, wait_for_server=False, common_topic=True):
        topic = str(uuid.uuid4()) if common_topic else None
        for _ in range(number):
            self.spawn_server(wait_for_server, topic)
