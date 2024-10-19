# Copyright 2020 LINE Corp.
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

import queue
import socket
import threading
import time

from oslo_config import cfg
from oslo_log import log as logging
from oslo_metrics import message_type
from oslo_utils import eventletutils
from oslo_utils import importutils


LOG = logging.getLogger(__name__)

eventlet = importutils.try_import('eventlet')
if eventlet and eventletutils.is_monkey_patched("thread"):
    # Here we initialize module with the native python threading module
    # if it was already monkey patched by eventlet/greenlet.
    stdlib_threading = eventlet.patcher.original('threading')
else:
    # Manage the case where we run this driver in a non patched environment
    # and where user even so configure the driver to run heartbeat through
    # a python thread, if we don't do that when the heartbeat will start
    # we will facing an issue by trying to override the threading module.
    stdlib_threading = threading


oslo_messaging_metrics = [
    cfg.BoolOpt('metrics_enabled', default=False,
                help='Boolean to send rpc metrics to oslo.metrics.'),
    cfg.IntOpt('metrics_buffer_size', default=1000,
               help='Buffer size to store in oslo.messaging.'),
    cfg.StrOpt('metrics_socket_file',
               default='/var/tmp/metrics_collector.sock',  # nosec
               help='Unix domain socket file to be used'
                    ' to send rpc related metrics'),
    cfg.StrOpt('metrics_process_name',
               default='',
               help='Process name which is used to identify which process'
                    ' produce metrics'),
    cfg.IntOpt('metrics_thread_stop_timeout',
               default=10,
               help='Sending thread stop once metrics_thread_stop_timeout'
                    ' seconds after the last successful metrics send.'
                    ' So that this thread will not be the blocker'
                    ' when process is shutting down.'
                    ' If the process is still running, sending thread will'
                    ' be restarted at the next metrics queueing time')
]
cfg.CONF.register_opts(oslo_messaging_metrics, group='oslo_messaging_metrics')


class MetricsCollectorClient:

    def __init__(self, conf, metrics_type, **kwargs):
        self.conf = conf.oslo_messaging_metrics
        self.unix_socket = self.conf.metrics_socket_file
        buffer_size = self.conf.metrics_buffer_size
        self.tx_queue = queue.Queue(buffer_size)
        self.next_send_metric = None
        self.metrics_type = metrics_type
        self.args = kwargs
        self.send_thread = threading.Thread(target=self.send_loop)
        self.send_thread.start()

    def __enter__(self):
        if not self.conf.metrics_enabled:
            return None
        self.start_time = time.time()
        send_method = getattr(self, self.metrics_type +
                              "_invocation_start_total")
        send_method(**self.args)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.conf.metrics_enabled:
            duration = time.time() - self.start_time
            send_method = getattr(
                self, self.metrics_type + "_processing_seconds")
            send_method(duration=duration, **self.args)
            send_method = getattr(
                self, self.metrics_type + "_invocation_end_total")
            send_method(**self.args)

    def put_into_txqueue(self, metrics_name, action, **labels):

        labels['process'] = \
            self.conf.metrics_process_name
        m = message_type.Metric("oslo_messaging", metrics_name, action,
                                **labels)

        try:
            self.tx_queue.put_nowait(m)
        except queue.Full:
            LOG.warning("tx queues is already full(%s/%s). Fails to "
                        "send the metrics(%s)" %
                        (self.tx_queue.qsize(), self.tx_queue.maxsize, m))

        if not self.send_thread.is_alive():
            self.send_thread = threading.Thread(target=self.send_loop)
            self.send_thread.start()

    def send_loop(self):
        timeout = self.conf.metrics_thread_stop_timeout
        stoptime = time.time() + timeout
        while stoptime > time.time():
            if self.next_send_metric is None:
                try:
                    self.next_send_metric = self.tx_queue.get(timeout=timeout)
                except queue.Empty:
                    continue
            try:
                self.send_metric(self.next_send_metric)
                self.next_send_metric = None
                stoptime = time.time() + timeout
            except Exception as e:
                LOG.error("Failed to send metrics: %s. "
                          "Wait 1 seconds for next try." % e)
                time.sleep(1)

    def send_metric(self, metric):
        s = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        s.connect(self.unix_socket)
        s.send(metric.to_json().encode())
        s.close()

    def put_rpc_client_metrics_to_txqueue(self, metric_name, action,
                                          target, method, call_type, timeout,
                                          exception=None):
        kwargs = {
            'call_type': call_type,
            'exchange': target.exchange,
            'topic': target.topic,
            'namespace': target.namespace,
            'version': target.version,
            'server': target.server,
            'fanout': target.fanout,
            'method': method,
            'timeout': timeout,
        }
        if exception:
            kwargs['exception'] = exception

        self.put_into_txqueue(metric_name, action, **kwargs)

    def rpc_client_invocation_start_total(self, target, method, call_type,
                                          timeout=None):
        self.put_rpc_client_metrics_to_txqueue(
            "rpc_client_invocation_start_total",
            message_type.MetricAction("inc", None),
            target, method, call_type, timeout
        )

    def rpc_client_invocation_end_total(self, target, method, call_type,
                                        timeout=None):
        self.put_rpc_client_metrics_to_txqueue(
            "rpc_client_invocation_end_total",
            message_type.MetricAction("inc", None),
            target, method, call_type, timeout
        )

    def rpc_client_processing_seconds(self, target, method, call_type,
                                      duration, timeout=None):
        self.put_rpc_client_metrics_to_txqueue(
            "rpc_client_processing_seconds",
            message_type.MetricAction("observe", duration),
            target, method, call_type, timeout
        )

    def rpc_client_exception_total(self, target, method, call_type, exception,
                                   timeout=None):
        self.put_rpc_client_metrics_to_txqueue(
            "rpc_client_exception_total",
            message_type.MetricAction("inc", None),
            target, method, call_type, timeout, exception
        )

    def put_rpc_server_metrics_to_txqueue(self, metric_name, action,
                                          target, endpoint, ns, ver, method,
                                          exception=None):
        kwargs = {
            'endpoint': endpoint,
            'namespace': ns,
            'version': ver,
            'method': method,
            'exchange': None,
            'topic': None,
            'server': None
        }
        if target:
            kwargs['exchange'] = target.exchange
            kwargs['topic'] = target.topic
            kwargs['server'] = target.server
        if exception:
            kwargs['exception'] = exception

        self.put_into_txqueue(metric_name, action, **kwargs)

    def rpc_server_invocation_start_total(self, target, endpoint,
                                          ns, ver, method):
        self.put_rpc_server_metrics_to_txqueue(
            "rpc_server_invocation_start_total",
            message_type.MetricAction("inc", None),
            target, endpoint, ns, ver, method
        )

    def rpc_server_invocation_end_total(self, target, endpoint,
                                        ns, ver, method):
        self.put_rpc_server_metrics_to_txqueue(
            "rpc_server_invocation_end_total",
            message_type.MetricAction("inc", None),
            target, endpoint, ns, ver, method
        )

    def rpc_server_processing_seconds(self, target, endpoint, ns, ver,
                                      method, duration):
        self.put_rpc_server_metrics_to_txqueue(
            "rpc_server_processing_seconds",
            message_type.MetricAction("observe", duration),
            target, endpoint, ns, ver, method
        )

    def rpc_server_exception_total(self, target, endpoint, ns, ver,
                                   method, exception):
        self.put_rpc_server_metrics_to_txqueue(
            "rpc_server_exception_total",
            message_type.MetricAction("inc", None),
            target, endpoint, ns, ver, method, exception=exception
        )


METRICS_COLLECTOR = None


def get_collector(conf, metrics_type, **kwargs):
    global threading
    threading = stdlib_threading
    global METRICS_COLLECTOR
    if METRICS_COLLECTOR is None:
        METRICS_COLLECTOR = MetricsCollectorClient(
            conf, metrics_type, **kwargs)
    return METRICS_COLLECTOR
