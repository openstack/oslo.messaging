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

import argparse
import bisect
import collections
import functools
import itertools
import json
import logging
import os
import random
import signal
import six
import socket
import string
import sys
import threading
import time
import yaml

from oslo_config import cfg
import oslo_messaging as messaging
from oslo_messaging import notify  # noqa
from oslo_messaging import rpc  # noqa
from oslo_utils import timeutils

LOG = logging.getLogger()
CURRENT_PID = None
CURRENT_HOST = None
CLIENTS = []
MESSAGES = []
IS_RUNNING = True
SERVERS = []
TRANSPORT = None

USAGE = """ Usage: ./simulator.py [-h] [--url URL] [-d DEBUG]\
 {notify-server,notify-client,rpc-server,rpc-client} ...

Usage example:
 python tools/simulator.py\
 --url rabbit://stackrabbit:secretrabbit@localhost/ rpc-server
 python tools/simulator.py\
 --url rabbit://stackrabbit:secretrabbit@localhost/ rpc-client\
 --exit-wait 15000 -p 64 -m 64"""

MESSAGES_LIMIT = 1000
DISTRIBUTION_BUCKET_SIZE = 500


def init_random_generator():
    data = []
    file_dir = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(file_dir, 'messages_length.yaml')) as m_file:
        content = yaml.safe_load(m_file)
        data += [int(n) for n in content[
            'test_data']['string_lengths'].split(', ')]

    ranges = collections.defaultdict(int)
    for msg_length in data:
        range_start = ((msg_length // DISTRIBUTION_BUCKET_SIZE) *
                       DISTRIBUTION_BUCKET_SIZE + 1)
        ranges[range_start] += 1

    ranges_start = sorted(ranges.keys())
    total_count = len(data)

    accumulated_distribution = []
    running_total = 0
    for range_start in ranges_start:
        norm = float(ranges[range_start]) / total_count
        running_total += norm
        accumulated_distribution.append(running_total)

    def weighted_random_choice():
        r = random.random() * running_total
        start = ranges_start[bisect.bisect_right(accumulated_distribution, r)]
        return random.randrange(start, start + DISTRIBUTION_BUCKET_SIZE)

    return weighted_random_choice


class LoggingNoParsingFilter(logging.Filter):
    def filter(self, record):
        msg = record.getMessage()
        for i in ['received {', 'MSG_ID is ']:
            if i in msg:
                return False
        return True


Message = collections.namedtuple(
    'Message', ['seq', 'cargo', 'client_ts', 'server_ts', 'return_ts'])


def make_message(seq, cargo, client_ts=0, server_ts=0, return_ts=0):
    return Message(seq, cargo, client_ts, server_ts, return_ts)


def update_message(message, **kwargs):
    return Message(*message)._replace(**kwargs)


class MessageStatsCollector(object):
    def __init__(self, label):
        self.label = label
        self.buffer = []  # buffer to store messages during report interval
        self.series = []  # stats for every report interval

        now = time.time()
        diff = int(now) - now + 1  # align start to whole seconds
        threading.Timer(diff, self.monitor).start()  # schedule in a second

    def monitor(self):
        global IS_RUNNING
        if IS_RUNNING:
            # NOTE(kbespalov): this way not properly works
            # because the monitor starting with range 1sec +-150 ms
            # due to high threading contention between rpc clients
            threading.Timer(1.0, self.monitor).start()
        now = time.time()

        count = len(self.buffer)

        size = 0
        min_latency = sys.maxsize
        max_latency = 0
        sum_latencies = 0

        for i in six.moves.range(count):
            p = self.buffer[i]
            size += len(p.cargo)

            latency = None
            if p.return_ts:
                latency = p.return_ts - p.client_ts  # round-trip
            elif p.server_ts:
                latency = p.server_ts - p.client_ts  # client -> server

            if latency:
                sum_latencies += latency
                min_latency = min(min_latency, latency)
                max_latency = max(max_latency, latency)

        del self.buffer[:count]  # trim processed items

        seq = len(self.series)
        stats = dict(seq=seq, timestamp=now, count=count, size=size)
        msg = ('%-14s: seq: %-4d count: %-6d bytes: %-10d' %
               (self.label, seq, count, size))

        if sum_latencies:
            latency = sum_latencies / count
            stats.update(dict(latency=latency,
                              min_latency=min_latency,
                              max_latency=max_latency))
            msg += (' latency: %-9.3f min: %-9.3f max: %-9.3f' %
                    (latency, min_latency, max_latency))

        self.series.append(stats)
        LOG.info(msg)

    def push(self, parsed_message):
        self.buffer.append(parsed_message)

    def get_series(self):
        return self.series

    @staticmethod
    def calc_stats(label, *collectors):
        count = 0
        size = 0
        min_latency = sys.maxsize
        max_latency = 0
        sum_latencies = 0
        start = sys.maxsize
        end = 0

        for point in itertools.chain(*(c.get_series() for c in collectors)):
            count += point['count']
            size += point['size']
            if point['count']:
                # NOTE(kbespalov):
                # we except the start and end time as time of
                # first and last processed message, no reason
                # to set boundaries if server was idle before
                # running of clients and after.
                start = min(start, point['timestamp'])
                end = max(end, point['timestamp'])

            if 'latency' in point:
                sum_latencies += point['latency'] * point['count']
                min_latency = min(min_latency, point['min_latency'])
                max_latency = max(max_latency, point['max_latency'])

        # start is the timestamp of the earliest block, which inclides samples
        # for the prior second
        start -= 1
        duration = end - start if count else 0
        stats = dict(count=count, size=size, duration=duration, count_p_s=0,
                     size_p_s=0)
        if duration:
            stats.update(dict(start=start, end=end,
                              count_p_s=count / duration,
                              size_p_s=size / duration))

        msg = ('%s: duration: %.2f count: %d (%.1f msg/sec) '
               'bytes: %d (%.0f bps)' %
               (label, duration, count, stats['count_p_s'],
                size, stats['size_p_s']))

        if sum_latencies:
            latency = sum_latencies / count
            stats.update(dict(latency=latency,
                              min_latency=min_latency,
                              max_latency=max_latency))
            msg += (' latency: %.3f min: %.3f max: %.3f' %
                    (latency, min_latency, max_latency))

        LOG.info(msg)
        return stats


class NotifyEndpoint(object):
    def __init__(self, wait_before_answer, requeue):
        self.wait_before_answer = wait_before_answer
        self.requeue = requeue
        self.received_messages = MessageStatsCollector('server')
        self.cache = set()

    def info(self, ctxt, publisher_id, event_type, payload, metadata):
        LOG.debug("%s %s %s %s", ctxt, publisher_id, event_type, payload)

        server_ts = time.time()

        message = update_message(payload, server_ts=server_ts)
        self.received_messages.push(message)

        if self.requeue and message.seq not in self.cache:
            self.cache.add(message.seq)

            if self.wait_before_answer > 0:
                time.sleep(self.wait_before_answer)

            return messaging.NotificationResult.REQUEUE

        return messaging.NotificationResult.HANDLED


def notify_server(transport, topic, wait_before_answer, duration, requeue):
    endpoints = [NotifyEndpoint(wait_before_answer, requeue)]
    target = messaging.Target(topic=topic)
    server = notify.get_notification_listener(transport, [target],
                                              endpoints, executor='eventlet')
    run_server(server, duration=duration)

    return endpoints[0]


class BatchNotifyEndpoint(object):
    def __init__(self, wait_before_answer, requeue):
        self.wait_before_answer = wait_before_answer
        self.requeue = requeue
        self.received_messages = MessageStatsCollector('server')
        self.cache = set()

    def info(self, batch):
        LOG.debug('msg rcv')
        LOG.debug("%s", batch)

        server_ts = time.time()

        for item in batch:
            message = update_message(item['payload'], server_ts=server_ts)
            self.received_messages.push(message)

        return messaging.NotificationResult.HANDLED


def batch_notify_server(transport, topic, wait_before_answer, duration,
                        requeue):
    endpoints = [BatchNotifyEndpoint(wait_before_answer, requeue)]
    target = messaging.Target(topic=topic)
    server = notify.get_batch_notification_listener(
        transport, [target],
        endpoints, executor='eventlet',
        batch_size=1000, batch_timeout=5)
    run_server(server, duration=duration)

    return endpoints[0]


class RpcEndpoint(object):
    def __init__(self, wait_before_answer):
        self.wait_before_answer = wait_before_answer
        self.received_messages = MessageStatsCollector('server')

    def info(self, ctxt, message):
        server_ts = time.time()

        LOG.debug("######## RCV: %s", message)

        reply = update_message(message, server_ts=server_ts)
        self.received_messages.push(reply)

        if self.wait_before_answer > 0:
            time.sleep(self.wait_before_answer)

        return reply


class ServerControlEndpoint(object):
    def __init__(self, controlled_server):
        self.connected_clients = set()
        self.controlled_server = controlled_server

    def sync_start(self, ctx, message):
        """Handle start reports from clients"""

        client_id = message['id']
        LOG.info('The client %s started to send messages' % client_id)
        self.connected_clients.add(client_id)

    def sync_done(self, ctx, message):
        """Handle done reports from clients"""

        client_id = message['id']
        LOG.info('The client %s finished msg sending.' % client_id)

        if client_id in self.connected_clients:
            self.connected_clients.remove(client_id)

        if not self.connected_clients:
            LOG.info(
                'The clients sent all messages. Shutting down the server..')
            threading.Timer(1, self._stop_server_with_delay).start()

    def _stop_server_with_delay(self):
        self.controlled_server.stop()
        self.controlled_server.wait()


class Client(object):
    def __init__(self, client_id, client, method, has_result,
                 wait_after_msg):
        self.client_id = client_id
        self.client = client
        self.method = method
        self.wait_after_msg = wait_after_msg

        self.seq = 0
        self.messages_count = len(MESSAGES)
        # Start sending the messages from a random position to avoid
        # memory re-usage and generate more realistic load on the library
        # and a message transport
        self.position = random.randint(0, self.messages_count - 1)
        self.sent_messages = MessageStatsCollector('client-%s' % client_id)
        self.errors = MessageStatsCollector('error-%s' % client_id)

        if has_result:
            self.round_trip_messages = MessageStatsCollector(
                'round-trip-%s' % client_id)

    def host_based_id(self):
        _id = "%(client_id)s %(salt)s@%(hostname)s"
        return _id % {'hostname': CURRENT_HOST,
                      'salt': hex(id(self))[2:],
                      'client_id': self.client_id}

    def send_msg(self):
        msg = make_message(self.seq, MESSAGES[self.position], time.time())
        self.sent_messages.push(msg)

        res = None
        try:
            res = self.method(self.client, msg)
        except Exception:
            self.errors.push(msg)
        else:
            LOG.debug("SENT: %s", msg)

        if res:
            return_ts = time.time()
            res = update_message(res, return_ts=return_ts)
            self.round_trip_messages.push(res)

        self.seq += 1
        self.position = (self.position + 1) % self.messages_count
        if self.wait_after_msg > 0:
            time.sleep(self.wait_after_msg)


class RPCClient(Client):
    def __init__(self, client_id, transport, target, timeout, is_cast,
                 wait_after_msg, sync_mode=False):

        client = rpc.RPCClient(transport, target)
        method = _rpc_cast if is_cast else _rpc_call

        super(RPCClient, self).__init__(client_id,
                                        client.prepare(timeout=timeout),
                                        method,
                                        not is_cast, wait_after_msg)
        self.sync_mode = sync_mode
        self.is_sync = False

        # prepare the sync client
        if sync_mode:
            if sync_mode == 'call':
                self.sync_client = self.client
            else:
                self.sync_client = client.prepare(fanout=True, timeout=timeout)

    def send_msg(self):
        if self.sync_mode and not self.is_sync:
            self.is_sync = self.sync_start()
        super(RPCClient, self).send_msg()

    def sync_start(self):
        try:
            msg = {'id': self.host_based_id()}
            method = _rpc_call if self.sync_mode == 'call' else _rpc_cast
            method(self.sync_client, msg, 'sync_start')
        except Exception:
            LOG.error('The client: %s failed to sync with %s.' %
                      (self.client_id, self.client.target))
            return False
        LOG.info('The client: %s successfully sync with  %s' % (
            self.client_id, self.client.target))
        return True

    def sync_done(self):
        try:
            msg = {'id': self.host_based_id()}
            method = _rpc_call if self.sync_mode == 'call' else _rpc_cast
            method(self.sync_client, msg, 'sync_done')
        except Exception:
            LOG.error('The client: %s failed finish the sync with %s.'
                      % (self.client_id, self.client.target))
            return False
        LOG.info('The client: %s successfully finished sync with %s'
                 % (self.client_id, self.client.target))
        return True


class NotifyClient(Client):
    def __init__(self, client_id, transport, topic, wait_after_msg):
        client = notify.Notifier(transport, driver='messaging', topic=topic)
        client = client.prepare(publisher_id='publisher-%d' % client_id)
        method = _notify
        super(NotifyClient, self).__init__(client_id, client, method,
                                           False, wait_after_msg)


def generate_messages(messages_count):
    # Limit the messages amount. Clients will reiterate the array again
    # if an amount of messages to be sent is bigger than MESSAGES_LIMIT
    if messages_count > MESSAGES_LIMIT:
        messages_count = MESSAGES_LIMIT
    LOG.info("Generating %d random messages", messages_count)
    generator = init_random_generator()
    for i in six.moves.range(messages_count):
        length = generator()
        msg = ''.join(random.choice(
                      string.ascii_lowercase) for x in six.moves.range(length))
        MESSAGES.append(msg)

    LOG.info("Messages has been prepared")


def wrap_sigexit(f):
    def inner(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except SignalExit as e:
            LOG.info('Signal %s is caught. Interrupting the execution',
                     e.signo)
            for server in SERVERS:
                server.stop()
                server.wait()
        finally:
            if TRANSPORT:
                TRANSPORT.cleanup()
    return inner


@wrap_sigexit
def run_server(server, duration=None):
    global IS_RUNNING
    SERVERS.append(server)
    server.start()
    if duration:
        with timeutils.StopWatch(duration) as stop_watch:
            while not stop_watch.expired() and IS_RUNNING:
                time.sleep(1)
        server.stop()
        IS_RUNNING = False
    server.wait()
    LOG.info('The server is terminating')
    time.sleep(1)  # wait for stats collector to process the last second


def rpc_server(transport, target, wait_before_answer, executor, duration):

    endpoints = [RpcEndpoint(wait_before_answer)]
    server = rpc.get_rpc_server(transport, target, endpoints, executor)

    # make the rpc server controllable by rpc clients
    endpoints.append(ServerControlEndpoint(server))

    LOG.debug("starting RPC server for target %s", target)

    run_server(server, duration=duration)

    return server.dispatcher.endpoints[0]


@wrap_sigexit
def spawn_rpc_clients(threads, transport, targets, wait_after_msg, timeout,
                      is_cast, messages_count, duration, sync_mode):
    p = eventlet.GreenPool(size=threads)
    targets = itertools.cycle(targets)

    for i in six.moves.range(threads):
        target = next(targets)
        LOG.debug("starting RPC client for target %s", target)
        client_builder = functools.partial(RPCClient, i, transport, target,
                                           timeout, is_cast, wait_after_msg,
                                           sync_mode)
        p.spawn_n(send_messages, i, client_builder,
                  messages_count, duration)
    p.waitall()


@wrap_sigexit
def spawn_notify_clients(threads, topic, transport, message_count,
                         wait_after_msg, timeout, duration):
    p = eventlet.GreenPool(size=threads)
    for i in six.moves.range(threads):
        client_builder = functools.partial(NotifyClient, i, transport, topic,
                                           wait_after_msg)
        p.spawn_n(send_messages, i, client_builder, message_count, duration)
    p.waitall()


def send_messages(client_id, client_builder, messages_count, duration):
    global IS_RUNNING
    client = client_builder()
    CLIENTS.append(client)

    # align message sending closer to whole seconds
    now = time.time()
    diff = int(now) - now + 1
    time.sleep(diff)

    if duration:
        with timeutils.StopWatch(duration) as stop_watch:
            while not stop_watch.expired() and IS_RUNNING:
                client.send_msg()
                eventlet.sleep()
        IS_RUNNING = False
    else:
        LOG.debug("Sending %d messages using client %d",
                  messages_count, client_id)
        for _ in six.moves.range(messages_count):
            client.send_msg()
            eventlet.sleep()
            if not IS_RUNNING:
                break
        LOG.debug("Client %d has sent %d messages", client_id, messages_count)

    # wait for replies to be collected
    time.sleep(1)

    # send stop request to the rpc server
    if isinstance(client, RPCClient) and client.is_sync:
        client.sync_done()


def _rpc_call(client, msg, remote_method='info'):
    try:
        res = client.call({}, remote_method, message=msg)
    except Exception as e:
        LOG.exception('Error %s on CALL for message %s', str(e), msg)
        raise
    else:
        LOG.debug("SENT: %s, RCV: %s", msg, res)
        return res


def _rpc_cast(client, msg, remote_method='info'):
    try:
        client.cast({}, remote_method, message=msg)
    except Exception as e:
        LOG.exception('Error %s on CAST for message %s', str(e), msg)
        raise
    else:
        LOG.debug("SENT: %s", msg)


def _notify(notification_client, msg):
    notification_client.info({}, 'compute.start', msg)


def show_server_stats(endpoint, json_filename):
    LOG.info('=' * 35 + ' summary ' + '=' * 35)
    output = dict(series={}, summary={})
    output['series']['server'] = endpoint.received_messages.get_series()
    stats = MessageStatsCollector.calc_stats(
        'server', endpoint.received_messages)
    output['summary'] = stats

    if json_filename:
        write_json_file(json_filename, output)


def show_client_stats(clients, json_filename, has_reply=False):
    LOG.info('=' * 35 + ' summary ' + '=' * 35)
    output = dict(series={}, summary={})

    for cl in clients:
        cl_id = cl.client_id
        output['series']['client_%s' % cl_id] = cl.sent_messages.get_series()
        output['series']['error_%s' % cl_id] = cl.errors.get_series()

        if has_reply:
            output['series']['round_trip_%s' % cl_id] = (
                cl.round_trip_messages.get_series())

    sent_stats = MessageStatsCollector.calc_stats(
        'client', *(cl.sent_messages for cl in clients))
    output['summary']['client'] = sent_stats

    error_stats = MessageStatsCollector.calc_stats(
        'error', *(cl.errors for cl in clients))
    output['summary']['error'] = error_stats

    if has_reply:
        round_trip_stats = MessageStatsCollector.calc_stats(
            'round-trip', *(cl.round_trip_messages for cl in clients))
        output['summary']['round_trip'] = round_trip_stats

    if json_filename:
        write_json_file(json_filename, output)


def write_json_file(filename, output):
    with open(filename, 'w') as f:
        f.write(json.dumps(output))
        LOG.info('Stats are written into %s', filename)


class SignalExit(SystemExit):
    def __init__(self, signo, exccode=1):
        super(SignalExit, self).__init__(exccode)
        self.signo = signo


def signal_handler(signum, frame):
    global IS_RUNNING
    IS_RUNNING = False

    raise SignalExit(signum)


def _setup_logging(is_debug):
    log_level = logging.DEBUG if is_debug else logging.INFO
    logging.basicConfig(
        stream=sys.stdout, level=log_level,
        format="%(asctime)-15s %(levelname)s %(name)s %(message)s")
    logging.getLogger().handlers[0].addFilter(LoggingNoParsingFilter())
    for i in ['kombu', 'amqp', 'stevedore', 'qpid.messaging'
              'oslo.messaging._drivers.amqp', ]:
        logging.getLogger(i).setLevel(logging.WARN)


def main():
    parser = argparse.ArgumentParser(
        description='Tools to play with oslo.messaging\'s RPC',
        usage=USAGE,
    )
    parser.add_argument('--url', dest='url',
                        help="oslo.messaging transport url")
    parser.add_argument('-d', '--debug', dest='debug', action='store_true',
                        help="Turn on DEBUG logging level instead of WARN")
    parser.add_argument('-tp', '--topic', dest='topic',
                        default="profiler_topic",
                        help="Topics to publish/receive messages to/from.")
    parser.add_argument('-s', '--server', dest='server',
                        default="profiler_server",
                        help="Servers to publish/receive messages to/from.")
    parser.add_argument('-tg', '--targets', dest='targets', nargs="+",
                        default=["profiler_topic.profiler_server"],
                        help="Targets to publish/receive messages to/from.")
    parser.add_argument('-l', dest='duration', type=int,
                        help='send messages for certain time')
    parser.add_argument('-j', '--json', dest='json_filename',
                        help='File name to store results in JSON format')
    parser.add_argument('--config-file', dest='config_file', type=str,
                        help="Oslo messaging config file")

    subparsers = parser.add_subparsers(dest='mode',
                                       help='notify/rpc server/client mode')

    server = subparsers.add_parser('notify-server')
    server.add_argument('-w', dest='wait_before_answer', type=int, default=-1)
    server.add_argument('--requeue', dest='requeue', action='store_true')

    server = subparsers.add_parser('batch-notify-server')
    server.add_argument('-w', dest='wait_before_answer', type=int, default=-1)
    server.add_argument('--requeue', dest='requeue', action='store_true')

    client = subparsers.add_parser('notify-client')
    client.add_argument('-p', dest='threads', type=int, default=1,
                        help='number of client threads')
    client.add_argument('-m', dest='messages', type=int, default=1,
                        help='number of call per threads')
    client.add_argument('-w', dest='wait_after_msg', type=float, default=-1,
                        help='sleep time between two messages')
    client.add_argument('--timeout', dest='timeout', type=int, default=3,
                        help='client timeout')

    server = subparsers.add_parser('rpc-server')
    server.add_argument('-w', dest='wait_before_answer', type=int, default=-1)
    server.add_argument('-e', '--executor', dest='executor',
                        type=str, default='eventlet',
                        help='name of a message executor')

    client = subparsers.add_parser('rpc-client')
    client.add_argument('-p', dest='threads', type=int, default=1,
                        help='number of client threads')
    client.add_argument('-m', dest='messages', type=int, default=1,
                        help='number of call per threads')
    client.add_argument('-w', dest='wait_after_msg', type=float, default=-1,
                        help='sleep time between two messages')
    client.add_argument('--timeout', dest='timeout', type=int, default=3,
                        help='client timeout')
    client.add_argument('--exit-wait', dest='exit_wait', type=int, default=0,
                        help='Keep connections open N seconds after calls '
                        'have been done')
    client.add_argument('--is-cast', dest='is_cast', action='store_true',
                        help='Use `call` or `cast` RPC methods')
    client.add_argument('--is-fanout', dest='is_fanout', action='store_true',
                        help='fanout=True for CAST messages')

    client.add_argument('--sync', dest='sync', choices=('call', 'fanout'),
                        help="stop server when all msg was sent by clients")

    args = parser.parse_args()

    _setup_logging(is_debug=args.debug)

    if args.config_file:
        cfg.CONF(["--config-file", args.config_file])

    global TRANSPORT
    if args.mode in ['rpc-server', 'rpc-client']:
        TRANSPORT = messaging.get_transport(cfg.CONF, url=args.url)
    else:
        TRANSPORT = messaging.get_notification_transport(cfg.CONF,
                                                         url=args.url)

    if args.mode in ['rpc-client', 'notify-client']:
        # always generate maximum number of messages for duration-limited tests
        generate_messages(MESSAGES_LIMIT if args.duration else args.messages)

    # oslo.config defaults
    cfg.CONF.heartbeat_interval = 5
    cfg.CONF.prog = os.path.basename(__file__)
    cfg.CONF.project = 'oslo.messaging'

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    if args.mode == 'rpc-server':
        target = messaging.Target(topic=args.topic, server=args.server)
        endpoint = rpc_server(TRANSPORT, target, args.wait_before_answer,
                              args.executor, args.duration)
        show_server_stats(endpoint, args.json_filename)

    elif args.mode == 'notify-server':
        endpoint = notify_server(TRANSPORT, args.topic,
                                 args.wait_before_answer, args.duration,
                                 args.requeue)
        show_server_stats(endpoint, args.json_filename)

    elif args.mode == 'batch-notify-server':
        endpoint = batch_notify_server(TRANSPORT, args.topic,
                                       args.wait_before_answer,
                                       args.duration, args.requeue)
        show_server_stats(endpoint, args.json_filename)

    elif args.mode == 'notify-client':
        spawn_notify_clients(args.threads, args.topic, TRANSPORT,
                             args.messages, args.wait_after_msg,
                             args.timeout, args.duration)
        show_client_stats(CLIENTS, args.json_filename)

    elif args.mode == 'rpc-client':

        targets = []
        for target in args.targets:
            tp, srv = target.partition('.')[::2]
            t = messaging.Target(topic=tp, server=srv, fanout=args.is_fanout)
            targets.append(t)

        spawn_rpc_clients(args.threads, TRANSPORT, targets,
                          args.wait_after_msg, args.timeout, args.is_cast,
                          args.messages, args.duration, args.sync)
        show_client_stats(CLIENTS, args.json_filename, not args.is_cast)

        if args.exit_wait:
            LOG.info("Finished. waiting for %d seconds", args.exit_wait)
            time.sleep(args.exit_wait)


if __name__ == '__main__':
    CURRENT_PID = os.getpid()
    CURRENT_HOST = socket.gethostname()
    main()
