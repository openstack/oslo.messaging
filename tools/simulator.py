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

# Usage example:
#  python tools/simulator.py \
#     --url rabbit://stackrabbit:secretrabbit@localhost/ rpc-server
#  python tools/simulator.py
#     --url rabbit://stackrabbit:secretrabbit@localhost/ rpc-client \
#     --exit-wait 15000 -p 64 -m 64

import eventlet
eventlet.monkey_patch()

import argparse
import logging
import sys
import time

from oslo.config import cfg
from oslo import messaging
from oslo.messaging import notify
from oslo.messaging import rpc

LOG = logging.getLogger()


class LoggingNoParsingFilter(logging.Filter):
    def filter(self, record):
        msg = record.getMessage()
        for i in ['received {', 'MSG_ID is ']:
            if i in msg:
                return False
        return True


class NotifyEndpoint(object):
    def __init__(self):
        self.cache = []

    def info(self, ctxt, publisher_id, event_type, payload, metadata):
        LOG.info('msg rcv')
        LOG.info("%s %s %s %s" % (ctxt, publisher_id, event_type, payload))
        if payload not in self.cache:
            LOG.info('requeue msg')
            self.cache.append(payload)
            for i in range(15):
                eventlet.sleep(1)
            return messaging.NotificationResult.REQUEUE
        else:
            LOG.info('ack msg')
        return messaging.NotificationResult.HANDLED


def notify_server(transport):
    endpoints = [NotifyEndpoint()]
    target = messaging.Target(topic='n-t1')
    server = notify.get_notification_listener(transport, [target],
                                              endpoints, executor='eventlet')
    server.start()
    server.wait()


class RpcEndpoint(object):
    def __init__(self, wait_before_answer):
        self.count = None
        self.wait_before_answer = wait_before_answer

    def info(self, ctxt, message):
        i = int(message.replace('test ', ''))
        if self.count is None:
            self.count = i
        elif i == 0:
            self.count = 0
        else:
            self.count += 1

        LOG.info("######## RCV: %s/%s" % (self.count, message))
        if self.wait_before_answer > 0:
            time.sleep(self.wait_before_answer)
        return "OK: %s" % message


def rpc_server(transport, wait_before_answer):
    endpoints = [RpcEndpoint(wait_before_answer)]
    target = messaging.Target(topic='t1', server='moi')
    server = rpc.get_rpc_server(transport, target,
                                endpoints, executor='eventlet')
    server.start()
    server.wait()


def threads_spawner(threads, method, *args, **kwargs):
    p = eventlet.GreenPool(size=threads)
    for i in range(0, threads):
        p.spawn_n(method, i, *args, **kwargs)
    p.waitall()


def rpc_call(_id, transport, messages, wait_after_msg, timeout):
    target = messaging.Target(topic='t1', server='moi')
    c = rpc.RPCClient(transport, target)
    c = c.prepare(timeout=timeout)
    for i in range(0, messages):
        payload = "test %d" % i
        LOG.info("SEND: %s" % payload)
        try:
            res = c.call({}, 'info', message=payload)
        except Exception:
            LOG.exception('no RCV for %s' % i)
        else:
            LOG.info("RCV: %s" % res)
        if wait_after_msg > 0:
            time.sleep(wait_after_msg)


def notifier(_id, transport, messages, wait_after_msg, timeout):
    n1 = notify.Notifier(transport, topic="n-t1").prepare(
        publisher_id='publisher-%d' % _id)
    msg = 0
    for i in range(0, messages):
        msg = 1 + msg
        ctxt = {}
        payload = dict(msg=msg, vm='test', otherdata='ahah')
        LOG.info("send msg")
        LOG.info(payload)
        n1.info(ctxt, 'compute.start1', payload)
        if wait_after_msg > 0:
            time.sleep(wait_after_msg)


def main():
    parser = argparse.ArgumentParser(description='RPC DEMO')
    parser.add_argument('--url', dest='url',
                        default='rabbit://guest:password@localhost/',
                        help="oslo.messaging transport url")
    subparsers = parser.add_subparsers(dest='mode',
                                       help='notify/rpc server/client mode')

    server = subparsers.add_parser('notify-server')
    client = subparsers.add_parser('notify-client')
    client.add_argument('-p', dest='threads', type=int, default=1,
                        help='number of client threads')
    client.add_argument('-m', dest='messages', type=int, default=1,
                        help='number of call per threads')
    client.add_argument('-w', dest='wait_after_msg', type=int, default=-1,
                        help='sleep time between two messages')
    client.add_argument('-t', dest='timeout', type=int, default=3,
                        help='client timeout')

    server = subparsers.add_parser('rpc-server')
    server.add_argument('-w', dest='wait_before_answer', type=int, default=-1)

    client = subparsers.add_parser('rpc-client')
    client.add_argument('-p', dest='threads', type=int, default=1,
                        help='number of client threads')
    client.add_argument('-m', dest='messages', type=int, default=1,
                        help='number of call per threads')
    client.add_argument('-w', dest='wait_after_msg', type=int, default=-1,
                        help='sleep time between two messages')
    client.add_argument('-t', dest='timeout', type=int, default=3,
                        help='client timeout')
    client.add_argument('--exit-wait', dest='exit_wait', type=int, default=0,
                        help='Keep connections open N seconds after calls '
                        'have been done')

    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    logging.getLogger().handlers[0].addFilter(LoggingNoParsingFilter())
    for i in ['kombu', 'amqp', 'stevedore', 'qpid.messaging'
              'oslo.messaging._drivers.amqp', ]:
        logging.getLogger(i).setLevel(logging.WARN)

    # oslo.config defaults
    cfg.CONF.heartbeat_interval = 5
    cfg.CONF.notification_topics = "notif"
    cfg.CONF.notification_driver = "messaging"

    # the transport
    transport = messaging.get_transport(cfg.CONF, url=args.url)

    if args.mode == 'rpc-server':
        rpc_server(transport, args.wait_before_answer)
    elif args.mode == 'notify-server':
        notify_server(transport)
    elif args.mode == 'notify-client':
        threads_spawner(args.threads, notifier, transport, args.messages,
                        args.wait_after_msg, args.timeout)
    elif args.mode == 'rpc-client':
        threads_spawner(args.threads, rpc_call, transport, args.messages,
                        args.wait_after_msg, args.timeout)
        LOG.info("calls finished, wait %d seconds" % args.exit_wait)
        time.sleep(args.exit_wait)


if __name__ == '__main__':
    main()
