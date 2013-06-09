

import eventlet

eventlet.monkey_patch(os=False)

import socket

from oslo.config import cfg

from openstack.common import log as logging
from openstack.common import messaging
from openstack.common.messaging import eventlet as evmsg

logging.setup('test-eventlet')

_opts = [
    cfg.StrOpt('host', default=socket.gethostname()),
]

CONF = cfg.CONF
CONF.register_opts(_opts)

class Server(evmsg.EventletRPCServer):

    def __init__(self, transport):
        target = messaging.Target(topic='testtopic',
                                  server=transport.conf.host,
                                  version='2.5')
        super(Server, self).__init__(transport, target, [self])

    def test(self, ctxt, arg):
        return arg

transport = messaging.get_transport(CONF, 'fake:///testexchange')

server = Server(transport)
server.start()


class Client(messaging.RPCClient):

    def __init__(self, transport):
        target = messaging.Target(topic='testtopic', version='2.0')
        super(Client, self).__init__(transport, target)

    def test(self, ctxt, arg):
        cctxt = self.prepare(version='2.5')
        return cctxt.call(ctxt, 'test', arg=arg)


client = Client(transport)
print client.test({'c': 'b'}, 'foo')
