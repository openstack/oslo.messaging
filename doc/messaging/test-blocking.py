
import socket
import threading

from oslo.config import cfg

from openstack.common import log as logging
from openstack.common import messaging

logging.setup('test-blocking')

_opts = [
    cfg.StrOpt('host', default=socket.gethostname()),
]

CONF = cfg.CONF
CONF.register_opts(_opts)

class Server(messaging.BlockingRPCServer):

    def __init__(self, transport):
        target = messaging.Target(topic='testtopic',
                                  server=transport.conf.host,
                                  version='2.5')
        super(Server, self).__init__(transport, target, [self])

    def test(self, ctxt, arg):
        self.stop()
        return arg

transport = messaging.get_transport(CONF, 'fake:///testexchange')

server = Server(transport)

def server_thread(server):
    server.start()

thread = threading.Thread(target=server_thread, args=[server])
thread.daemon = True
thread.start()

class Client(messaging.RPCClient):

    def __init__(self, transport):
        target = messaging.Target(topic='testtopic', version='2.0')
        super(Client, self).__init__(transport, target)

    def test(self, ctxt, arg):
        cctxt = self.prepare(version='2.5')
        return cctxt.call(ctxt, 'test', arg=arg)


client = Client(transport)
print client.test({'c': 'b'}, 'foo')

while thread.isAlive():
    thread.join(.05)
