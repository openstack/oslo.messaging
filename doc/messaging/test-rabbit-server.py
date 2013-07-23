
import eventlet

eventlet.monkey_patch(os=False)

import logging
import socket

from oslo.config import cfg

from oslo import messaging

_opts = [
    cfg.StrOpt('host', default=socket.gethostname()),
]

CONF = cfg.CONF
CONF.register_opts(_opts)

LOG = logging.getLogger('server')

CONF()
CONF.log_opt_values(LOG, logging.DEBUG)

logging.basicConfig(level=logging.DEBUG)

class Server(object):

    def __init__(self, transport):
        self.target = messaging.Target(topic='topic',
                                       server=transport.conf.host)
        self._server = messaging.get_rpc_server(transport,
                                                self.target,
                                                [self],
                                                executor='eventlet')
        super(Server, self).__init__()

    def start(self):
        self._server.start()

    def wait(self):
        self._server.wait()

    def ping(self, ctxt):
        LOG.info("PING")
        return 'ping'

transport = messaging.get_transport(CONF, 'rabbit:///test')

server = Server(transport)
server.start()
server.wait()
