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

LOG = logging.getLogger('client')

logging.basicConfig(level=logging.DEBUG)

CONF()
CONF.log_opt_values(LOG, logging.DEBUG)

class Client(object):

    def __init__(self, transport):
        target = messaging.Target(topic='topic')
        self._client = messaging.RPCClient(transport, target)
        super(Client, self).__init__()

    def ping(self, ctxt):
        return self._client.call(ctxt, 'ping')

transport = messaging.get_transport(CONF, 'rabbit:///test')

client = Client(transport)
print client.ping({})
