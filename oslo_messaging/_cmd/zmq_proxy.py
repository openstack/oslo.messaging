#    Copyright 2015-2016 Mirantis, Inc.
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

from oslo_config import cfg

from oslo_messaging._drivers.zmq_driver.proxy import zmq_proxy
from oslo_messaging._drivers.zmq_driver import zmq_options
from oslo_messaging._i18n import _LI

LOG = logging.getLogger(__name__)


def main():

    conf = cfg.CONF
    opt_group = cfg.OptGroup(name='zmq_proxy_opts',
                             title='ZeroMQ proxy options')
    conf.register_opts(zmq_proxy.zmq_proxy_opts, group=opt_group)
    zmq_options.register_opts(conf)
    zmq_proxy.parse_command_line_args(conf)

    reactor = zmq_proxy.ZmqProxy(conf)

    try:
        while True:
            reactor.run()
    except (KeyboardInterrupt, SystemExit):
        LOG.info(_LI("Exit proxy by interrupt signal."))
    finally:
        reactor.close()


if __name__ == "__main__":
    main()
