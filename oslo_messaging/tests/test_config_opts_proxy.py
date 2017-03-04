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

from oslo_config import cfg
from oslo_config import types

from oslo_messaging._drivers import common as drv_cmn
from oslo_messaging.tests import utils as test_utils
from oslo_messaging import transport


class TestConfigOptsProxy(test_utils.BaseTestCase):

    def test_rabbit(self):
        group = 'oslo_messaging_rabbit'
        self.config(rabbit_retry_interval=1,
                    rabbit_qos_prefetch_count=0,
                    rabbit_max_retries=3,
                    group=group)
        dummy_opts = [cfg.ListOpt('list_str', item_type=types.String(),
                                  default=[]),
                      cfg.ListOpt('list_int', item_type=types.Integer(),
                                  default=[]),
                      cfg.DictOpt('dict', default={}),
                      cfg.BoolOpt('bool', default=False),
                      cfg.StrOpt('str', default='default')]
        self.conf.register_opts(dummy_opts, group=group)
        url = transport.TransportURL.parse(
            self.conf, "rabbit:///"
                       "?rabbit_qos_prefetch_count=2"
                       "&list_str=1&list_str=2&list_str=3"
                       "&list_int=1&list_int=2&list_int=3"
                       "&dict=x:1&dict=y:2&dict=z:3"
                       "&bool=True"
        )
        conf = drv_cmn.ConfigOptsProxy(self.conf, url, group)
        self.assertRaises(cfg.NoSuchOptError,
                          conf.__getattr__,
                          'unknown_group')
        self.assertIsInstance(getattr(conf, group),
                              conf.GroupAttrProxy)
        self.assertEqual(1, conf.oslo_messaging_rabbit.rabbit_retry_interval)
        self.assertEqual(2,
                         conf.oslo_messaging_rabbit.rabbit_qos_prefetch_count)
        self.assertEqual(3, conf.oslo_messaging_rabbit.rabbit_max_retries)
        self.assertEqual(['1', '2', '3'], conf.oslo_messaging_rabbit.list_str)
        self.assertEqual([1, 2, 3], conf.oslo_messaging_rabbit.list_int)
        self.assertEqual({'x': '1', 'y': '2', 'z': '3'},
                         conf.oslo_messaging_rabbit.dict)
        self.assertEqual(True, conf.oslo_messaging_rabbit.bool)
        self.assertEqual('default', conf.oslo_messaging_rabbit.str)

    def test_not_in_group(self):
        group = 'oslo_messaging_rabbit'
        url = transport.TransportURL.parse(
            self.conf, "rabbit:///?unknown_opt=4"
        )
        self.assertRaises(cfg.NoSuchOptError,
                          drv_cmn.ConfigOptsProxy,
                          self.conf, url, group)

    def test_invalid_value(self):
        group = 'oslo_messaging_rabbit'
        self.config(kombu_reconnect_delay=5.0,
                    group=group)
        url = transport.TransportURL.parse(
            self.conf, "rabbit:///?kombu_reconnect_delay=invalid_value"
        )
        self.assertRaises(ValueError, drv_cmn.ConfigOptsProxy, self.conf,
                          url, group)
