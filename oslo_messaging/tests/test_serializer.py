# Copyright 2015 Mirantis Inc.
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

from oslo_context import context as common_context
from six.moves import mock

from oslo_messaging import serializer
from oslo_messaging.tests import utils as test_utils


class TestRequestContextSerializer(test_utils.BaseTestCase):

    def setUp(self):
        super(TestRequestContextSerializer, self).setUp()

        self.serializer = serializer.RequestContextSerializer(mock.MagicMock())
        self.context = common_context.RequestContext()
        self.entity = {'foo': 'bar'}

    def test_serialize_entity(self):
        self.serializer.serialize_entity(self.context, self.entity)
        self.serializer._base.serialize_entity.assert_called_with(
            self.context, self.entity)

    def test_serialize_entity_empty_base(self):
        # NOTE(viktors): Return False for check `if self.serializer._base:`
        bool_args = {'__bool__': lambda *args: False,
                     '__nonzero__': lambda *args: False}
        self.serializer._base.configure_mock(**bool_args)

        entity = self.serializer.serialize_entity(self.context, self.entity)
        self.assertFalse(self.serializer._base.serialize_entity.called)
        self.assertEqual(self.entity, entity)

    def test_deserialize_entity(self):
        self.serializer.deserialize_entity(self.context, self.entity)
        self.serializer._base.deserialize_entity.assert_called_with(
            self.context, self.entity)

    def test_deserialize_entity_empty_base(self):
        # NOTE(viktors): Return False for check `if self.serializer._base:`
        bool_args = {'__bool__': lambda *args: False,
                     '__nonzero__': lambda *args: False}
        self.serializer._base.configure_mock(**bool_args)

        entity = self.serializer.deserialize_entity(self.context, self.entity)
        self.assertFalse(self.serializer._base.serialize_entity.called)
        self.assertEqual(self.entity, entity)

    def test_serialize_context(self):
        new_context = self.serializer.serialize_context(self.context)

        self.assertEqual(self.context.to_dict(), new_context)

    @mock.patch.object(common_context.RequestContext, 'from_dict',
                       return_value='foobar')
    def test_deserialize_context(self, mock_to_dict):
        new_context = self.serializer.deserialize_context(self.context)

        mock_to_dict.assert_called_with(self.context)
        self.assertEqual(
            common_context.RequestContext.from_dict(self.context),
            new_context
        )
