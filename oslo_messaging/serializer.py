#    Copyright 2013 IBM Corp.
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

"""Provides the definition of a message serialization handler"""

import abc

from oslo_serialization import jsonutils
import six

__all__ = ['Serializer', 'NoOpSerializer', 'JsonPayloadSerializer']


@six.add_metaclass(abc.ABCMeta)
class Serializer(object):
    """Generic (de-)serialization definition base class."""

    @abc.abstractmethod
    def serialize_entity(self, ctxt, entity):
        """Serialize something to primitive form.

        :param ctxt: Request context, in deserialized form
        :param entity: Entity to be serialized
        :returns: Serialized form of entity
        """

    @abc.abstractmethod
    def deserialize_entity(self, ctxt, entity):
        """Deserialize something from primitive form.

        :param ctxt: Request context, in deserialized form
        :param entity: Primitive to be deserialized
        :returns: Deserialized form of entity
        """

    @abc.abstractmethod
    def serialize_context(self, ctxt):
        """Serialize a request context into a dictionary.

        :param ctxt: Request context
        :returns: Serialized form of context
        """

    @abc.abstractmethod
    def deserialize_context(self, ctxt):
        """Deserialize a dictionary into a request context.

        :param ctxt: Request context dictionary
        :returns: Deserialized form of entity
        """


class NoOpSerializer(Serializer):
    """A serializer that does nothing."""

    def serialize_entity(self, ctxt, entity):
        return entity

    def deserialize_entity(self, ctxt, entity):
        return entity

    def serialize_context(self, ctxt):
        return ctxt

    def deserialize_context(self, ctxt):
        return ctxt


class JsonPayloadSerializer(NoOpSerializer):
    @staticmethod
    def serialize_entity(context, entity):
        return jsonutils.to_primitive(entity, convert_instances=True)
