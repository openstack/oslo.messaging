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

import os
import re

from oslo_messaging._drivers.common import RPCException
from oslo_messaging._drivers.zmq_driver import zmq_serializer
from oslo_messaging.tests import utils as test_utils


class TestZmqSerializer(test_utils.BaseTestCase):

    def test_message_without_topic_raises_RPCException(self):
        # The topic is the 4th element of the message.
        msg_without_topic = ['only', 'three', 'parts']

        expected = "Message did not contain a topic: %s" % msg_without_topic
        with self.assertRaisesRegexp(RPCException, re.escape(expected)):
            zmq_serializer.get_topic_from_call_message(msg_without_topic)

    def test_invalid_topic_format_raises_RPCException(self):
        invalid_topic = "no dots to split on, so not index-able".encode('utf8')
        bad_message = ['', '', '', invalid_topic]

        expected_msg = "Topic was not formatted correctly: %s"
        expected_msg = expected_msg % invalid_topic.decode('utf8')
        with self.assertRaisesRegexp(RPCException, expected_msg):
            zmq_serializer.get_topic_from_call_message(bad_message)

    def test_py3_decodes_bytes_correctly(self):
        message = ['', '', '', b'topic.ipaddress']

        actual, _ = zmq_serializer.get_topic_from_call_message(message)

        self.assertEqual('topic', actual)

    def test_bad_characters_in_topic_raise_RPCException(self):
        # handle unexpected os path separators:
        unexpected_evil = '<'
        os.path.sep = unexpected_evil

        unexpected_alt_evil = '>'
        os.path.altsep = unexpected_alt_evil

        evil_chars = [unexpected_evil, unexpected_alt_evil, '\\', '/']

        for evil_char in evil_chars:
            evil_topic = '%s%s%s' % ('trust.me', evil_char, 'please')
            evil_topic = evil_topic.encode('utf8')
            evil_message = ['', '', '', evil_topic]

            expected_msg = "Topic contained dangerous characters: %s"
            expected_msg = expected_msg % evil_topic.decode('utf8')
            expected_msg = re.escape(expected_msg)

            with self.assertRaisesRegexp(RPCException, expected_msg):
                zmq_serializer.get_topic_from_call_message(evil_message)
