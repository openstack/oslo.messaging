# Copyright 2011 OpenStack Foundation.
# All Rights Reserved.
# Copyright 2013 Red Hat, Inc.
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

from oslo_messaging.notify import notifier

NOTIFICATIONS = []


def reset():
    "Clear out the list of recorded notifications."
    global NOTIFICATIONS
    NOTIFICATIONS = []


class TestDriver(notifier.Driver):

    "Store notifications in memory for test verification."

    def notify(self, ctxt, message, priority, retry):
        NOTIFICATIONS.append((ctxt, message, priority, retry))
