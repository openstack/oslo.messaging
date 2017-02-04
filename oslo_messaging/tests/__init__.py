# Copyright 2014 eNovance
# All Rights Reserved.
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

import eventlet
eventlet.monkey_patch()

# oslotest prepares mock for six in oslotest/__init__.py as follow:
# six.add_move(six.MovedModule('mock', 'mock', 'unittest.mock')) and
# oslo.messaging imports oslotest before importing test submodules to
# setup six.moves for mock, then "from six.moves import mock" works well.
import oslotest
