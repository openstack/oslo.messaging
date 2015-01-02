
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


def version_is_compatible(imp_version, version):
    """Determine whether versions are compatible.

    :param imp_version: The version implemented
    :param version: The version requested by an incoming message.
    """
    version_parts = version.split('.')
    imp_version_parts = imp_version.split('.')
    try:
        rev = version_parts[2]
    except IndexError:
        rev = 0
    try:
        imp_rev = imp_version_parts[2]
    except IndexError:
        imp_rev = 0

    if int(version_parts[0]) != int(imp_version_parts[0]):  # Major
        return False
    if int(version_parts[1]) > int(imp_version_parts[1]):  # Minor
        return False
    if (int(version_parts[1]) == int(imp_version_parts[1]) and
            int(rev) > int(imp_rev)):  # Revision
        return False
    return True
