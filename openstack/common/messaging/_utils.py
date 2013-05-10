
# Copyright 2010 United States Government as represented by the
# Administrator of the National Aeronautics and Space Administration.
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

import urlparse


def version_is_compatible(imp_version, version):
    """Determine whether versions are compatible.

    :param imp_version: The version implemented
    :param version: The version requested by an incoming message.
    """
    version_parts = version.split('.')
    imp_version_parts = imp_version.split('.')
    if int(version_parts[0]) != int(imp_version_parts[0]):  # Major
        return False
    if int(version_parts[1]) > int(imp_version_parts[1]):  # Minor
        return False
    return True


def exchange_from_url(self, url, default_exchange=None):
    """Parse an exchange name from a URL.

    Assuming a URL takes the form of:

      transport:///myexchange

    then parse the URL and return the exchange name.

    :param url: the URL to parse
    :type url: str
    :param default_exchange: what to return if no exchange found in URL
    :type default_exchange: str
    """
    if not url:
        return default_exchange

    url = urlparse.urlparse(url)
    if not url.path.startswith('/'):
        return default_exchange

    parts = u.path[1:].split('/')

    return parts[0] if parts[0] else default_exchange
