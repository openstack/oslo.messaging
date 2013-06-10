
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


def parse_url(url, default_exchange=None):
    """Parse an url.

    Assimung a URL takes the form of:

        transport://username:password@host1:port[,hostN:portN]/exchange[?option=value]

    then parse the URL and return a dictionary with the following structure:

        {
            'exchange': 'exchange'
            'transport': 'transport',
            'hosts': [{'username': 'username',
                       'password': 'password'
                       'host': 'host1:port1'},
                       ...],
            'parameters': {'option': 'value'}
        }

    Netloc is parsed following the sequence bellow:

    * It is first splitted by ',' in order to support multiple hosts
    * The last parsed username and password will be propagated to the rest
    of hotsts specified:

      user:passwd@host1:port1,host2:port2

      [
       {"username": "user", "password": "passwd", "host": "host1:port1"},
       {"username": "user", "password": "passwd", "host": "host2:port2"}
      ]

    * In order to avoid the above propagation, it is possible to alter the order
    in which the hosts are specified or specify a set of fake credentials using
    ",:@host2:port2"


      user:passwd@host1:port1,:@host2:port2

      [
       {"username": "user", "password": "passwd", "host": "host1:port1"},
       {"username": "", "password": "", "host": "host2:port2"}
      ]

    :param url: The URL to parse
    :type url: str
    :param default_exchange: what to return if no exchange found in URL
    :type default_exchange: str
    :returns: A dictionary with the parsed data
    """

    # NOTE(flaper87): Not PY3K compliant
    if not isinstance(url, basestring):
        raise TypeError("Wrong URL type")

    url = urlparse.urlparse(url)

    parsed = dict(transport=url.scheme)

    # NOTE(flaper87): Set the exchange.
    # if it is / or None then use the
    # default one.
    exchange = default_exchange
    if url.path and url.path != "/":
        exchange = url.path[1:].split("/")[0]
    parsed["exchange"] = exchange

    # NOTE(flaper87): Parse netloc.
    hosts = []
    username = password = ''
    for host in url.netloc.split(","):
        if not host:
            continue

        if "@" in host:
            creds, host = host.split("@")
            username, password = creds.split(":")

        hosts.append({
            "host": host,
            "username": username,
            "password": password,
        })

    parsed["hosts"] = hosts

    parameters = {}
    if url.query:
        # NOTE(flaper87): This returns a dict with
        # key -> [value], those values need to be
        # normalized
        parameters = urlparse.parse_qs(url.query)
    parsed['parameters'] = parameters

    return parsed


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
