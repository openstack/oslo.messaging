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
import logging
import random
import socket
import threading
import time

from oslo_config import cfg
import pika
from pika import credentials as pika_credentials

from oslo_messaging._drivers.pika_driver import pika_commons as pika_drv_cmns
from oslo_messaging._drivers.pika_driver import pika_connection
from oslo_messaging._drivers.pika_driver import pika_exceptions as pika_drv_exc

LOG = logging.getLogger(__name__)

# constant for setting tcp_user_timeout socket option
# (it should be defined in 'select' module of standard library in future)
TCP_USER_TIMEOUT = 18

# constants for creating connection statistics
HOST_CONNECTION_LAST_TRY_TIME = "last_try_time"
HOST_CONNECTION_LAST_SUCCESS_TRY_TIME = "last_success_try_time"

pika_opts = [
    cfg.IntOpt('channel_max',
               help='Maximum number of channels to allow'),
    cfg.IntOpt('frame_max',
               help='The maximum byte size for an AMQP frame'),
    cfg.IntOpt('heartbeat_interval', default=3,
               help="How often to send heartbeats for consumer's connections"),
    cfg.BoolOpt('ssl',
                help='Enable SSL'),
    cfg.DictOpt('ssl_options',
                help='Arguments passed to ssl.wrap_socket'),
    cfg.FloatOpt('socket_timeout', default=0.25,
                 help="Set socket timeout in seconds for connection's socket"),
    cfg.FloatOpt('tcp_user_timeout', default=0.25,
                 help="Set TCP_USER_TIMEOUT in seconds for connection's "
                      "socket"),
    cfg.FloatOpt('host_connection_reconnect_delay', default=0.25,
                 help="Set delay for reconnection to some host which has "
                      "connection error"),
    cfg.StrOpt('connection_factory', default="single",
               choices=["new", "single", "read_write"],
               help='Connection factory implementation')
]


class PikaConnectionFactory(object):

    def __init__(self, url, conf):
        self._url = url
        self._conf = conf

        self._connection_lock = threading.RLock()

        if not url.hosts:
            raise ValueError("You should provide at least one RabbitMQ host")

        # initializing connection parameters for configured RabbitMQ hosts
        self._common_pika_params = {
            'virtual_host': url.virtual_host,
            'channel_max': conf.oslo_messaging_pika.channel_max,
            'frame_max': conf.oslo_messaging_pika.frame_max,
            'ssl': conf.oslo_messaging_pika.ssl,
            'ssl_options': conf.oslo_messaging_pika.ssl_options,
            'socket_timeout': conf.oslo_messaging_pika.socket_timeout
        }

        self._host_list = url.hosts
        self._heartbeat_interval = conf.oslo_messaging_pika.heartbeat_interval
        self._host_connection_reconnect_delay = (
            conf.oslo_messaging_pika.host_connection_reconnect_delay
        )
        self._tcp_user_timeout = conf.oslo_messaging_pika.tcp_user_timeout

        self._connection_host_status = {}

        self._cur_connection_host_num = random.randint(
            0, len(url.hosts) - 1
        )

    def cleanup(self):
        pass

    def create_connection(self, for_listening=False):
        """Create and return connection to any available host.

        :return: created connection
        :raise: ConnectionException if all hosts are not reachable
        """

        with self._connection_lock:

            host_count = len(self._host_list)
            connection_attempts = host_count

            while connection_attempts > 0:
                self._cur_connection_host_num += 1
                self._cur_connection_host_num %= host_count
                try:
                    return self._create_host_connection(
                        self._cur_connection_host_num, for_listening
                    )
                except pika_drv_cmns.PIKA_CONNECTIVITY_ERRORS as e:
                    LOG.warning("Can't establish connection to host. %s", e)
                except pika_drv_exc.HostConnectionNotAllowedException as e:
                    LOG.warning("Connection to host is not allowed. %s", e)

                connection_attempts -= 1

            raise pika_drv_exc.EstablishConnectionException(
                "Can not establish connection to any configured RabbitMQ "
                "host: " + str(self._host_list)
            )

    def _set_tcp_user_timeout(self, s):
        if not self._tcp_user_timeout:
            return
        try:
            s.setsockopt(
                socket.IPPROTO_TCP, TCP_USER_TIMEOUT,
                int(self._tcp_user_timeout * 1000)
            )
        except socket.error:
            LOG.warning(
                "Whoops, this kernel doesn't seem to support TCP_USER_TIMEOUT."
            )

    def _create_host_connection(self, host_index, for_listening):
        """Create new connection to host #host_index

        :param host_index: Integer, number of host for connection establishing
        :param for_listening: Boolean, creates connection for listening
            if True
        :return: New connection
        """
        host = self._host_list[host_index]

        cur_time = time.time()

        host_connection_status = self._connection_host_status.get(host)

        if host_connection_status is None:
            host_connection_status = {
                HOST_CONNECTION_LAST_SUCCESS_TRY_TIME: 0,
                HOST_CONNECTION_LAST_TRY_TIME: 0
            }
            self._connection_host_status[host] = host_connection_status

        last_success_time = host_connection_status[
            HOST_CONNECTION_LAST_SUCCESS_TRY_TIME
        ]
        last_time = host_connection_status[
            HOST_CONNECTION_LAST_TRY_TIME
        ]

        # raise HostConnectionNotAllowedException if we tried to establish
        # connection in last 'host_connection_reconnect_delay' and got
        # failure
        if (last_time != last_success_time and
                cur_time - last_time <
                self._host_connection_reconnect_delay):
            raise pika_drv_exc.HostConnectionNotAllowedException(
                "Connection to host #{} is not allowed now because of "
                "previous failure".format(host_index)
            )

        try:
            connection = self._do_create_host_connection(
                host, for_listening
            )
            self._connection_host_status[host][
                HOST_CONNECTION_LAST_SUCCESS_TRY_TIME
            ] = cur_time

            return connection
        finally:
            self._connection_host_status[host][
                HOST_CONNECTION_LAST_TRY_TIME
            ] = cur_time

    def _do_create_host_connection(self, host, for_listening):
        connection_params = pika.ConnectionParameters(
            host=host.hostname,
            port=host.port,
            credentials=pika_credentials.PlainCredentials(
                host.username, host.password
            ),
            heartbeat_interval=(
                self._heartbeat_interval if for_listening else None
            ),
            **self._common_pika_params
        )
        if for_listening:
            connection = pika_connection.ThreadSafePikaConnection(
                parameters=connection_params
            )
        else:
            connection = pika.BlockingConnection(
                parameters=connection_params
            )
            connection.params = connection_params

        self._set_tcp_user_timeout(connection._impl.socket)
        return connection


class NotClosableConnection(object):
    def __init__(self, connection):
        self._connection = connection

    def __getattr__(self, item):
        return getattr(self._connection, item)

    def close(self):
        pass


class SinglePikaConnectionFactory(PikaConnectionFactory):
    def __init__(self, url, conf):
        super(SinglePikaConnectionFactory, self).__init__(url, conf)
        self._connection = None

    def create_connection(self, for_listening=False):
        with self._connection_lock:
            if self._connection is None or not self._connection.is_open:
                self._connection = (
                    super(SinglePikaConnectionFactory, self).create_connection(
                        True
                    )
                )
            return NotClosableConnection(self._connection)

    def cleanup(self):
        with self._connection_lock:
            if self._connection is not None and self._connection.is_open:
                try:
                    self._connection.close()
                except Exception:
                    LOG.warning(
                        "Unexpected exception during connection closing",
                        exc_info=True
                    )
            self._connection = None


class ReadWritePikaConnectionFactory(PikaConnectionFactory):
    def __init__(self, url, conf):
        super(ReadWritePikaConnectionFactory, self).__init__(url, conf)
        self._read_connection = None
        self._write_connection = None

    def create_connection(self, for_listening=False):
        with self._connection_lock:
            if for_listening:
                if (self._read_connection is None or
                        not self._read_connection.is_open):
                    self._read_connection = super(
                        ReadWritePikaConnectionFactory, self
                    ).create_connection(True)
                return NotClosableConnection(self._read_connection)
            else:
                if (self._write_connection is None or
                        not self._write_connection.is_open):
                    self._write_connection = super(
                        ReadWritePikaConnectionFactory, self
                    ).create_connection(True)
                return NotClosableConnection(self._write_connection)

    def cleanup(self):
        with self._connection_lock:
            if (self._read_connection is not None and
                    self._read_connection.is_open):
                try:
                    self._read_connection.close()
                except Exception:
                    LOG.warning(
                        "Unexpected exception during connection closing",
                        exc_info=True
                    )
            self._read_connection = None

            if (self._write_connection is not None and
                    self._write_connection.is_open):
                try:
                    self._write_connection.close()
                except Exception:
                    LOG.warning(
                        "Unexpected exception during connection closing",
                        exc_info=True
                    )
            self._write_connection = None
