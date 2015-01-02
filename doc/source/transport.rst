---------
Transport
---------

.. currentmodule:: oslo_messaging

.. autofunction:: get_transport

.. autoclass:: Transport

.. autoclass:: TransportURL
   :members:

.. autoclass:: TransportHost

.. autofunction:: set_transport_defaults


Forking Processes and oslo.messaging Transport objects
------------------------------------------------------

oslo.messaging can't ensure that forking a process that shares the same
transport object is safe for the library consumer, because it relies on
different 3rd party libraries that don't ensure that. In certain
cases, with some drivers, it does work:

* rabbit: works only if no connection have already been established.
* qpid: doesn't work (The qpid library has a global state that uses
  file descriptors that can't be reset)
* amqp1: works
