---------
Transport
---------

.. currentmodule:: oslo.messaging

.. autofunction:: get_transport

.. autoclass:: Transport

.. autoclass:: TransportURL
   :members:

.. autoclass:: TransportHost

.. autofunction:: set_transport_defaults


About fork oslo.messaging transport object
------------------------------------------

oslo.messaging can't ensure that forking a process that shares the same
transport object is safe for the library consumer, because it relies on
different 3rd party libraries that don't ensure that too, but in certain
case/driver it works:

* rabbit: works only if no connection have already been established.
* qpid: doesn't work (qpid library have a global state that use fd
  that can't be resetted)
* amqp1: works
