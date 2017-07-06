=========
Executors
=========

Executors control how a received message is scheduled for processing
by a Server.  This scheduling can be *synchronous* or *asynchronous*.

A synchronous executor will process the message on the Server's
thread.  This means the Server can process only one message at a time.
Other incoming messages will not be processed until the current
message is done processing.  For example, in the case of an RPCServer
only one method call will be invoked at a time.  A synchronous
executor guarantees that messages complete processing in the order
that they are received.

An asynchronous executor will process received messages concurrently.
The Server thread will not be blocked by message processing and can
continue to service incoming messages.  There are no ordering
guarantees - message processing may complete in a different order than
they were received.  The executor may be configured to limit the
maximum number of messages that are processed at once.


Available Executors
===================

.. list-plugins:: oslo.messaging.executors
   :detailed:
