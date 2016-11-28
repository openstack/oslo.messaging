##############################
Oslo.messaging AMQP 1.0 Driver
##############################
:Date: $Date: 2016-08-02 $
:Revision: $Revision: 0.04 $

Introduction
============

This document describes the architecture and implementation of the
oslo.messaging AMQP 1.0 driver. The AMQP 1.0 driver provides an
implementation of the oslo.messaging base driver service interfaces
that map client application RPC and Notify methods "onto" the
operation of an AMQP 1.0 protocol messaging bus. The blueprint for the original
driver can be found here [1]_ and the original implementation is described in
[2]_. The feature specification for the updates to the AMQP 1.0 driver for the
OpenStack Newton release can be found here [3]_

The driver effectively hides the details of the AMQP 1.0 protocol transport and
message processing from the client applications. The Pyngus messaging
framework [4]_ built on the QPID Proton engine [5]_ provides a
callback-based API for message passing. The driver implementation is
comprised of the callback "handlers" that drive the messaging APIs to
connect to the message bus, subscribe servers, send and receive messages.

::

   +------------+ +------------+ +-------------+ +-------------+
   |            | |            | |             | |             |  OpenStack
   | RPC Client | | RPC Server | |    Notify   | |    Notify   |  Application
   |            | |            | |    Client   | |    Server   |
   +------------+ +------------+ +-------------+ +-------------+
   XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
   +-----------------------------------------------------------+
   |             Oslo.Messaging "Base Driver Interface"        |  Oslo Messaging
   +-----------------------------------------------------------+  Driver
   |               Oslo.Messaging AMQP 1.0 Driver              |
   +-----------------------------------------------------------+
   XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
   +-----------------------------------------------------------+
   |                 Pyngus Messaging Framework                |
   +-----------------------------------------------------------+
   |                     QPID Proton Library                   |  AMQP 1.0
   +-----------------------------------------------------------+  Protocol
   |                      AMQP 1.0 Protocol                    |  Exchange
   +-----------------------------------------------------------+
   |                     TCP/IP Network Layer                  |
   +-----------------------------------------------------------+


Development View
================

Code Base
---------

The AMQP 1.0 driver source code is maintained in the OpenStack
oslo.messaging repository [7]_. The driver implementation, tests and
user guide are located in the sub-directories of the repository.

::

   ├── doc
   │   └── source
   │       ├── AMQP1.0.rst
   ├── oslo_messaging
       ├── _drivers
       │   ├── amqp1_driver
       │   │   ├── addressing.py
       │   │   ├── controller.py
       │   │   ├── eventloop.py
       │   │   ├── opts.py
       │   ├── impl_amqp1.py
       ├── tests
           ├── drivers
               ├── test_amqp_driver.py




+-----------------+----------------------------------------------------+
|File             |                   Content                          |
+=================+====================================================+
|doc/             |The AMQP 1.0 driver user guide details              |
|source/          |prerequisite, configuration and platform deployment |
|AMQP1.0.rst      |considerations.                                     |
|                 |                                                    |
+-----------------+----------------------------------------------------+
|_drivers/        |This file provides the oslo.messaging driver entry  |
|impl_amqp1.py    |points for the AMQP 1.0 driver. The file provides   |
|                 |implementations for the base.RpcIncomingMessage,    |
|                 |base.PollStyleListener and base.BaseDriver oslo     |
|                 |messaging entities.                                 |
+-----------------+----------------------------------------------------+
|_drivers/        |This file provides a set of utilities that translate|
|amqp1_driver/    |a target address to a well-formed AMQP 1.0 address. |
|addressing.py    |                                                    |
|                 |                                                    |
+-----------------+----------------------------------------------------+
|_drivers/        |The controller manages the interface between the    |
|amqp1_driver/    |driver and the messaging service protocol exchange. |
|controller.py    |                                                    |
|                 |                                                    |
+-----------------+----------------------------------------------------+
|_drivers/        |This module provides a background thread that       |
|amqp1_driver/    |handles scheduled messaging operations. All         |
|eventloop.py     |protocol specific exchanges are executed on this    |
|                 |background thread.                                  |
+-----------------+----------------------------------------------------+
|_drivers/        |This file manages the AMQP 1.0 driver configuration |
|amqp1_driver/    |options (oslo_messaging_amqp).                      |
|opts.py          |                                                    |
|                 |                                                    |
+-----------------+----------------------------------------------------+
|tests/           |This file contains a set of functional tests that   |
|drivers/         |target the capabilities of the driver. A message    |
|test_amqp_driver |intermediary is included to emulate the full        |
|                 |messaging protocol exchanges.                       |
+-----------------+----------------------------------------------------+

Deployment
==========

The Oslo Messaging AMQP 1.0 driver is deployed on each node of the
OpenStack infrastructure where one or more OpenStack services will be deployed.

::

 Node                                      Node
 +--------------------------------+        +-----------------------------------+
 |                +-------------+ |        | +--------------+ +--------------+ |
 |                |             | |        | |              | |              | |
 |                | OpenStack   | |        | |   OpenStack  | |   OpenStack  | |
 |                |  Service    | |        | |    Service   | |    Service   | |
 |                |             | |        | |              | |              | |
 |                +-------------+ |        | +--------------+ +--------------+ |
 |                |   Oslo      | |        | |   Oslo       | |   Oslo       | |
 |                |  Messaging  | |        | |  Messaging   | |  Messaging   | |
 | +------------+ +-------------+ |        | +--------------+ +--------------+ |
 | | AMQP 1.0   | |  AMQP 1.0   | |        | |  AMQP 1.0    | |  AMQP 1.0    | |
 | |Intermediary| |   Driver    | |        | |   Driver     | |   Driver     | |
 | +------------+ +-------------+ |        | +--------------+ +--------------+ |
 | +----------------------------+ |        | +-------------------------------+ |
 | |           TCP/IP           | |        | |          TCP/IP               | |
 | |           Stack            | |        | |          Stack                | |
 | +----------------------------+ |        | +-------------------------------+ |
 +--------------------------------+        +-----------------------------------+
            ^           ^                           ^             ^
            |           |                           |             |
            |           |     Public Network        |             |
 +----------------------v-----------------------------------------v------------+
            v                 Internal Network      v
 +-----------------------------------------------------------------------------+

The configuration of each OpenStack service must provide the
transport information that indicates to the oslo messaging layer that the AMQP
1.0 driver is to be instantiated for the back-end. During instantiation of
the driver, a connection is established from the driver to an AMQP 1.0
intermediary that provides the messaging bus capabilities. The
intermediary can be co-located on nodes that are running OpenStack
services or can be located on separate stand-alone nodes in the
control plane.

The driver architecture is intended to support any messaging
intermediary (e.g. broker or router) that implements version 1.0 of the
AMQP protocol. Support for additional classes of intermediaries might
require changes to driver configuration parameters and addressing syntax
but should not otherwise require changes to the driver architecture.

Driver Structure
================

The functionality of the AMQP 1.0 driver is implemented across a number of
components that encapsulate the mapping of the driver activities onto the AMQP
protocol exchange. The *Controller* implements the primary functional logic for
the driver and serves as the interface between the driver entry points ( *Proton
Driver* ) and the I/O operations associated with sending and receiving messages
on links attached to the message bus. Each sending or receiving link is
associated with a specific driver activity such as sending an RPC Call/Cast or
Notify message, receiving an RPC reply message, or receiving an RPC or Notify
server request.

::

                  _______________________
                 /                      /
                /    Application       /
               /     (OpenStack)      /
              /______________________/
                         |
  XXXXXXXXXXXXXXXXXXXXXXX|XXXXXXXXXXXXXXXXXXXXXXXXXXXXX
                         |
                   +----------+
       +-----------|  Proton  |
       V           |  Driver  |
   +-------+       +----------+
   | Tasks |             |
   +-------+      +------------+
       +--------->| Controller |
              ----|            |----
             /    +------------+    \
            /            |           \
           /             |            \
      +---------+   +---------+     +---------+
      |  Sender |<--| Replies |     | Server  |
      |         |   |         |     |         |
      +---------+   +---------+     +---------+
           |             |               |
           |        +---------+     +---------+
           |        | Proton  |     | Proton  |
           |        |Listener |     |Listener |
           |        +---------+     +---------+
           |             |               |
  XXXXXXXXX|XXXXXXXXXXXXX|XXXXXXXXXXXXXXX|XXXXXXXXXXXXX
           |             |               |
      +--------+     +--------+      +--------+
      |  Send  |     | Receive|      | Receive|
      |  Link  |     |  Link  |      |  Link  |
      +--------+     +--------+      +--------+


Task Orchestration
------------------

The AMQP 1.0 driver maintains a thread for processing protocol events
and timers. Therefore, the driver must orchestrate and synchronize
requests from the client applications with this internal thread. The
*Proton Driver* will act as a proxy for each client request and
constructs a task request object on the caller's thread via the
*Controller*. The task request object contains the necessary information
to execute the desired method on the driver invocation thread of
control. This method is executed synchronously - the client thread
pends until the driver thread completes processing the task. The unique
task objects provided for driver thread invocation include:

* Subscribe Task
* Send Task (for RPC Cast or Notify)
* RPC Call Task
* RPC Reply Task
* Message Disposition Task

::

    +------------------------+             +-------------------------------+
    |     Client Thread      |             |        Driver Thread          |
    | +--------+ +---------+ |             | +------+ +--------+ +-------+ |
    | |Proton  | |Control  | |             | |Event | |Control | |Pyngus | |
    | |Driver  | |(-ler)   | |             | |Loop  | |(-ler)  | |Frmwrk | |
    | +---+----+ +----+----+ |             | +---+--+ +---+----+ +---+---+ |
    |     |create     |      |             |     |        |          |     |
    |     |task()     |      |             |     |        |          |     |
    |     |---------->|      |             |     |        |          |     |
    |     |add        |      |             |     |        |          |     |
    |     |task()     |      |   Request   |     |        |          |     |
    |     |---------->|      |    Queue    |     |        |          |     |
    |     |           | enq  |   +------+  | deq |        |          |     |
    |     |           |------|---> |||||+--|---->| exec() |          |     |
    |     |           |      |   +------+  |     |------->|          |     |
    |     |           |      |             |     |        |----------|-+   |
    |     | wait()    |      |             |     |        | Protocol | |   |
    |     #-----------|------|------+      |     |        | Exchange | |   |
    |     #           |      |      V      |     |        |          | |   |
    |     #           |      |   +-----+   |     | set()  |<---------|-+   |
    |     #           |      |   |Event|<--------|--------|          |     |
    |     #           |      |   |     |   |     |        |          |     |
    |     #           |      |   +-----+   |     |        |          |     |
    |     #           |      |      |      |     |        |          |     |
    |     #<----------|------|------+      |     |        |          |     |
    |     |           |      |             |     |        |          |     |
    |     +           +      |             |     +        +          +     |
    |                        |             |                               |
    |                        |             |                               |
    +------------------------+             +-------------------------------+


Scheduling - Execution
^^^^^^^^^^^^^^^^^^^^^^

Following the method task construction, the task is added to the *Controller*
queue of requests for execution. Following the placement of the task on this
queue, the caller will wait for the execution to complete (or possibly timeout
or raise an exception).

The eventloop running in its own thread will dequeue the task request and invoke
the corresponding method on the *Controller* servant using the
information stored in the task request object retrieved. The calls
executed on this eventloop thread via the *Controller* perform all the
protocol specific intelligence required for the pyngus framework. In
addition to the target method invocation, the eventloop may call on
the request object for message communication state changes or other
indications from the peer.

::

                                                                         Request
           +--------------------------------------------+   +----------+ Tasks
           |Client Thread                            /\ |   |          |
           |     *  *           *  *           *  * / v |   |        + V +
  listen() |  *        *     *        *     *        *  |   |        |---|
  -------->| *  Init    *-->* Schedule *-->*   Wait   * |   |        |---|
           | *          *   *          *   *          * |   |        |---|
           |  *        *     *        *     *        *  |   |        +_|_+
           |     *  *           *  *\          *  *     |   |          V
           |                         +------------------|-->|   +--------------+
           +--------------------------------------------+   |   |  Eventloop   |
                                                            |   |     *  *     |
           +--------------------------------------------+   |   |  *        *  |
           |Client Thread                            /\ |   |   | *  Execute * |
           |     *  *           *  *           *  * / v |   |   | *          * |
  call()   |  *        *     *        *     *        *  |   |   |  *        *  |
  -------->| *  Init    *-->* Schedule *-->*   Wait   * |   |   |   ^ *  * \   |
           | *          *   *          *   *          * |   |   |  /        \  |
           |  *        *     *        *     *        *  |   |   | /         /  |
           |     *  *           *  *\          *  *     |   |   |  \       /   |
           |                         +------------------|-->|   |   \ *  *v    |
           +--------------------------------------------+   |   |  *        *  |
                                 o                          |   | * Protocol * |
                                 o                          |   | * Exchange * |
                                 o                          |   |  *        *  |
           +--------------------------------------------+   |   |     *  *     |
           |Client Thread                            /\ |   |   +--------------+
           |     *  *           *  *           *  * / v |   |
  cast()   |  *        *     *        *     *        *  |   |
  -------->| *  Init    *-->* Schedule *-->*   Wait   * |   |
           | *          *   *          *   *          * |   |
           |  *        *     *        *     *        *  |   |
           |     *  *           *  *\          *  *     |   |
           |                         +------------------|-->
           +--------------------------------------------+


Completion
^^^^^^^^^^

After carrying out the messaging protocol exchange for the requested
task or upon a timeout/exception condition, the eventloop thread will
wake-up the callers thread to indicate the task completion.

Use Scenarios
=============

The primary use scenarios for the AMQP 1.0 Driver correspond to the activities
supported by the oslo messaging base driver interface. These activities include
the ability to subscribe RPC and Notify servers (referred to as
"Servers" in the graphics) as well the ability to send RPC (cast and
call) messages and Notification messages into the control plane
infrastructure. Following RPC and Notify server processing
(e.g. dispatch to the application) the ability to indicate the final
disposition of the message is supported and mapped onto the message
delivery and settlement capabilities of the AMQP messaging bus. The
composition of the AMQP driver and its dynamic behaviors is defined by
the support of these primary activities.

Load Driver
-----------

The operational life-cycle of the AMQP 1.0 driver begins when the oslo messaging
loads and instantiates the driver instance for use by an application. To
complete this activity, the driver will retrieve the oslo_messaging_amqp
configuration options in order to define the driver's run time behaviors. The
transport URL specifier provided will be used by the driver to create a
connection to the AMQP 1.0 messaging bus. The transport URL is of the form

   amqp://user:pass@host1:port[,hostN:portN]

Where the transport scheme specifies **amqp** as the back-end. It
should be noted that oslo.messaging is deprecating the discrete host,
port and auth configuration options [6]_.

The driver provides the capability to transform the "Target" provided
by an application to an addressing format that can be associated to the
sender and receive links that take part in the AMQP protocol exchange.

::

   load()---+
             \                   -----------
              \              +--- Transport
               >  *  *       |   -----------
               *        *<---+
              *  Prepare *
              *  Driver  *
               *        *
                  *  *
  ----------        |
  Cfg Opts          |
  ----------\       |
             \      v
              v   *  *
               *        *
              * Retrieve *
              *  Config  *
               *        *
                  *  *
                    |
                    |
                    v
                  *  *
               *  Start *
              * Protocol *
              *  Thread  *
               *        *
                  *  *
                    |
                    |
                    v
                  *  *                 +--------------+
               * Connect*              |    AMQP      |
              *    to    *<----------->|  Protocol    |
              *  Message *             |  Exchange    |
               *   Bus  *              +--------------+
                  *  * \
                    |   \
                    |    \       ------------
                    v     +-----> Connection --+
                  *  *           ------------  |
               *        *                      |
              *  Address *<--------------------+
              *  Factory *
               *        *
                  *  *

When the AMQP 1.0 driver connects to the messaging bus, it will
identify the intermediary that it is connected to (e.g. broker or
router). Based on the intermediary type, the driver will dynamically
select an addressing syntax that is optimal for operation in a router
mesh or a syntax that is appropriate for broker backed queues or topics.


Subscribe Server
----------------

The AMQP 1.0 driver maintains a set of (RPC or Notification) servers that are
created via the subscribe server activity. For each server, the driver will
create and attach a set of addresses for the target that corresponds to the
server endpoint for an AMQP protocol exchange. A unique *ProtonListener* (e.g.
AMQP 1.0 Receiver Link) is instantiated for each server subscription and the
driver will attach event handlers to perform message transport
performatives for the link. The driver maintains a single incoming
queue that messages from all attached links will be placed upon.

::

 listen()
       +
        \
         \        *  *
          \    *        *
           +> *  Create  *
              *  Listener*
               *        *
                  *  *  \         ----------
   --------         |    +-------> Incoming
    Target -+       |           / ----------
   --------  \      |     +----+
              \     v    /
               v  *  *  v
               *        *
              *  Create  *
              *  Server  *
               *        *\
                  *  *    \
  ----------        |      \        -----------
  Connection        |       +------> Addresses
  ----------\       |              /-----------
             \      v             /
              v   *  *           /
               *        *<------+
              *  Attach  *
              *  Links   *
               *        *
                  *  *
                    |
                    |
                    v
             +--------------+
             |    AMQP      |
             |  Protocol    |
             |  Exchange    |
             +--------------+


Send Message
------------

The AMQP 1.0 driver provides the ability to send messages (e.g. RPC Call/Cast
or Notify) to a target specified by a client application. The driver
maintains a cache of senders corresponding to each unique target that is
referenced across the driver life-cycle. The driver maintains a single
receiver link that will be the incoming link for all RPC reply
messages received by the driver. Prior to sending an RPC call message
that expects a reply, the driver will allocate a unique correlation
identifier for inclusion in the call message. The driver will also set
the message's reply-to field to the address of the RPC reply
link. This correlation identifier will appear in the RPC reply message
and is used to deliver the reply to the proper client.

Prior to sending the message, the AMQP 1.0 driver will determine if the sender
link is active and has enough credits for the transfer to proceed. If
there are not enough credits to send the message, the driver will
retain the pending message until it can be sent or times out. If there are
credits to send a message, the driver will first check if there are
any messages from a previous request pending to be sent. The driver
will service these pending requests in FIFO order and may defer
sending the current message request if credits to send run out.

The AMQP 1.0 driver tracks the settlement status of all request
messages sent to the messaging bus. For each message sent, the driver
will maintain a count of the number of retry attempts made on the
message. The driver will re-send a message that is not acknowledged up
until the retry limit is reached or a send timeout deadline is reached.

::

   send()
       +                          --------
        \                     +--- Target
         \        *  *       |    --------
          \    *        *<---+
           +> *  Prepare *
              *  Request *---+     -------------
              /*        *    +----> Request Msg <-----+
             /    *  *             -------------      |
  ------- <-+       |                                 |
  Sender            |                                 |
  -------           |                                 |
                    v                                 |
                  *  *              ------------      |
               *        *--------->  Correlation      |
              * Prepare  *          ------------      |
              * Response *                            |
               *        *                             |
                  *  *                                |
                    |                                 |
                    |                                 |
                    v                ---------        |
                  *  *    +---------> Pending         |
               *        */           ---------        |
              *  Send    *                            |
              *  Message *\       ---------           |
               *        *  +-----> Unacked <---+      |
                  *  *            ---------    |      |
                    |                          |      |
                    |                          |      +
                    v                          |     /
             +--------------+                *  *   v
             |    AMQP      |              *       *
             |  Protocol    |-----------> * Settle  *
             |  Exchange    |             * Message *
             +--------------+              *       *
                                             *  *

Server Receive
--------------

The AMQP 1.0 driver (via subscribe)  maintains a groups of links that
receive messages from a set of addresses derived from the Targets
associated with a Server instantiation. Messages arriving from these
links are placed on the Listener's incoming queue via the Server's
incoming message handler. The Listener's poll method will  return the
message to the application for subsequent application service dispatching.

::

             +--------------+
             |    AMQP      |
             |  Protocol    |
             |  Exchange    |
             +--------------+
                    |       ^
   --------         V       |       ---------
   Receiver-+     *  *      +------- Address
   --------  \  *       *           ---------
              v* Message *
               * Received*
                *       *
                  *  *   \
                          \        -----------------
                           +------> Incoming Message --+
                  *  *             -----------------   |
                *       *                              |
               *  Poll   *<--+                         |
               *         *   |                         |
                *       *    |                         |
                  *  *       +-------------------------+



RPC Reply Receive
-----------------

The AMQP 1.0 driver instantiates a single receiving link for the
reception of all RPC reply messages. Messages received on this
receiving link are routed to the originating caller using the
correlation-id embedded in the header of the  message itself. To
ensure the responsiveness and throughput on the shared RPC receiving
link, the AMQP 1.0 driver will immediately update the link transfer
credits and will acknowledge the successful receipt of the RPC reply.

::

             +--------------+
             |    AMQP      |
             |  Protocol    |
             |  Exchange    |
             +--------------+
                    |              -----------------
                    V      + ------ Incoming Message
                  *  *    /        -----------------
                *       *v
               * Message *
               * Received*<---+
                *       *     |
                  *  * \      |     -------------
                    |   \     +----  Correlation
                    V    \          -------------
                  *  *    \
                *       *  \         ---------------
               * Update  *  +------>  Reply Message
               * Credit  *           ---------------
                *       *
                  *  *
                    |
                    V
                   *  *
                *       *
               * Accept  *
               * Message *
                *       *
                  *  *
                    |
                    V
             +--------------+
             |    AMQP      |
             |  Protocol    |
             |  Exchange    |
             +--------------+


Disposition
-----------

For each incoming message provided by the AMQP 1.0 driver to a server
application (e.g. RPC or Notify), the delivery disposition of the
incoming message can be indicated to the driver. The disposition can
either be to acknowledge the message indicating the message was
accepted by the application or to requeue the message indicating that
application processing could not successfully take place. The driver
will initiate the appropriate settlement of the message through an
AMQP protocol exchange over the message bus.

::

   acknowledge()--------+                   requeue() --------+
                        |                                     |
                        v                                     v
                       *  *                                  *  *
                    *        *                            *        *
                   *   Ack    *                          * Requeue  *
                   *  Message *\                     ----* Message  *
                    *        *  \                   /     *        *
                       *  *      \                 /         *  *
                        |         v ------------- v           |
                        |            Incoming Msg             |
                        |         / -------------             |
                        |        /                            |
                        v       v                             |
                 +--------------+                             |
                 |    AMQP      |<----------------------------+
                 |  Protocol    |
                 |  Exchange    |
                 +--------------+


Driver Components
=================

This section describes the components of the AMQP 1.0 driver
implementation. For each component, its primary responsibilities and
the relationships to other components are included. These
relationships are derived from service requests placed upon the other
components. Architectural or system-level constraints on the component
(e.g. multiplicity, concurrency, parameterization) that change the
depiction of the architecture are included. Additionally, any list of issues
waiting resolution are described.

Controller
----------
+-----------------+----------------------------------------------------+
|Component        | *Controller*                                       |
+=================+====================================================+
|Responsibilities | Responsible for performing messaging-related       |
|                 | operations requested by the driver (tasks)         |
|                 | and for managing the connection to the messaging   |
|                 | service provided by the AMQP 1.0 intermediaries.   |
|                 |                                                    |
|                 | This component provides the logic for addressing,  |
|                 | sending and receiving messages as well as managing |
|                 | the messaging bus connection life-cycle.           |
+-----------------+----------------------------------------------------+
|Collaborators    |                                                    |
|                 | Sender (pyngus.SenderEventHandler)                 |
|                 | Server (pyngus.ReceiverEventHandler)               |
|                 | Replies (pyngus.ReceiverEventHandler)              |
+-----------------+----------------------------------------------------+
|Notes            | The component is dynamically created and destroyed.|
|                 | It is created whenever the driver is instantiated  |
|                 | in a client application process. The component     |
|                 | will terminate the driver operation when the client|
|                 | initiates a shutdown of the driver.                |
|                 |                                                    |
|                 | All AMQP 1.0 protocol exchanges (e.g. messaging    |
|                 | and I/O work) are done on the Eventloop driver     |
|                 | thread. This allows the driver to run              |
|                 | asynchronously from the messaging clients.         |
|                 |                                                    |
|                 | The component supports addressing modes defined    |
|                 | by the driver configuration and through dynamic    |
|                 | inspection of the connection to the messaging      |
|                 | intermediary.                                      |
+-----------------+----------------------------------------------------+
|Issues           | A cache of sender links indexed by address is      |
|                 | maintained. Currently, removal from the cache is   |
|                 | is not implemented.                                |
+-----------------+----------------------------------------------------+

Sender
------
+-----------------+----------------------------------------------------+
|Component        | *Sender* (pyngus.SenderEventHander)                |
+=================+====================================================+
|Responsibilities | Responsible for managing a sender link life-cycle  |
|                 | and queueing/tracking the message delivery.        |
|                 | (implementation of Pyngus.SenderEventHandle)       |
|                 |                                                    |
|                 | Provides the capabilities for sending to a         |
|                 | particular address on the message bus.             |
|                 |                                                    |
|                 | Provides the capability to queue (pending)         |
|                 | *SendTask* when link not active or insufficient    |
|                 | link credit capacity.                              |
|                 |                                                    |
|                 | Provides the capability to retry send following a  |
|                 | recoverable connection or link failure.            |
+-----------------+----------------------------------------------------+
|Collaborators    |                                                    |
|                 | Addresser                                          |
|                 | Connection                                         |
|                 | Pyngus.SenderLink                                  |
|                 | SendTask                                           |
+-----------------+----------------------------------------------------+
|Notes            | The component is dynamically created and destroyed.|
|                 | It is created by the *Controller* on a client      |
|                 | caller thread and retained in a *Sender* cache.    |
+-----------------+----------------------------------------------------+
|Issues           | Sender cache aging (see above)                     |
+-----------------+----------------------------------------------------+

Server
------
+-----------------+----------------------------------------------------+
|Component        | *Server* (pyngus.ReceiverEventHander)              |
+=================+====================================================+
|Responsibilities | Responsible for operations for the lifecycle of an |
|                 | incoming queue that is used for messages received  |
|                 | from a set of target addresses.                    |
|                 |                                                    |
+-----------------+----------------------------------------------------+
|Collaborators    | Connection                                         |
|                 | Pyngus.ReceiverLink                                |
+-----------------+----------------------------------------------------+
|Notes            | The component is dynamically created and destroyed.|
|                 | It is created whenever a client application        |
|                 | subscribes a RPC or Notification server to the     |
|                 | messaging bus. When the client application closes  |
|                 | the transport, this component and its associated   |
|                 | links will be detached/closed.                     |
|                 |                                                    |
|                 | Individual receiver links are created over the     |
|                 | message bus connection for all the addresses       |
|                 | generated for the server target.                   |
|                 |                                                    |
|                 | All the receiver links share a single event        |
|                 | callback handler.                                  |
+-----------------+----------------------------------------------------+
|Issues           | The credit per link is presently hard-coded. A     |
|                 | mechanism to monitor for a back-up of inbound      |
|                 | messages to back-pressure the sender is proposed.  |
+-----------------+----------------------------------------------------+

Replies
-------
+-----------------+----------------------------------------------------+
|Component        | *Replies* (pyngus.ReceiverEventHander)             |
+=================+====================================================+
|Responsibilities | Responsible for the operations and managing        |
|                 | the life-cycle of the receiver link for all RPC    |
|                 | reply messages. A single instance of an RPC reply  |
|                 | link is maintained for the driver.                 |
+-----------------+----------------------------------------------------+
|Collaborators    | Connection                                         |
|                 | Pyngus.ReceiverLink                                |
+-----------------+----------------------------------------------------+
|Notes            | The component is dynamically created and destroyed.|
|                 | The reply link is created when the connection to   |
|                 | the messaging bus is activated.                    |
|                 |                                                    |
|                 | The origination of RPC calls is inhibited until    |
|                 | the replies link is active.                        |
|                 |                                                    |
|                 | Message are routed to the originator's incoming    |
|                 | queue using the correlation-id header that is      |
|                 | contained in the response message.                 |
+-----------------+----------------------------------------------------+
|Issues           |                                                    |
+-----------------+----------------------------------------------------+

ProtonDriver
------------
+-----------------+----------------------------------------------------+
|Component        | *ProtonDriver*                                     |
+=================+====================================================+
|Responsibilities | Responsible for providing the oslo.Messaging       |
|                 | BaseDriver implementation.                         |
|                 |                                                    |
|                 | Provides the capabilities to send RPC and          |
|                 | Notification messages and create subscriptions for |
|                 | the application.                                   |
|                 |                                                    |
|                 | Each operation generates a task that is scheduled  |
|                 | for execution on the *Controller* eventloop        |
|                 | thread.                                            |
|                 |                                                    |
|                 | The calling thread blocks until execution completes|
|                 | or timeout.                                        |
+-----------------+----------------------------------------------------+
|Collaborators    |                                                    |
|                 | Controller                                         |
|                 | RPCCallTask                                        |
|                 | SendTask                                           |
|                 | SubscribeTask                                      |
+-----------------+----------------------------------------------------+
|Notes            | The component is dynamically created and destroyed.|
|                 | It is created whenever the oslo.messaging AMQP 1.0 |
|                 | driver is loaded by an application (process).      |
|                 |                                                    |
|                 | The component manages the life-cycle of the        |
|                 | *Controller* component. Tasks may be created but   |
|                 | will not be processed until the Controller         |
|                 | connection to the messaging service completes.     |
|                 |                                                    |
|                 | There are separate timeout values for RPC Send,    |
|                 | Notify Send, and RPC Call Reply.                   |
+-----------------+----------------------------------------------------+
|Issues           |                                                    |
|                 | The unmarshalling of an RPC response could cause   |
|                 | an exception/failure and should be optimally       |
|                 | communicated back up to the caller.                |
+-----------------+----------------------------------------------------+

ProtonIncomingMessage
---------------------
+-----------------+----------------------------------------------------+
|Component        | *ProtonIncomingMessage*                            |
+=================+====================================================+
|Responsibilities | Responsible for managing the life-cycle of an      |
|                 | incoming message received on a RPC or notification |
|                 | Server link.                                       |
|                 |                                                    |
|                 | Provides the capability to set the disposition of  |
|                 | the incoming message as acknowledge (e.g. settled) |
|                 | or requeue.                                        |
|                 |                                                    |
|                 | Provides the capability to marshal and send the    |
|                 | reply to an RPC Call message.                      |
|                 |                                                    |
+-----------------+----------------------------------------------------+
|Collaborators    | Controller                                         |
|                 | ProtonListener                                     |
|                 | MessageDispositionTask                              |
|                 | SendTask                                           |
|                 |                                                    |
+-----------------+----------------------------------------------------+
|Notes            | The component is dynamically created and destroyed.|
|                 | A ProtonListener returns this component from the   |
|                 | poll of the incoming queue.                        |
|                 |                                                    |
|                 | The message reply_to and id fields of the incoming |
|                 | message are used to generate the target for the    |
|                 | RPC reply message.                                 |
|                 |                                                    |
|                 | The RPC reply and message disposition operations   |
|                 | are scheduled for execution on the Controller      |
|                 | eventoloop thread. The caller on the component is  |
|                 | blocked until task completion (or timeout).        |
+-----------------+----------------------------------------------------+
|Issues           | The ProtonIncomingMessage is used for both RPC     |
|                 | and Notification Server instances. Conceptually,   |
|                 | a Notification Server should not schedule a reply  |
|                 | and a  RPC Server should not schedule a message    |
|                 | requeue. Subclassing base.IncomingMessage for      |
|                 | Notifications and base.RpcIncomingMessage for RPC  |
|                 | could be a consideration.                          |
+-----------------+----------------------------------------------------+

ProtonListener
--------------
+-----------------+----------------------------------------------------+
|Component        | *ProtonListener*                                   |
+=================+====================================================+
|Responsibilities | Responsible for providing the oslo.Messaging       |
|                 | base.PollStyleListener implementation.             |
|                 |                                                    |
|                 | Provides the capabilities to manage the queue of   |
|                 | incoming messages received from the messaging links|
|                 |                                                    |
|                 | Returns instance of ProtonIncomingMessage to       |
|                 | to Servers                                         |
+-----------------+----------------------------------------------------+
|Collaborators    |                                                    |
|                 | Queue                                              |
+-----------------+----------------------------------------------------+
|Notes            | The component is dynamically created and destroyed.|
|                 | An instance is created for each subscription       |
|                 | request (e.g. RPC or Notification Server).         |
|                 |                                                    |
|                 | The Controller maintains a map of Servers indexed  |
|                 | by each specific ProtonListener identifier (target)|
+-----------------+----------------------------------------------------+
|Issues           |                                                    |
+-----------------+----------------------------------------------------+

SubscribeTask
-------------
+-----------------+----------------------------------------------------+
|Component        | *SubscribeTask*                                    |
+=================+====================================================+
|Responsibilities | Responsible for orchestrating a subscription to a  |
|                 | given target.                                      |
|                 |                                                    |
|                 | Provides the capability to prepare and schedule    |
|                 | the subscription call on the Controller eventloop  |
|                 | thread.                                            |
+-----------------+----------------------------------------------------+
|Collaborators    |                                                    |
|                 | Controller                                         |
+-----------------+----------------------------------------------------+
|Notes            | The component is dynamically created and destroyed.|
|                 | It is created for each ProtonDriver subscription   |
|                 | request (e.g. listen or listen_for_notifications). |
|                 |                                                    |
|                 | The task is prepared and scheduled on the caller's |
|                 | thread. The subscribe operation is executed on the |
|                 | Controller's eventloop thread. The task completes  |
|                 | once the subscription has been established on the  |
|                 | message bus.                                       |
+-----------------+----------------------------------------------------+
|Issues           |                                                    |
+-----------------+----------------------------------------------------+

SendTask
--------
+-----------------+----------------------------------------------------+
|Component        | *SendTask*                                         |
+=================+====================================================+
|Responsibilities | Responsible for sending a message to a given       |
|                 | target.                                            |
|                 |                                                    |
|                 | Provides the capability to prepare and schedule    |
|                 | the send call on the Controller eventloop thread.  |
|                 |                                                    |
|                 | Provides the ability to be called by Controller    |
|                 | eventloop thread to indicate the settlement of the |
|                 | message (e.g. acknowledge or nack).                |
|                 |                                                    |
|                 | Provides the ability to be called by Controller    |
|                 | eventloop thread upon expiry of send timeout       |
|                 | duration or general message delivery failure.      |
+-----------------+----------------------------------------------------+
|Collaborators    |                                                    |
|                 | Controller                                         |
+-----------------+----------------------------------------------------+
|Notes            | The component is dynamically created and destroyed.|
|                 | It is created for each ProtonDriver "RPC Cast" or  |
|                 | "Notify" send request. The component is destroyed  |
|                 | when the message transfer has reached a terminal   |
|                 | state (e.g. settled).                              |
|                 |                                                    |
|                 | The task is prepared and scheduled on the caller's |
|                 | thread. The send operation is executed on the      |
|                 | Controller's eventloop thread.                     |
|                 |                                                    |
|                 | All retry, timeout and acknowledge operations are  |
|                 | performed on Controller eventloop thread and       |
|                 | indicated back to the caller thread.               |
+-----------------+----------------------------------------------------+
|Issues           |                                                    |
+-----------------+----------------------------------------------------+

RPCCallTask
-----------
+-----------------+----------------------------------------------------+
|Component        | *RPCCallTask*                                      |
+=================+====================================================+
|Responsibilities | Responsible for sending an RPC Call message to a   |
|                 | given target.                                      |
|                 |                                                    |
|                 | Provides all the capabilities derived from the     |
|                 | parent SendTask component.                         |
|                 |                                                    |
|                 | Provides the additional capability to prepare for  |
|                 | the RPC Call response message that will be returned|
|                 | on the senders reply link.                         |
+-----------------+----------------------------------------------------+
|Collaborators    |                                                    |
|                 | Controller                                         |
|                 | Sender                                             |
+-----------------+----------------------------------------------------+
|Notes            | The component is dynamically created and destroyed.|
|                 | It is created for each ProtonDriver "RPC Call"     |
|                 | send request. It is destroyed once the RPC         |
|                 | exchanged has reached its terminal state.          |
|                 |                                                    |
|                 | The task is prepared and scheduled on the caller's |
|                 | thread. The send operation is executed on the      |
|                 | Controller's eventloop thread.                     |
|                 |                                                    |
|                 | The Controller manages a single receiving link for |
|                 | all RPC reply messages. Message are routed         |
|                 | using the correlation-id header in the response    |
|                 | message.                                           |
+-----------------+----------------------------------------------------+
|Issues           |                                                    |
+-----------------+----------------------------------------------------+

MessageDispositionTasks
-----------------------
+-----------------+----------------------------------------------------+
|Component        | *MessageDispositionTask*                           |
+=================+====================================================+
|Responsibilities | Responsible for updating the message disposition   |
|                 | for ProtonIncomingMessage.                         |
|                 |                                                    |
|                 | Provides the ability to acknowledge or requeue the |
|                 | message according to application determination.    |
+-----------------+----------------------------------------------------+
|Collaborators    |                                                    |
|                 | Controller                                         |
|                 | ProtonIncomingMessage                              |
|                 | Server                                             |
+-----------------+----------------------------------------------------+
|Notes            | The component is dynamically created and destroyed.|
|                 | It is created by ProtonIncomingMessage settlement  |
|                 | calls (acknowledge or requeue). It is destroyed    |
|                 | once the disposition is updated in the Proton      |
|                 | protocol engine.                                   |
|                 |                                                    |
|                 | the task is prepared and scheduled on the caller's |
|                 | thread. The disposition operation is a function    |
|                 | closure on the target server, receiver link and    |
|                 | delivery handle for the message received on the    |
|                 | Server receiver call back. The closure is executed |
|                 | on the Controller's eventloop thread.              |
|                 |                                                    |
|                 | The settlement of RPC responses is automatic and   |
|                 | not under application control.                     |
+-----------------+----------------------------------------------------+
|Issues           |                                                    |
+-----------------+----------------------------------------------------+

Service and Operational Qualities
=================================

This section describes the primary service and operational qualities
that are relevant to the driver architecture and implementation. These
non-functional factors define the behavior of the driver implementation
(e.g. limits and capacities). These behaviors can be generally
categorized as being due to a design time (e.g. limit enforced by
implementation) or a run time (e.g. limit due to environment,
resources, etc.) constraint. The full detail and measures for these
qualities is outside the scope of this document but should be included
in any performance and scalability analysis of the driver implementation.

+-------------+--------------------------------------------+------------+
|  Quality    |              Description                   |    Limit   |
+-------------+--------------------------------------------+------------+
| Servers     | The number of RPC or Notify servers that   | Environment|
|             | the driver will concurrently subscribe to  |            |
|             | the messaging bus (e.g. Listeners)         |            |
+-------------+--------------------------------------------+------------+
| Subscription| The maximum rate at which servers can be   | Environment|
| Rate        | subscribed and attached to the message bus |            |
+-------------+--------------------------------------------+------------+
| Senders     | The number of unique Targets that can      | Environment|
|             | be concurrently defined for the destination|            |
|             | of RPC or Notify message transfer          |            |
+-------------+--------------------------------------------+------------+
| Pending     | The number of messages that the driver     | Environment|
| Sends       | will queue while waiting for link          |            |
|             | availability or flow credit                |            |
+-------------+--------------------------------------------+------------+
| Sends       | The number of concurrent unacked messages  | Environment|
| Outstanding | the driver will send                       |            |
|             |                                            |            |
+-------------+--------------------------------------------+------------+
| Server Link | The number of message credits an RPC or    | Design     |
| Credits     | Notification server will issue             |            |
|             |                                            |            |
+-------------+--------------------------------------------+------------+
| RPC Reply   | The number of RPC reply message credits    | Design     |
| Link Credits| the driver will issue                      |            |
|             |                                            |            |
+-------------+--------------------------------------------+------------+
| Message     | The rate that the driver will transfer     | Environment|
| Transfer    | requests to the message bus                |            |
| Rate        |                                            |            |
+-------------+--------------------------------------------+------------+
| Message     | The rate of transfer for the message       | Environment|
| Data        | body "payload"                             |            |
| Throughput  |                                            |            |
+-------------+--------------------------------------------+------------+
| Tasks       | The number of concurrent client requests   | Design     |
| Outstanding | that can be queued for driver thread       |            |
|             | processing.                                |            |
+-------------+--------------------------------------------+------------+
| Message     | The number of attempts the driver will     | Design     |
| Retries     | make to send a message                     |            |
|             |                                            |            |
+-------------+--------------------------------------------+------------+
| Transport   | The number of Transport Hosts that can     | Environment|
| Hosts       | be specified for connection management     |            |
|             | (e.g. selection and failover)              |            |
+-------------+--------------------------------------------+------------+

References
==========

.. [1] https://blueprints.launchpad.net/oslo.messaging/+spec/amqp10-driver-implementation
.. [2] https://git.openstack.org/cgit/openstack/oslo-specs/tree/specs/juno/amqp10-driver-implementation.rst
.. [3] https://review.openstack.org/#/c/314603/
.. [4] https://github.com/kgiusti/pyngus
.. [5] https://github.com/apache/qpid-proton
.. [6] https://review.openstack.org/#/c/317285/
.. [7] https://git.openstack.org/openstack/oslo.messaging
