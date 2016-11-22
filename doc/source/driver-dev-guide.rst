---------------------------------------
Guide for Transport Driver Implementors
---------------------------------------

.. currentmodule:: oslo_messaging

.. automodule:: oslo_messaging._drivers.base

============
Introduction
============

This document is a *best practices* guide for the developer interested
in creating a new transport driver for Oslo.Messaging.  It should also
be used by maintainers as a reference for proper driver behavior.
This document will describe the driver interface and prescribe the
expected behavior of any driver implemented to this interface.

**Note well:** The API described in this document is internal to the
oslo.messaging library and therefore **private**.  Under no
circumstances should this API be referenced by code external to the
oslo.messaging library.

================
Driver Interface
================

The driver interface is defined by a set of abstract base classes. The
developer creates a driver by defining concrete classes from these
bases.  The derived classes embody the logic that is specific for the
messaging back-end that is to be supported.

These base classes are defined in the *base.py* file in the *_drivers*
subdirectory.

===============
IncomingMessage
===============

.. autoclass:: IncomingMessage
   :members:

==================
RpcIncomingMessage
==================

.. autoclass:: RpcIncomingMessage
   :members:

========
Listener
========

.. autoclass:: Listener
   :members:

=================
PollStyleListener
=================

.. autoclass:: PollStyleListener
   :members:

==========
BaseDriver
==========

.. autoclass:: BaseDriver
   :members:






