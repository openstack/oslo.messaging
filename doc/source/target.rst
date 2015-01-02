------
Target
------

.. currentmodule:: oslo_messaging

.. autoclass:: Target

===============
Target Versions
===============

Target version numbers take the form Major.Minor. For a given message with
version X.Y, the server must be marked as able to handle messages of version
A.B, where A == X and  B >= Y.

The Major version number should be incremented for an almost completely new
API. The Minor version number would be incremented for backwards compatible
changes to an existing API.  A backwards compatible change could be something
like adding a new method, adding an argument to an existing method (but not
requiring it), or changing the type for an existing argument (but still
handling the old type as well).

If no version is specified it defaults to '1.0'.

In the case of RPC, if you wish to allow your server interfaces to evolve such
that clients do not need to be updated in lockstep with the server, you should
take care to implement the server changes in a backwards compatible and have
the clients specify which interface version they require for each method.

Adding a new method to an endpoint is a backwards compatible change and the
version attribute of the endpoint's target should be bumped from X.Y to X.Y+1.
On the client side, the new RPC invocation should have a specific version
specified to indicate the minimum API version that must be implemented for the
method to be supported.  For example::

    def get_host_uptime(self, ctxt, host):
        cctxt = self.client.prepare(server=host, version='1.1')
        return cctxt.call(ctxt, 'get_host_uptime')

In this case, version '1.1' is the first version that supported the
get_host_uptime() method.

Adding a new parameter to an RPC method can be made backwards compatible. The
endpoint version on the server side should be bumped. The implementation of
the method must not expect the parameter to be present.::

    def some_remote_method(self, arg1, arg2, newarg=None):
        # The code needs to deal with newarg=None for cases
        # where an older client sends a message without it.
        pass

On the client side, the same changes should be made as in example 1. The
minimum version that supports the new parameter should be specified.
