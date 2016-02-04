=============================
 Supported Messaging Drivers
=============================

RabbitMQ may not be sufficient for the entire community as the community
grows. Pluggability is still something we should maintain, but we should
have a very high standard for drivers that are shipped and documented
as being supported.

This document defines a very clear policy as to the requirements
for drivers to be carried in oslo.messaging and thus supported by the
OpenStack community as a whole. We will deprecate any drivers that do not
meet the requirements, and announce said deprecations in any appropriate
channels to give users time to signal their needs. Deprecation will last
for two release cycles before removing the code. We will also review and
update documentation to annotate which drivers are supported and which
are deprecated given these policies

Policy
------

Testing
~~~~~~~

* Must have unit and/or functional test coverage of at least 60% as
  reported by coverage report. Unit tests must be run for all versions
  of python oslo.messaging currently gates on.

* Must have integration testing including at least 3 popular oslo.messaging
  dependents, preferably at the minimum a devstack-gate job with Nova,
  Cinder, and Neutron.

* All testing above must be voting in the gate of oslo.messaging.

Documentation
~~~~~~~~~~~~~

* Must have a reasonable amount of documentation including documentation
  in the official OpenStack deployment guide.

Support
~~~~~~~

* Must have at least two individuals from the community committed to
  triaging and fixing bugs, and responding to test failures in a timely
  manner.

Prospective Drivers
~~~~~~~~~~~~~~~~~~~

* Drivers that intend to meet the requirements above, but that do not yet
  meet them will be given one full release cycle, or 6 months, whichever
  is longer, to comply before being marked for deprecation. Their use,
  however, will not be supported by the community. This will prevent a
  chicken and egg problem for new drivers.

.. note::

  This work is licensed under a Creative Commons Attribution 3.0 Unported License.
  http://creativecommons.org/licenses/by/3.0/legalcode
