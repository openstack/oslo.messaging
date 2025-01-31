============================
Oslo Messaging Simulator
============================

This guide explains how to set up and run the oslo messaging simulator for testing
different messaging scenarios.

Prerequisites
-------------
* Python 3.x
* virtualenv
* wget (for Kafka scenarios)

Environment Setup
-----------------
This assumes you have git cloned the oslo.messaging repository and are in the root
directory of the repository.

Create and activate a virtual environment::

    python -m venv .venv
    source .venv/bin/activate

Install required packages::

    pip install pifpaf
    pip install -e .

Running the Simulator
---------------------
The simulator supports different scenarios for testing messaging patterns. Below
are the common usage patterns.

Basic Setup
^^^^^^^^^^^
Before running the simulator, set up the messaging environment::

    ./tools/setup-scenario-env.sh

Available Scenarios
^^^^^^^^^^^^^^^^^^^
The simulator supports two main scenarios:

Scenario 01 (RabbitMQ only)
***************************
This scenario uses RabbitMQ for both RPC and notifications::

    export SCENARIO=scenario01
    ./tools/setup-scenario-env.sh

Scenario 02 (RabbitMQ + Kafka)
******************************
This scenario uses RabbitMQ for RPC and Kafka for notifications::

    export SCENARIO=scenario02
    ./tools/setup-scenario-env.sh

Running the Simulator
^^^^^^^^^^^^^^^^^^^^^

RPC Server Example
******************
To start the RPC server::

    python tools/simulator.py --url rabbit://pifpaf:secret@127.0.0.1:5682/ rpc-server

RPC Client Example
******************
To start the RPC client::

    python tools/simulator.py --url rabbit://pifpaf:secret@127.0.0.1:5682/ rpc-client --exit-wait 15000 -p 64 -m 64

Optional Configuration
----------------------
You can generate a sample configuration file using oslo-config-generator::

    oslo-config-generator --namespace oslo.messaging > oslo.messaging.conf

For reference on all available configuration options, visit:
https://docs.openstack.org/oslo.messaging/latest/configuration/opts.html

To use a configuration file with the simulator, use the --config-file option::

    python tools/simulator.py --config-file oslo.messaging.conf [other options]

Command Line Options
--------------------
The simulator supports various command line options:

--url URL
    The transport URL for the messaging service
--config-file PATH
    Path to a configuration file
-d, --debug
    Enable debug mode
-p PROCESSES
    Number of processes (for client)
-m MESSAGES
    Number of messages (for client)
--exit-wait MILLISECONDS
    Wait time before exit (for client)

Cleanup
-------
To clean up the environment, you can terminate the running processes::

    pkill -f "RABBITMQ"

Notes
-----
* The default scenario is scenario01 if not specified
* Kafka setup is automatic when using scenario02
* The simulator uses pifpaf to manage the message broker processes
* Installing with ``pip install -e .`` allows for development mode installation
* Configuration options can be referenced in the official documentation
