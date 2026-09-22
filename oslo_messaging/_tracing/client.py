# Copyright 2026 Red Hat, Inc.
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

import contextlib

from oslo_config import cfg
from oslo_log import log as logging


LOG = logging.getLogger(__name__)

oslo_messaging_tracing = [
    cfg.BoolOpt(
        'tracing_enabled',
        default=False,
        help='Enable OpenTelemetry tracing for RPC calls.',
    ),
    cfg.StrOpt(
        'tracing_service_name',
        default='openstack',
        help='Service name reported in trace spans.',
    ),
    cfg.StrOpt(
        'otel_exporter_otlp_endpoint',
        default='http://localhost:4318',
        help='OTLP exporter endpoint URL. For HTTP/protobuf '
        'protocol, traces are sent to '
        '<endpoint>/v1/traces.',
    ),
    cfg.StrOpt(
        'otlp_protocol',
        default='http/protobuf',
        choices=[
            ('http/protobuf', 'OTLP over HTTP with Protocol Buffers'),
            ('grpc', 'OTLP over gRPC'),
        ],
        help='OTLP transport protocol.',
    ),
    cfg.BoolOpt(
        'insecure',
        default=False,
        help='Disable TLS verification for the OTLP '
        'endpoint. Only relevant when using the '
        'gRPC protocol.',
    ),
]
cfg.CONF.register_opts(oslo_messaging_tracing, group='oslo_messaging_tracing')


_TRACER = None


def _create_exporter(conf):
    """Create the OTLP span exporter based on configuration.

    The protocol-specific exporter package is imported here rather
    than at module level so that tracing-http and tracing-grpc extras
    are fully independent.
    """
    protocol = conf.oslo_messaging_tracing.otlp_protocol
    endpoint = conf.oslo_messaging_tracing.otel_exporter_otlp_endpoint
    insecure = conf.oslo_messaging_tracing.insecure

    match protocol:
        case 'http/protobuf':
            try:
                import opentelemetry.exporter.otlp.proto.http as otel_http
            except ImportError:
                raise RuntimeError(
                    'OTLP HTTP exporter is not installed. '
                    'Install it with: '
                    'pip install oslo.messaging[tracing-http]'
                )

            return otel_http.trace_exporter.OTLPSpanExporter(
                endpoint=endpoint + '/v1/traces',
            )
        case 'grpc':
            try:
                import opentelemetry.exporter.otlp.proto.grpc as otel_grpc
            except ImportError:
                raise RuntimeError(
                    'OTLP gRPC exporter is not installed. '
                    'Install it with: '
                    'pip install oslo.messaging[tracing-grpc]'
                )

            return otel_grpc.trace_exporter.OTLPSpanExporter(
                endpoint=endpoint,
                insecure=insecure,
            )
        case _:
            raise RuntimeError(f'invalid protocol: {protocol}')


def _get_tracer(conf):
    global _TRACER
    if _TRACER is not None:
        return _TRACER

    from opentelemetry import trace as otel_trace

    provider = otel_trace.get_tracer_provider()

    # If a TracerProvider was already configured (e.g. by
    # oslo.middleware's WSGI tracing), reuse it. Otherwise
    # set up our own provider with the configured exporter.
    if isinstance(provider, otel_trace.ProxyTracerProvider):
        from opentelemetry.sdk import resources as otel_resources
        from opentelemetry.sdk import trace as otel_sdk_trace
        from opentelemetry.sdk.trace import export as otel_export

        resource = otel_resources.Resource.create(
            {
                "service.name": (
                    conf.oslo_messaging_tracing.tracing_service_name
                )
            }
        )
        provider = otel_sdk_trace.TracerProvider(resource=resource)
        exporter = _create_exporter(conf)
        provider.add_span_processor(otel_export.BatchSpanProcessor(exporter))
        otel_trace.set_tracer_provider(provider)

    _TRACER = otel_trace.get_tracer("oslo.messaging")
    return _TRACER


@contextlib.contextmanager
def trace_send(conf, target, method, call_type, msg_ctxt):
    if not conf.oslo_messaging_tracing.tracing_enabled:
        yield
        return

    try:
        tracer = _get_tracer(conf)

        from opentelemetry import propagate as otel_propagate
        from opentelemetry import trace as otel_trace
    except Exception:
        LOG.warning(
            "Failed to set up tracing for send, skipping", exc_info=True
        )
        yield
        return

    with tracer.start_as_current_span(
        f"{target.topic}.{method}",
        kind=otel_trace.SpanKind.PRODUCER,
    ) as span:
        span.set_attribute("messaging.system", "rabbitmq")
        span.set_attribute("messaging.operation", call_type)
        span.set_attribute("messaging.destination.name", target.topic)
        span.set_attribute("rpc.method", method)
        if target.namespace:
            span.set_attribute("rpc.service", target.namespace)
        if msg_ctxt.get("request_id"):
            span.set_attribute("openstack.request_id", msg_ctxt["request_id"])

        otel_propagate.inject(msg_ctxt)

        try:
            yield
        except Exception as ex:
            span.set_status(otel_trace.StatusCode.ERROR, str(ex))
            span.record_exception(ex)
            raise


@contextlib.contextmanager
def trace_receive(conf, message):
    if not conf.oslo_messaging_tracing.tracing_enabled:
        yield
        return

    try:
        tracer = _get_tracer(conf)

        from opentelemetry import propagate as otel_propagate
        from opentelemetry import trace as otel_trace

        ctxt = message.ctxt
        method = message.message.get('method', 'unknown')

        parent_ctx = otel_propagate.extract(ctxt)
    except Exception:
        LOG.warning(
            "Failed to set up tracing for receive, skipping", exc_info=True
        )
        yield
        return

    with tracer.start_as_current_span(
        f"{method} receive",
        context=parent_ctx,
        kind=otel_trace.SpanKind.CONSUMER,
    ) as span:
        span.set_attribute("messaging.system", "rabbitmq")
        span.set_attribute("messaging.operation", "receive")
        span.set_attribute("rpc.method", method)
        if message.msg_id:
            span.set_attribute("messaging.message.id", message.msg_id)
        if ctxt.get("request_id"):
            span.set_attribute("openstack.request_id", ctxt["request_id"])

        try:
            yield
        except Exception as ex:
            span.set_status(otel_trace.StatusCode.ERROR, str(ex))
            span.record_exception(ex)
            raise
