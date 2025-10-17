#
#
# Copyright (c) 2025 Dynatrace Open Source
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
#
import json
import os
from contextlib import contextmanager
import re
from typing import Any, Dict, List
import functools
import datetime
from unittest.mock import patch, Mock

from snowflake import snowpark

from _snowflake import read_secret
from dtagent.agent import DynatraceSnowAgent
from dtagent.config import Configuration

read_secret(
    secret_name="dtagent_token",
    from_field="_DTAGENT_API_KEY",
    from_file="conf/config-test.json",
    env_name="DTAGENT_TOKEN",
)

# Global variable to store the current test source for telemetry mocking
_current_test_source = [None]


@functools.lru_cache(maxsize=1)
def _get_credentials() -> Dict:
    """
    {
        "account": "<your snowflake account>",
        "user": "<your snowflake user>",
        "password": "<your snowflake password>",
        "role": "<your snowflake role>",  # Optional
        "warehouse": "<your snowflake warehouse>",  # Optional
        "schema": "<your snowflake schema>"  # Optional
    }
    """
    credentials = {}
    credentials_path = "test/credentials.json"
    if os.path.isfile(credentials_path):
        with open(credentials_path, "r", encoding="utf-8") as f:
            credentials = json.loads(f.read())

    return credentials or {"local_testing": True}


def is_local_testing() -> bool:
    return _get_credentials().get("local_testing", False)


def _get_session() -> snowpark.Session:
    # Import the Session class from the snowflake.snowpark package
    from snowflake.snowpark import Session

    credentials = _get_credentials()
    session = Session.builder.configs(credentials).create()
    if "warehouse" in credentials:
        session.use_warehouse(credentials["warehouse"])

    return session


class TestConfiguration(Configuration):

    def get_last_measurement_update(self, session: snowpark.Session, source: str):
        from dtagent.util import _get_timestamp_in_sec

        return _get_timestamp_in_sec()


class TestDynatraceSnowAgent(DynatraceSnowAgent):

    def __init__(self, session: snowpark.Session, config: Configuration) -> None:
        self._local_configuration = config
        super().__init__(session)

    def _get_config(self, session: snowpark.Session) -> Configuration:
        return _overwrite_plugin_local_config_key(
            self._local_configuration,
            "users",
            "roles_monitoring_mode",
            ["DIRECT_ROLES", "ALL_ROLES", "ALL_PRIVILEGES"],
        )

    def process(
        self,
        sources: List,
        run_proc: bool = True,
    ) -> Dict:
        from dtagent.otel.otel_manager import OtelManager

        process_results = {}

        OtelManager.reset_current_fail_count()
        if is_local_testing():
            _current_test_source[0] = sources[0] if sources else None
            with mock_telemetry_sending():
                process_results = super().process(sources, run_proc)
                self._logs.shutdown_logger()
                self._spans.shutdown_tracer()
        else:
            process_results = super().process(sources, run_proc)

        return process_results


def _overwrite_plugin_local_config_key(test_conf: TestConfiguration, plugin_name: str, key_name: str, new_value: Any):
    # added to make sure we always run tests for each mode in users plugin
    test_conf._config["plugins"][plugin_name][key_name] = new_value
    return test_conf


@contextmanager
def mock_telemetry_sending():
    with (
        patch("dtagent.otel.otel_manager.CustomLoggingSession.send") as mock_otel,
        patch("dtagent.otel.metrics.requests.post") as mock_metrics,
        patch("dtagent.otel.events.requests.post") as mock_events,
        patch("dtagent.otel.bizevents.requests.post") as mock_bizevents,
        patch("snowflake.snowpark.Session.sql") as mock_sql,
    ):
        # Set up HTTP mocks
        mock_otel.side_effect = side_effect_function
        mock_metrics.side_effect = side_effect_function
        mock_events.side_effect = side_effect_function
        mock_bizevents.side_effect = side_effect_function

        # Set up session.sql mock to prevent actual Snowflake calls
        current_time = datetime.datetime.now(datetime.timezone.utc)
        one_hour_ago = current_time - datetime.timedelta(hours=1)
        mock_sql_instance = Mock()
        mock_row = Mock()
        mock_row.__getitem__ = Mock(return_value=one_hour_ago)
        mock_sql_instance.collect.return_value = [mock_row]
        mock_sql.return_value = mock_sql_instance

        yield


def decode_object_from_protobuf(protobuf_bytes: bytes) -> str:
    """
    Decode protobuf logs to extract key-value pairs from log bodies.

    Args:
        protobuf_bytes: The binary protobuf data from OpenTelemetry logs

    Returns:
        A dictionary representing the extracted key-value pairs.
    """
    from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceRequest

    request = ExportLogsServiceRequest()
    request.ParseFromString(protobuf_bytes)

    kv_data = {}

    for resource_log in getattr(request, "resource_logs"):
        for scope_log in resource_log.scope_logs:
            for log_record in scope_log.log_records:
                # Extract from body.kvlist_value.values
                if log_record.body.HasField("kvlist_value"):
                    for kv_pair in log_record.body.kvlist_value.values:
                        key = kv_pair.key
                        # Get the actual value based on the type
                        if kv_pair.value.HasField("string_value"):
                            value = kv_pair.value.string_value
                        elif kv_pair.value.HasField("int_value"):
                            value = kv_pair.value.int_value
                        elif kv_pair.value.HasField("double_value"):
                            value = kv_pair.value.double_value
                        elif kv_pair.value.HasField("bool_value"):
                            value = kv_pair.value.bool_value
                        else:
                            value = str(kv_pair.value)  # fallback

                        kv_data[key] = value

    return kv_data


def side_effect_function(*args, **kwargs):
    import inspect
    from unittest.mock import MagicMock
    from dtagent.otel.bizevents import BizEvents
    from dtagent.otel.events import Events
    from dtagent.otel.logs import Logs
    from dtagent.otel.metrics import Metrics
    from dtagent.otel.spans import Spans

    # Use the global test source instead of inspecting the stack
    source_context = _current_test_source[0]

    # Handle both requests.post mocks (args[0] is url) and CustomLoggingSession.send mocks (args[0] is request)
    if hasattr(args[0], "url"):
        # CustomLoggingSession.send: args[0] is request object
        url = args[0].url
        data = args[0].body
    else:
        # requests.post: args[0] is url string
        url = args[0]
        data = kwargs.get("data")

    # Determine telemetry_type based on url and set up mock response
    mock_response = MagicMock()
    telemetry_type = None

    if url.endswith(BizEvents.ENDPOINT_PATH):
        telemetry_type = "biz_events"
        mock_response.status_code = 202
        # we skip self-monitoring entries saving
        if (isinstance(data, list) and any(item.get("data", {}).get("dsoa.run.context") == "self-monitoring" for item in data)) or (
            isinstance(data, str) and ' "dsoa.run.context": "self-monitoring"' in data
        ):
            data = None
    elif url.endswith(Events.ENDPOINT_PATH):
        telemetry_type = "events"
        mock_response.status_code = 201
    elif url.endswith(Logs.ENDPOINT_PATH):
        telemetry_type = "logs"
        mock_response.status_code = 200
    elif url.endswith(Spans.ENDPOINT_PATH):
        telemetry_type = "spans"
        mock_response.status_code = 200
    elif url.endswith(Metrics.ENDPOINT_PATH):
        telemetry_type = "metrics"
        mock_response.status_code = 202

    if data and telemetry_type and source_context:
        ext = "txt" if telemetry_type == "metrics" else "json"
        filepath = f"test/test_results/{source_context}/{telemetry_type}.{ext}"
        content = None

        if not os.path.exists(filepath):
            os.makedirs(os.path.dirname(filepath), exist_ok=True)

            if isinstance(data, (dict, str)):
                content = data
            elif isinstance(data, bytes):
                if telemetry_type in ("logs", "spans"):
                    content = decode_object_from_protobuf(data)
            else:
                content = str(data)

            if content is not None:
                with open(filepath, "w", encoding="utf-8") as f:
                    if ext == "json":
                        try:
                            json_data = json.loads(content) if isinstance(content, str) else content
                            json.dump(json_data, f, indent=2)
                        except (json.JSONDecodeError, TypeError):
                            f.write(content)
                    else:
                        f.write(content)

    return mock_response
