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

        OtelManager.reset_current_fail_count()
        if is_local_testing():
            with patch("dtagent.otel.otel_manager.CustomLoggingSession.send") as mock_otel, patch(
                "dtagent.otel.metrics.requests.post"
            ) as mock_metrics, patch("dtagent.otel.events.requests.post") as mock_events, patch(
                "dtagent.otel.bizevents.requests.post"
            ) as mock_bizevents, patch(
                "snowflake.snowpark.Session.sql"
            ) as mock_sql:
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

                return super().process(sources, run_proc)
        else:
            return super().process(sources, run_proc)


def _overwrite_plugin_local_config_key(test_conf: TestConfiguration, plugin_name: str, key_name: str, new_value: Any):
    # added to make sure we always run tests for each mode in users plugin
    test_conf._config["plugins"][plugin_name][key_name] = new_value
    return test_conf


def side_effect_function(*args, **kwargs):
    from unittest.mock import MagicMock
    from dtagent.otel.bizevents import BizEvents
    from dtagent.otel.events import Events
    from dtagent.otel.logs import Logs
    from dtagent.otel.metrics import Metrics
    from dtagent.otel.spans import Spans

    mock_response = MagicMock()

    if args[0].endswith(BizEvents.ENDPOINT_PATH) or args[0].endswith(Metrics.ENDPOINT_PATH):  # For BizEvents and Metrics
        mock_response.status_code = 202

    if args[0].endswith(Events.ENDPOINT_PATH):  # For events
        mock_response.status_code = 201

    if args[0].endswith(Logs.ENDPOINT_PATH) or args[0].endswith(Spans.ENDPOINT_PATH):  # For logs and spans
        mock_response.status_code = 200

    return mock_response
