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
import os
import sys
import datetime
from typing import Any, Generator, Dict, List, Optional, Callable, Tuple
import logging
import json
import fnmatch
import jsonstrip
from contextlib import contextmanager
from unittest.mock import patch, Mock
from snowflake import snowpark
from dtagent.config import Configuration
from dtagent.connector import TelemetrySender
from dtagent import config
from dtagent.util import is_select_for_table
from test import is_local_testing

TEST_CONFIG_FILE_NAME = "./test/conf/config-download.json"


def side_effect_function(*args, **kwargs):
    import inspect
    from unittest.mock import MagicMock
    from dtagent.otel.bizevents import BizEvents
    from dtagent.otel.events import Events
    from dtagent.otel.logs import Logs
    from dtagent.otel.metrics import Metrics
    from dtagent.otel.spans import Spans

    # Inspect the call stack to find the 'source' variable from DynatraceSnowAgent.process()
    source_context = None
    frame = inspect.currentframe()
    while frame:
        if "source" in frame.f_locals and isinstance(frame.f_locals["source"], str) and frame.f_locals["source"].startswith("test_"):
            source_context = frame.f_locals["source"]
            break
        frame = frame.f_back

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

        if not os.path.exists(filepath):
            os.makedirs(os.path.dirname(filepath), exist_ok=True)

            if isinstance(data, (dict, str)):
                content = data
            elif isinstance(data, bytes):
                content = data.decode("utf-8")
            else:
                content = str(data)

            with open(filepath, "w", encoding="utf-8") as f:
                if ext == "json":
                    # Parse and pretty-print as JSON
                    try:
                        json_data = json.loads(content) if isinstance(content, str) else content
                        json.dump(json_data, f, indent=2)
                    except (json.JSONDecodeError, TypeError):
                        f.write(content)
                else:
                    f.write(content)

    return mock_response


def _pickle_all(session: snowpark.Session, pickles: dict, force: bool = False):
    """
    Pickle all tables provided in the pickles dictionary if necessary or forced.

    Args:
        session (snowpark.Session): The Snowflake session used to access tables.
        pickles (dict): A dictionary mapping table names to pickle file names.
        force (bool, optional): If True, force pickling even if not necessary. Defaults to False.

    Returns:
        None
    """
    if force or should_pickle(pickles.values()):
        for table_name, pickle_name in pickles.items():
            _pickle_data_history(session, table_name, pickle_name)


def _pickle_data_history(
    session: snowpark.Session, t_data: str, pickle_name: str, operation: Optional[Callable] = None
) -> Generator[Dict, None, None]:
    if is_select_for_table(t_data):
        import pandas as pd

        df_data = session.sql(t_data).collect()
        pd_data = pd.DataFrame(df_data)
    else:
        df_data = session.table(t_data)
        if operation:
            df_data = operation(df_data)
        pd_data = df_data.to_pandas()

    pd_data.to_pickle(pickle_name)
    print("Pickled " + str(pickle_name))


def _logging_findings(
    session: snowpark.Session,
    dtagent,
    log_tag: str,
    log_level: logging,
    show_detailed_logs: bool,
):

    if log_level != "":
        logging.basicConfig(level=log_level)
    if show_detailed_logs:
        from dtagent import LOG, LL_TRACE

        console_handler = logging.StreamHandler()  # Console handler
        LOG.addHandler(console_handler)
        LOG.setLevel(LL_TRACE)
        console_handler.setLevel(LL_TRACE)

        print(LOG.getEffectiveLevel())

    results = dtagent.process([str(log_tag)], False)

    print(f"!!!! RESULTS = {results}")

    dtagent.teardown()
    session.close()


def _safe_get_unpickled_entries(pickles: dict, table_name: str, *args, **kwargs) -> Generator[Dict, None, None]:
    """
    Safely get unpickled entries for the given table name from the pickles dictionary.

    Args:
        pickles (dict): Dictionary mapping table names to pickle file paths.
        table_name (str): The name of the table to retrieve unpickled entries for.
        *args: Additional positional arguments passed to the underlying unpickling function.
        **kwargs: Additional keyword arguments passed to the underlying unpickling function.

    Returns:
        Generator[Dict, None, None]: A generator yielding dictionaries representing unpickled entries for the specified table.

    Raises:
        ValueError: If the table name is not found in the pickles dictionary.
    """
    if table_name not in pickles:
        raise ValueError(f"Unknown table name: {table_name}")
    return _get_unpickled_entries(pickles[table_name], *args, **kwargs)


def _get_unpickled_entries(
    pickle_name: str,
    limit: int = None,
    adjust_ts: bool = True,
    start_time: str = "START_TIME",
    end_time: str = "END_TIME",
) -> Generator[Dict, None, None]:
    import pandas as pd

    pandas_df = pd.read_pickle(pickle_name)

    print(f"Unpickled {pickle_name}")
    #####
    ndjson_name = os.path.splitext(pickle_name)[0] + ".ndjson"
    collected_rows = []

    if limit is not None:
        if 0 < len(pandas_df) < limit:
            n_repeats = limit // len(pandas_df)
            is_remainder = limit % len(pandas_df) > 0

            dfs_to_concat = [pandas_df] * (n_repeats + (1 if is_remainder else 0))

            # Concatenate them and reset the index
            pandas_df = pd.concat(dfs_to_concat, ignore_index=True)

        pandas_df = pandas_df.head(limit)

    for _, row in pandas_df.iterrows():
        from dtagent.util import _adjust_timestamp

        row_dict = row.to_dict()
        if adjust_ts:
            _adjust_timestamp(row_dict, start_time=start_time, end_time=end_time)

        collected_rows.append(row_dict)
        yield row_dict

    if not os.path.exists(ndjson_name):
        with open(ndjson_name, "w", encoding="utf-8") as f:
            for row in collected_rows:
                f.write(json.dumps(row) + "\n")


def should_pickle(pickle_files: list) -> bool:

    return (len(sys.argv) > 1 and sys.argv[1] == "-p") or any(not os.path.exists(file_name) for file_name in pickle_files)


class TestConfiguration(Configuration):

    def __init__(self, configuration: dict):  # pylint: disable=W0231
        self._config = configuration


def _merge_pickles_from_tests() -> Dict[str, str]:
    """Merges all PICKLES dictionaries from test_*.py files in the plugins directory into a single dictionary.

    Returns:
        Dict: A dictionary containing all merged PICKLES dictionaries,
        mapping all table names to their corresponding pickle file paths.
    """
    import importlib
    import inspect

    pickles = {}
    plugins_dir = os.path.join(os.path.dirname(__file__), "plugins")
    for filename in os.listdir(plugins_dir):
        if filename.startswith("test_") and filename.endswith(".py"):
            module_name = f"test.plugins.{filename[:-3]}"
            try:
                module = importlib.import_module(module_name)
                for _, member in inspect.getmembers(module):
                    if inspect.isclass(member) and hasattr(member, "PICKLES"):
                        pickles.update(member.PICKLES)
            except ImportError as e:
                print(f"Could not import {module_name}: {e}")
    return pickles


class LocalTelemetrySender(TelemetrySender):
    PICKLES = _merge_pickles_from_tests()

    def __init__(self, session: snowpark.Session, params: dict, limit_results: int = 2, config: TestConfiguration = None):

        self._local_config = config
        self.limit_results = limit_results

        TelemetrySender.__init__(self, session, params)

        self._configuration.get_last_measurement_update = lambda *args, **kwargs: datetime.datetime.fromtimestamp(
            0, tz=datetime.timezone.utc
        )

    def _get_config(self, session: snowpark.Session) -> Configuration:
        return self._local_config if self._local_config else TelemetrySender._get_config(self, session)

    def _get_table_rows(self, t_data: str) -> Generator[Dict, None, None]:
        if t_data in self.PICKLES:
            return _get_unpickled_entries(self.PICKLES[t_data], limit=self.limit_results)

        return TelemetrySender._get_table_rows(self, t_data)

    def _flush_logs(self) -> None:
        self._logs._otel_logger_provider.force_flush()


@contextmanager
def mock_telemetry_sending():
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

        yield


def telemetry_test_sender(
    session: snowpark.Session, sources: str, params: dict, limit_results: int = 2, config: TestConfiguration = None
) -> Tuple[int, int, int, int, int]:
    """
    Invokes send_data function on a LocalTelemetrySender instance, which uses pickled data for testing purposes
    Returns:
        Tuple[int, int, int, int]: Count of objects, log lines, metrics, events, and bizevents sent
    """
    sender = LocalTelemetrySender(session, params, limit_results=limit_results, config=config)
    with mock_telemetry_sending():
        results = sender.send_data(sources)
    sender.teardown()

    return results


def get_config(pickle_conf: str = None) -> TestConfiguration:
    conf = {}
    if pickle_conf == "y":  # recreate the config file
        from test import _get_session

        session = _get_session()
        conf_class = config.Configuration(session)
        conf = conf_class._config

        with open(TEST_CONFIG_FILE_NAME, "w", encoding="utf-8") as f:
            json.dump(conf, f, indent=4)

    elif os.path.isfile(TEST_CONFIG_FILE_NAME):  # load existing config file
        with open(TEST_CONFIG_FILE_NAME, "r", encoding="utf-8") as f:
            conf = json.load(f)
    else:  # we need to create the config from scratch with dummy settings based on defaults
        from dtagent.otel.metrics import Metrics
        from dtagent.otel.events import Events
        from dtagent.otel.bizevents import BizEvents
        from dtagent.otel.logs import Logs
        from dtagent.otel.spans import Spans

        dt_url = "dsoa2025.live.dynatrace.com"
        sf_name = "test.dsoa2025"
        plugins = {}
        instruments = {"dimensions": {}, "metrics": {}, "attributes": {}, "event_timestamps": {}}
        conf = {
            "dt.token": "dt0c01.XXXXXXXXXXXXXXXXXXXXXXXX.XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
            "logs.http": f"https://{dt_url}{Logs.ENDPOINT_PATH}",
            "spans.http": f"https://{dt_url}{Spans.ENDPOINT_PATH}",
            "metrics.http": f"https://{dt_url}{Metrics.ENDPOINT_PATH}",
            "events.http": f"https://{dt_url}{Events.ENDPOINT_PATH}",
            "bizevents.http": f"https://{dt_url}{BizEvents.ENDPOINT_PATH}",
            "resource.attributes": Configuration.RESOURCE_ATTRIBUTES
            | {
                "service.name": sf_name,
                "deployment.environment": "TEST",
                "host.name": f"{sf_name}.snowflakecomputing.com",
            },
            "plugins": plugins,
            "instruments": instruments,
        }
        for file_path in find_files("src/dtagent/plugins", "*-config.json"):
            plugin_conf = lowercase_keys(read_clean_json_from_file(file_path))
            plugins.update(plugin_conf.get("plugins", {}))
        otel_config = lowercase_keys(read_clean_json_from_file("src/dtagent.conf/otel-config.json"))
        conf |= otel_config
        for file_path in find_files("src/", "instruments-def.yml"):
            instruments_data = read_clean_yml_from_file(file_path)
            instruments["dimensions"].update(instruments_data.get("dimensions", {}))
            instruments["metrics"].update(instruments_data.get("metrics", {}))
            instruments["attributes"].update(instruments_data.get("attributes", {}))
            instruments["event_timestamps"].update(instruments_data.get("event_timestamps", {}))
        conf["instruments"] = instruments

    return TestConfiguration(conf)


def read_clean_json_from_file(file_path: str) -> List[Dict]:
    """Reads given file into a dictionary, in case this is JSONC a clean JSON content is provided before turning into dict

    Args:
        file_path (str): path to the file with JSON or JSONC content

    Returns:
        List[Dict]: dictionary based on the content of the JSON/JSONC file
    """
    logging.debug("Reading file: %s", file_path)

    with open(file_path, "r", encoding="utf-8") as file:

        jsonc_str = file.read()
        json_str = jsonstrip.strip(jsonc_str)
        data = json.loads(json_str)

        return data

    return {}


def read_clean_yml_from_file(file_path: str) -> List[Dict]:
    """Reads given file into a dictionary.

    Args:
        file_path (str): path to the file with yaml content

    Returns:
        List[Dict]: dictionary based on the content of the YML/YAML file
    """
    import yaml

    logging.debug("Reading file: %s", file_path)

    with open(file_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

        return data

    return {}


def find_files(directory: str, filename_pattern: str) -> List[str]:
    """Lists all files with given name in the given directory
    Returns:
        list: List of file paths
    """

    matches = []
    for root, _, files in os.walk(directory):
        for filename in fnmatch.filter(files, filename_pattern):
            matches.append(os.path.join(root, filename))
    return matches


def lowercase_keys(data: Any) -> Any:
    """Lowercases recursively all keys in a dictionary (including nested dictionaries and lists)

    Args:
        data (Any): Input data (dict, list, or other)

    Returns:
        Any: Data with all dictionary keys lowercased
    """
    if isinstance(data, dict):
        return {k.lower(): lowercase_keys(v) for k, v in data.items()}

    if isinstance(data, list):
        return [lowercase_keys(item) for item in data]

    return data


def is_blank(value):
    return value is None or value == ""
