"""Mechanisms allowing for parsing and sending events."""

##region ------------------------------ IMPORTS  -----------------------------------------
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
import sys
import time
import uuid
import asyncio
import aiohttp
from abc import ABC, abstractmethod
from types import NoneType
from typing import Any, Dict, List, Optional, Tuple, Union, Generator

import requests

from dtagent.context import RUN_CONTEXT_KEY
from dtagent.otel import _log_warning
from dtagent.otel.otel_manager import OtelManager
from dtagent.otel.events import EventType, AbstractEvents
from dtagent.otel.events.generic import GenericEvents
from dtagent.util import StringEnum, get_timestamp_in_ms
from dtagent.version import VERSION


##endregion COMPILE_REMOVE

##region ------------------------ Davis EVENTS ---------------------------------


class DavisEvents(GenericEvents):
    """Allows for parsing and sending (Davis) Events payloads via Events v2 API
    https://docs.dynatrace.com/docs/discover-dynatrace/platform/openpipeline/reference/api-ingestion-reference#davis-events

    Note: Events API does not support sending multiple events at the same time, as a bulk, like in BizEvents or OpenPipelineEvents.

    Deprecated: use dtagent.otel.events.generic.GenericEvents instead.
    """

    from dtagent.config import Configuration  # COMPILE_REMOVE
    from dtagent.otel.instruments import Instruments  # COMPILE_REMOVE

    ENDPOINT_PATH = "/api/v2/events/ingest"

    def __init__(self, configuration: Configuration):
        """Initializes configuration's resources for events"""
        GenericEvents.__init__(self, configuration, event_type="davis_events")

    async def _send_async(self, _payload_list: List[Dict[str, Any]], _retries: int = 0) -> Tuple[int, List[Dict[str, Any]]]:
        """Sends given payload to Dynatrace Events v2 API asynchronously.
        Overrides GenericEvents.__send_async() as Events v2 API does not support sending multiple events at the same time,
        as a bulk, like in BizEvents or OpenPipelineEvents.

        Args:
            _payload_list (list): list of events to send
            _retries (int, optional): Number of retries - will stop retrying after 3 attempts. Defaults to 0.

        Returns:
            int: number of events that we managed to send
            List[Dict[str, Any]]: list of events that we failed to send and we need to resend
        """
        from dtagent import LL_TRACE, LOG  # COMPILE_REMOVE

        headers = {
            "Authorization": f'Api-Token {self._configuration.get("dt.token")}',
            "Content-Type": "application/json",
        } | OtelManager.get_dsoa_headers()

        response = None
        events_count = 0
        _payload_to_repeat = []  # we will keep events that failed to be delivered

        for _payload in _payload_list:
            try:
                payload = json.dumps(_payload)
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        self._api_url,
                        headers=headers,
                        data=payload,
                        timeout=aiohttp.ClientTimeout(total=self._api_post_timeout),
                    ) as response:
                        LOG.log(LL_TRACE, "Sent %d events payload; response: %d", len(_payload), response.status)

                        if response.status == 201:
                            events_count += 1
                            self._sent_count += 1
                            OtelManager.set_current_fail_count(0)
                        else:
                            LOG.warning(
                                "Problem sending %s to Dynatrace; error code: %s, reason: %s, response: %s, payload: %r",
                                "event",
                                response.status,
                                response.reason,
                                await response.text(),
                                str(_payload)[:100],
                            )

            except aiohttp.ClientError as e:
                self._log_send_error(_payload, _retries, e)
                _payload_to_repeat.append(_payload)

        if _payload_to_repeat:
            events_count, _payload_to_repeat = await self._resend_failed_events(
                _payload_to_repeat, events_count, _retries, response, self._ingest_retry_statuses
            )

        return events_count, _payload_to_repeat

    def _add_data_to_payload(self, payload: Dict[str, Any], event_data: Dict[str, Any]) -> Dict[str, Any]:
        """Adds given properties to event payload under 'properties' key
        Args:
            payload (Dict[str, Any]): Event payload in form of dict
            event_data (Dict[str, Any]): Properties to be added to event payload
        Returns:
            Dict[str, Any]: Event payload with added event data
        """

        def __limit_to_api(properties: Dict[str, str]) -> Dict:
            """Limit values to no longer than 4096 characters as per API documentation."""
            for key in properties:
                if isinstance(properties[key], str) and len(properties[key]) > 4096:
                    properties[key] = properties[key][:4096]

            return properties

        payload["properties"] = __limit_to_api(event_data or {})
        return payload

    def _split_payload(self, payload: List[Dict[str, Any]]) -> Generator[List[Dict[str, Any]], None, None]:
        """Overrides _split_payload from GenericEvents as Events v2 API does not support sending multiple events at the same time,
        and hence we do not need to split payloads.

        Args:
            payload (List[Dict[str, Any]]): List of events to be sent
        Yields:
            Generator[List[Dict[str, Any]], None, None]: generator of lists of events, each within allowed limits
        """
        yield payload


##endregion
