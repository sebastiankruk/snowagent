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

from calendar import c
import json
from multiprocessing.connection import Client
import sys
import time
import uuid
import asyncio
import logging
from abc import ABC, abstractmethod
from types import NoneType
from typing import Any, Dict, List, Optional, Tuple, Union, Generator

import aiohttp
import requests

from dtagent.context import RUN_CONTEXT_KEY
from dtagent.otel import _log_warning
from dtagent.otel.otel_manager import OtelManager
from dtagent.util import StringEnum, get_timestamp_in_ms
from dtagent.version import VERSION


##endregion COMPILE_REMOVE

##region ------------------------ Abstract EVENTS ---------------------------------

EventType = StringEnum(
    "EventType",
    [
        "AVAILABILITY_EVENT",
        "CUSTOM_ALERT",
        "CUSTOM_ANNOTATION",
        "CUSTOM_CONFIGURATION",
        "CUSTOM_DEPLOYMENT",
        "CUSTOM_INFO",
        "ERROR_EVENT",
        "MARKED_FOR_TERMINATION",
        "PERFORMANCE_EVENT",
        "RESOURCE_CONTENTION_EVENT",
    ],
)


class AbstractEvents(ABC):
    """This is an abstract class for OpenPipelineEvents and BizEvents implementations"""

    from dtagent.config import Configuration  # COMPILE_REMOVE
    from dtagent.otel.instruments import Instruments  # COMPILE_REMOVE

    ENDPOINT_PATH = None  # to be defined in child classes

    def __init__(self, configuration: Configuration, event_type: str = "events", default_params: Optional[Dict[str, Any]] = None):
        """Initializing payload cache and fields from configuration.

        Args:
            configuration (Configuration):                          Current configuration of the agent
            event_type (str, optional):                             Name of the events. Defaults to "events".
            default_params (Optional[Dict[str, Any]], optional):    Dictionary with overrides for default values for API parameters.
                                                                    Defaults to None.
        """
        _default_params = default_params or {}

        self._configuration = configuration
        self._resource_attributes = self._configuration.get("resource.attributes")
        self._max_concurrent_senders = self._configuration.get(
            otel_module=event_type, key="max_concurrent_senders", default_value=_default_params.get("max_concurrent_senders", 5)
        )
        self._max_payload_bytes = self._configuration.get(
            otel_module=event_type, key="max_payload_bytes", default_value=_default_params.get("max_payload_bytes", 10000000)
        )
        self._max_event_count = self._configuration.get(
            otel_module=event_type, key="max_event_count", default_value=_default_params.get("max_event_count", 400)
        )
        self._max_retries = self._configuration.get(
            otel_module=event_type, key="max_retries", default_value=_default_params.get("max_retries", 5)
        )
        self._retry_delay_ms = self._configuration.get(
            otel_module=event_type, key="retry_delay_ms", default_value=_default_params.get("retry_delay_ms", 10000)
        )
        self._ingest_retry_statuses = self._configuration.get(
            otel_module=event_type, key="retry_on_status", default_value=_default_params.get("retry_on_status", [429, 502, 503])
        )
        self._api_event_type = event_type
        self._api_url = self._configuration.get(f"{event_type}.http")
        self._api_content_type = _default_params.get("api_content_type", "application/json")
        self._api_post_timeout = self._configuration.get(
            otel_module=event_type, key="api_post_timeout", default_value=_default_params.get("api_post_timeout", 30)
        )

        self.PAYLOAD_QUEUE: asyncio.Queue[Dict[str, Any]] = asyncio.Queue()
        self._send_semaphore = asyncio.Semaphore(self._max_concurrent_senders)
        self._send_tasks: List[asyncio.Task] = []
        self._shutdown_event = asyncio.Event()
        self._enqueued_count = 0
        self._sent_count = 0

        # Start background senders
        try:
            asyncio.get_running_loop()
            for sender_id in range(self._max_concurrent_senders):
                task = asyncio.create_task(self._background_sender(sender_id))
                self._send_tasks.append(task)
        except RuntimeError:
            from dtagent import LL_TRACE, LOG  # COMPILE_REMOVE

            # No running event loop (e.g., in tests), skip starting background tasks
            LOG.debug("No running event loop; skipping background sender tasks startup")

    async def _send_async(self, _payload_list: List[Dict[str, Any]], _retries: int = 0) -> Tuple[int, List[Dict[str, Any]]]:
        """Sends given payload to Dynatrace asynchronously

        Args:
            _payload_list (List[Dict[str, Any]]): List of events to be sent
            _retries (int, optional): Current retry count. Defaults to 0.
        """
        from dtagent import LL_TRACE, LOG  # COMPILE_REMOVE

        response = None
        events_count = -1  # something was wrong with events sent - resetting to -1
        _payload_to_repeat = []  # we will keep events that failed to be delivered

        headers = {
            "Authorization": f'Api-Token {self._configuration.get("dt.token")}',
            "Accept": "application/json",
            "Content-Type": self._api_content_type,
        } | OtelManager.get_dsoa_headers()

        payload = json.dumps(_payload_list)
        payload_cnt = len(_payload_list)
        try:
            LOG.log(
                LL_TRACE,
                "Sending %d bytes payload with %d %s records to %s",
                sys.getsizeof(payload),
                payload_cnt,
                self._api_event_type,
                self._api_url,
            )

            LOG.log(
                LL_TRACE,
                "Sending %s as %s records to %s",
                payload,
                self._api_event_type,
                self._api_url,
            )

            response, events_count = await self._post_to_events_api(headers, payload, payload_cnt)

        except aiohttp.ClientError as e:
            self._log_send_error(_payload_list, _retries, e)
        finally:
            events_count, _payload_to_repeat = await self._resend_failed_events(
                _payload_list, events_count, _retries, response, self._ingest_retry_statuses
            )

        return events_count, _payload_to_repeat

    async def _post_to_events_api(
        self, headers: Dict[str, str], payload: str, payload_cnt: int, ok_status: int = 202
    ) -> Tuple[aiohttp.ClientResponse, int]:
        """Helper method to post given payload to Dynatrace Events API

        Args:
            headers (Dict[str, str]): Headers to be used for the request
            payload (str): Payload to be sent
            payload_cnt (int): Number of events in the payload
            ok_status (int, optional): Expected OK status code. Defaults to 202.

        Returns:
            Tuple[aiohttp.ClientResponse, int]: response object and number of events successfully sent
        """
        from dtagent import LL_TRACE, LOG  # COMPILE_REMOVE

        events_count = 0
        async with aiohttp.ClientSession() as session:
            async with session.post(
                self._api_url,
                headers=headers,
                data=payload,
                timeout=aiohttp.ClientTimeout(total=self._api_post_timeout),
            ) as response:
                LOG.log(
                    LL_TRACE,
                    "Sent payload with %d %s records; response: %s",
                    payload_cnt,
                    self._api_event_type,
                    response.status,
                )

                if response.status == ok_status:
                    events_count = payload_cnt
                    self._sent_count += payload_cnt
                    OtelManager.set_current_fail_count(0)
                else:
                    LOG.warning(
                        "Problem sending %s to Dynatrace; error code: %s, reason: %s, response: %s, payload: %r",
                        self._api_event_type,
                        response.status,
                        response.reason,
                        await response.text(),
                        payload[:100],
                    )

        return response, events_count

    async def _resend_failed_events(
        self,
        events_count: int,
        payload_list: List[Dict[str, Any]],
        retries: int,
        response: aiohttp.ClientResponse,
        ingest_retry_statuses: List[int] = None,
    ) -> Tuple[int, List[Dict[str, Any]]]:
        """Will attempt to re-send events that we failed to be sent recently

        Args:
            events_count (int): number of events successfully sent so far
            payload_list (List[Dict[str, Any]]): List of events that were attempted to be sent
            retries (int): number of retry attempts
            response (aiohttp.ClientResponse): response object from the last send attempt

        Returns:
            Tuple[int, List[Dict[str, Any]]]:
                int: number of events that we managed to send
                List[Dict[str, Any]]: list of events that we failed to send and we need to resend
        """
        from dtagent import LL_TRACE, LOG  # COMPILE_REMOVE

        _payload_to_repeat = []
        if response is not None:
            if ingest_retry_statuses is None or response.status in ingest_retry_statuses:
                if retries < self._max_retries:
                    await asyncio.sleep(self._retry_delay_ms / 1000)
                    repeated_events_send, _payload_to_repeat = await self._send_async(payload_list, retries + 1)
                    events_count += repeated_events_send
                else:
                    LOG.warning(
                        "Failed to send all %s data with %d (max=%d) attempts; last status code = %s",
                        self._api_event_type,
                        retries,
                        self._max_retries,
                        response.status,
                    )
                    _payload_to_repeat = payload_list
                    OtelManager.increase_current_fail_count(response)
            elif response.status >= 300:
                OtelManager.increase_current_fail_count(response)

        OtelManager.verify_communication()

        return events_count, _payload_to_repeat

    def _log_send_error(self, payload_list: Union[Dict[str, Any], List[Dict[str, Any]]], retries: int, e: aiohttp.ClientError):
        """Helper method to report problems when sending events.

        Args:
            payload_list (list): event data that was suppose to be sent
            retries (int): number of retry attempts
            e (aiohttp.ClientError): error object
        """
        from dtagent import LL_TRACE, LOG  # COMPILE_REMOVE

        if isinstance(e, asyncio.TimeoutError):
            LOG.error(
                "The request to send %d bytes payload with %d %s records timed out after %d seconds. (retry = %d)",
                sys.getsizeof(payload_list),
                len(payload_list),
                self._api_event_type,
                self._api_post_timeout,
                retries,
            )
        else:
            LOG.error(
                "An error occurred when sending %d bytes payload with %d %s records (retry = %d): %s",
                sys.getsizeof(payload_list),
                len(payload_list),
                self._api_event_type,
                retries,
                e,
            )

    def _split_payload(self, payload: List[Dict[str, Any]]) -> Generator[List[Dict[str, Any]], None, None]:
        """Enables to iterate over "ingestible" chunks of events payload
        Args:
            payload (List[Dict[str, Any]]): List of events to be sent
        Yields:
            Generator[List[Dict[str, Any]], None, None]: generator of lists of events, each within allowed limits
        """
        from dtagent import LL_TRACE, LOG  # COMPILE_REMOVE

        current_chunk = []
        current_size = 0
        for event in payload:
            event_size = sys.getsizeof(json.dumps(event))
            if event_size > self._max_payload_bytes:
                LOG.warning(
                    "%s records size %d exceeds max payload size %d, skipping event",
                    self._api_event_type,
                    event_size,
                    self._max_payload_bytes,
                )
                continue
            if current_size + event_size > self._max_payload_bytes or len(current_chunk) >= self._max_event_count:
                yield current_chunk
                current_chunk = []
                current_size = 0

            current_chunk.append(event)
            current_size += event_size

        if current_chunk:
            yield current_chunk

    def _enqueue_events(self, payload: List[Dict[str, Any]]) -> int:
        """Enqueues events for asynchronous sending.

        Args:
            payload (List[Dict[str, Any]]): List of events to enqueue.
        """
        for event in payload:
            self.PAYLOAD_QUEUE.put_nowait(event)
        self._enqueued_count += len(payload)
        return len(payload)

    async def _background_sender(self, sender_id: int) -> None:
        """Background task that processes the queue and sends chunks.
        The code attempts to accumulate to the maximal size of payload allowed - and
        will flush before we would exceed with new payload increment.
        IMPORTANT: call _flush_events() to flush at the end of processing
        """
        from dtagent import LOG, LL_TRACE  # COMPILE_REMOVE

        while not self._shutdown_event.is_set():
            try:
                # Collect up to max_event_count events
                chunk = []
                for _ in range(self._max_event_count):
                    try:
                        event = await asyncio.wait_for(self.PAYLOAD_QUEUE.get(), timeout=1.0)
                        LOG.debug(
                            "Background sender %d picked event from queue; current queue size: %d", sender_id, self.PAYLOAD_QUEUE.qsize()
                        )
                        chunk.append(event)
                    except asyncio.TimeoutError:
                        break
                if chunk:
                    async with self._send_semaphore:
                        LOG.log(
                            logging.DEBUG,
                            "Will send %d %s records from background sender %d",
                            len(chunk),
                            self._api_event_type,
                            sender_id,
                        )
                        _, failed = await self._send_async(chunk)
                        LOG.log(
                            logging.DEBUG,
                            "Have sent %d %s records from background sender %d; failed to send %d records",
                            len(chunk),
                            self._api_event_type,
                            sender_id,
                            len(failed),
                        )
                        # Re-enqueue failed events
                        for event in failed:
                            await self.PAYLOAD_QUEUE.put(event)
            except asyncio.CancelledError:
                break
            except Exception as e:  # noqa: BLE001,W0718
                LOG.error("Error in background sender: %s", e)

    async def flush_events(self) -> int:
        """Flush remaining events in the queue and return sent count."""
        from dtagent import LOG, LL_TRACE  # COMPILE_REMOVE

        # If no background tasks are running, process the queue directly
        if not self._send_tasks:
            while not self.PAYLOAD_QUEUE.empty():
                chunk = []
                for _ in range(self._max_event_count):
                    try:
                        event = self.PAYLOAD_QUEUE.get_nowait()
                        chunk.append(event)
                    except asyncio.QueueEmpty:
                        break
                if chunk:
                    LOG.log(logging.DEBUG, "Will send %d %s records from flush", len(chunk), self._api_event_type)
                    _, failed = await self._send_async(chunk)
                    LOG.log(
                        logging.DEBUG,
                        "Have sent %d %s records from flush; failed to send %d records",
                        len(chunk),
                        self._api_event_type,
                        len(failed),
                    )
                    # Re-enqueue failed events
                    for event in failed:
                        await self.PAYLOAD_QUEUE.put(event)
        else:
            # Wait for background tasks to finish processing the queue
            LOG.log(logging.DEBUG, "Waiting for queue to drain, current size: %d", self.PAYLOAD_QUEUE.qsize())
            while not self.PAYLOAD_QUEUE.empty():
                await asyncio.sleep(0.1)
            LOG.log(logging.DEBUG, "Queue drained, sent %d %s records", self._sent_count, self._api_event_type)

        sent = self._sent_count
        self._sent_count = 0
        return sent

    async def shutdown(self) -> None:
        """Shutdown background tasks."""
        self._shutdown_event.set()
        if self._send_tasks:
            await asyncio.gather(*self._send_tasks, return_exceptions=True)

    def get_sent_count(self) -> int:
        """Get the number of events successfully sent."""
        return self._sent_count

    def get_enqueued_count(self) -> int:
        """Get the number of events enqueued."""
        return self._enqueued_count

    @abstractmethod
    def _pack_event_data(
        self, event_type: Union[str, EventType], event_data: Dict[str, Any], context: Optional[Dict[str, Any]] = None, **kwargs
    ) -> Dict[str, Any]:
        """Packs given event data into payload accepted by Dynatrace Events API

        Args:
            event_type (str): Event type, e.g. EventType.CUSTOM_ALERT
            event_data (Dict[str, Any]): Event data in form of dict
            context (Optional[Dict[str, Any]]): Additional context to be added to event properties
            **kwargs: Additional parameters, like title, start_time_key, end_time_key, additional_payload, timeout, or formatted_time
        Returns:
            Dict[str, Any]: Event payload in form accepted by Dynatrace BizEvents endpoint
        """
        raise NotImplementedError("This is an abstract method and should be implemented in child classes")

    def send_events(
        self,
        events_data: List[Dict[str, Any]],
        event_type: Optional[Union[str, EventType]] = EventType.CUSTOM_ALERT,
        context: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> None:
        """Sends given list of events to Dynatrace via the generic OpenPipeline API.

        Args:
            events_data (List):                                     List of events data, each in form of dict
            event_type (Optional[Union[str, EventType]], optional): Event type to report under. Defaults to None.
            context (Dict, optional):                               Additional information that should be appended to event data based on
                                                                    agent execution context. Defaults to None.
            **kwargs:                                               Additional keyword arguments to be processed by child classes:
                additional_payload (Dict, optional):                    Additional lines of payload,
                title (str, optional):                                  Event title,
                start_time_key (str, optional):                         Key in event_data dict to be used as event start time,
                end_time_key (str, optional):                           Key in event_data dict to be used as event end time,
                timeout (int, optional):                                Timeout for sending events,

        Returns:
            None
        """
        from dtagent import LOG  # COMPILE_REMOVE

        events_data = [
            self._pack_event_data(
                event_type=event_type,
                event_data=event_data,
                context=context,
                **kwargs,
            )
            for event_data in events_data
        ]
        self._enqueue_events(events_data)

    def report_via_api(
        self,
        query_data: Union[List[Dict[str, Any]], Generator[Dict, None, None], Dict[str, Any]],
        event_type: Optional[Union[str, EventType]],
        *,
        is_data_structured: bool = True,
        context: Optional[Dict] = None,
        **kwargs,
    ) -> None:
        """Generates and sends payload with selected events to Dynatrace via the chosen API.

        Args:
            query_data (Union[List[Dict], Generator[Dict, None, None]]): List or generator of dictionaries with data to be sent as events
            event_type (Optional[Union[str, EventType]], optional):      Event type to report under. Defaults to None.
            is_data_structured (bool, optional):                         Indicates whether the data in query_data is structured into a
                                                                         DSOA standardized dictionary. Defaults to True.
            context (Optional[Dict], optional):                          Additional context that should be added to the event properties,
                                                                         formatted as dict which is merged with unpacked query_data contents
                                                                         based on agent execution context. Defaults to None.

            **kwargs: Additional keyword arguments to be passed to send_events() and processed in child classes:
                additional_payload (Dict, optional):        Additional lines of payload,
                title (str, optional):                      Event title,
                start_time_key (str, optional):             Key in event_data dict to be used as event start time,
                end_time_key (str, optional):               Key in event_data dict to be used as event end time,
                timeout (int, optional):                    Timeout for sending events,
                formatted_time (bool, optional):            Indicates whether the time values in event_data are already formatted

        Returns:
            None
        """
        from dtagent.util import _unpack_payload  # COMPILE_REMOVE

        _event_data = [query_data] if isinstance(query_data, dict) else query_data
        # unpacking only if data is structured; otherwise using as-is
        if is_data_structured:
            _event_data = [_unpack_payload(query_datum) for query_datum in _event_data]

        self.send_events(_event_data, event_type, context, **kwargs)


##endregion
