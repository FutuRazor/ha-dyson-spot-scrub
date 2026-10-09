"""Coordinator — bridges the paho MQTT thread to HA's asyncio event loop."""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback

from .const import (
    CONF_SERIAL,
    CONF_MQTT_PREFIX,
    CONF_AUTH_TOKEN,
    DEFAULT_MODE,
    CONF_CACHED_ROOMS,
)
from .dyson_api import get_iot_credentials, get_live_map, DysonApiError
from .dyson_mqtt import DysonMqttClient

_LOGGER = logging.getLogger(__name__)

# Reconnect backoff schedule (seconds): 15 s, 30 s, 60 s, then 120 s forever
_RECONNECT_DELAYS = [15, 30, 60, 120]

# Minimum seconds between upstream live-map API calls (shared across all viewers)
_LIVE_MAP_MIN_INTERVAL = 5.0


class DysonCoordinator:
    """Owns the MQTT client and distributes state to registered HA entities."""

    def __init__(
        self,
        hass: HomeAssistant,
        token: str,
        serial: str,
        mqtt_prefix: str,
        config_entry: ConfigEntry,
        verbose: bool = False,
    ) -> None:
        self.hass          = hass
        self._token        = token
        self.serial        = serial
        self._prefix       = mqtt_prefix
        self._config_entry = config_entry
        self._verbose      = verbose

        self.mqtt: DysonMqttClient | None = None

        # Entities register here; coordinator calls them on every state change
        self._listeners: list[Any] = []

        # Currently selected cleaning mode (persisted between restarts via HA storage)
        self.current_mode: str = DEFAULT_MODE

        # Room names from the robot's map preference cache.
        # Loaded from the config entry on startup so room switch entities can be
        # created immediately, even before the MQTT connection delivers fresh data.
        self.cached_room_names: list[str] = list(
            config_entry.data.get(CONF_CACHED_ROOMS, [])
        )

        # Rooms whose switch entity is toggled ON (for sequential clean)
        self.enabled_rooms: set[str] = set()

        # Reconnect state
        self._shutting_down: bool = False
        self._reconnect_task: asyncio.Task | None = None
        self._reconnect_attempt: int = 0

        # Live-map fetch: shared across all camera viewers (deduplication + 429 backoff)
        self._live_map_cache: dict | None = None
        self._live_map_fetched_at: float = 0.0
        self._live_map_backoff_until: float = 0.0
        self._live_map_lock: asyncio.Lock = asyncio.Lock()

    # ── Setup / teardown ──────────────────────────────────────────────────────

    async def async_setup(self) -> None:
        """Open the cloud MQTT connection.

        Fetches fresh IoT credentials from Dyson's API and connects to the
        AWS IoT Core broker over WebSocket (port 443).

        On failure the error is logged but NOT re-raised so the config entry
        still loads and entities are created.  They show as unavailable until
        a background reconnect succeeds.
        """
        self._shutting_down = False
        self._reconnect_attempt = 0

        try:
            await self._async_connect_cloud()
        except Exception as exc:
            _LOGGER.warning(
                "[%s] Initial MQTT connection failed (%s) — "
                "entities will load in unavailable state; "
                "reconnect will retry automatically",
                self.serial, exc,
            )
            if not self._shutting_down:
                self._reconnect_task = self.hass.async_create_task(
                    self._async_reconnect()
                )

    async def _async_connect_cloud(self) -> None:
        """Fetch fresh IoT credentials and connect to the cloud MQTT broker."""
        _LOGGER.debug("[%s] Fetching IoT credentials from Dyson API", self.serial)
        iot = await get_iot_credentials(self._token, self.serial)

        new_client = DysonMqttClient(
            serial=self.serial,
            mqtt_prefix=self._prefix,
            endpoint=iot["endpoint"],
            token_value=iot["token_value"],
            token_signature=iot["token_signature"],
            client_id=iot["client_id"],
            authorizer_name=iot["authorizer_name"],
            verbose=self._verbose,
        )
        new_client.on_connected      = self._on_mqtt_connected
        new_client.on_disconnected   = self._on_mqtt_disconnected
        new_client.on_prefix_changed = self._on_mqtt_prefix_changed
        new_client.register_callback(self._on_state_change)

        await self.hass.async_add_executor_job(new_client.connect)
        self.mqtt = new_client

    async def async_shutdown(self) -> None:
        self._shutting_down = True
        if self._reconnect_task and not self._reconnect_task.done():
            self._reconnect_task.cancel()
            try:
                await self._reconnect_task
            except asyncio.CancelledError:
                pass
        if self.mqtt:
            await self.hass.async_add_executor_job(self.mqtt.disconnect)

    # ── Reconnect logic ───────────────────────────────────────────────────────

    async def _async_reconnect(self) -> None:
        """Reconnect to Dyson's cloud MQTT broker, with backoff.

        Fetches fresh IoT credentials on each attempt — the tokens expire
        so they must not be cached across reconnects.
        """
        delay = _RECONNECT_DELAYS[
            min(self._reconnect_attempt, len(_RECONNECT_DELAYS) - 1)
        ]
        self._reconnect_attempt += 1
        _LOGGER.info(
            "[%s] Reconnect attempt %d — waiting %d s before retrying",
            self.serial, self._reconnect_attempt, delay,
        )
        await asyncio.sleep(delay)

        if self._shutting_down:
            return

        # Tear down the old client cleanly (stop its loop thread)
        old_mqtt = self.mqtt
        self.mqtt = None
        if old_mqtt:
            try:
                await self.hass.async_add_executor_job(old_mqtt.disconnect)
            except Exception:
                pass

        try:
            await self._async_connect_cloud()
            self._reconnect_attempt = 0  # Reset backoff on success
            _LOGGER.info("[%s] Reconnected to cloud broker successfully", self.serial)
        except Exception:
            _LOGGER.exception(
                "[%s] Reconnect failed — scheduling retry", self.serial
            )
            if not self._shutting_down:
                self._reconnect_task = self.hass.async_create_task(
                    self._async_reconnect()
                )

    # ── Entity registration ───────────────────────────────────────────────────

    def async_add_listener(self, listener: Any) -> None:
        """Register an entity to be notified of state changes."""
        self._listeners.append(listener)

    def async_remove_listener(self, listener: Any) -> None:
        self._listeners = [l for l in self._listeners if l is not listener]

    # ── Live-map fetch (shared across camera viewers) ─────────────────────────

    async def async_get_live_map(self) -> dict | None:
        """Fetch the live cleaning map, shared across all viewers.

        Returns cached data if it is still fresh (within _LIVE_MAP_MIN_INTERVAL).
        Returns None — without touching the cache — when inside a 429 back-off
        window or when the upstream request fails.  Callers should keep their
        previous good frame when None is returned during an active cleaning run.

        At most one coroutine fetches at a time; others wait and share the result.
        """
        now = time.monotonic()

        # Inside 429 back-off — don't attempt a new request
        if now < self._live_map_backoff_until:
            _LOGGER.debug(
                "[%s] Live-map: 429 back-off %.0f s remaining",
                self.serial,
                self._live_map_backoff_until - now,
            )
            return None

        # Return cached data if it is fresh enough (skip the lock — fast path)
        if (
            self._live_map_cache is not None
            and (now - self._live_map_fetched_at) < _LIVE_MAP_MIN_INTERVAL
        ):
            return self._live_map_cache

        # One coroutine fetches at a time; the rest wait and share the result
        async with self._live_map_lock:
            # Re-check after acquiring — a previous waiter may have already fetched
            now = time.monotonic()
            if now < self._live_map_backoff_until:
                return None
            if (
                self._live_map_cache is not None
                and (now - self._live_map_fetched_at) < _LIVE_MAP_MIN_INTERVAL
            ):
                return self._live_map_cache

            try:
                data = await get_live_map(self._token, self.serial)
                self._live_map_cache = data
                self._live_map_fetched_at = time.monotonic()
                return data
            except DysonApiError as exc:
                raw = str(exc)
                if "429" in raw:
                    # Parse the Retry-After value embedded by get_live_map()
                    retry_after = 30.0
                    if "Retry-After" in raw:
                        try:
                            after_keyword = raw.split("Retry-After", 1)[1]
                            retry_after = max(5.0, float(after_keyword.strip().rstrip(")").split()[0]))
                        except (ValueError, IndexError):
                            pass
                    self._live_map_backoff_until = time.monotonic() + retry_after
                    _LOGGER.warning(
                        "[%s] Live-map HTTP 429 — backing off %.0f s",
                        self.serial,
                        retry_after,
                    )
                else:
                    _LOGGER.debug("[%s] Live-map fetch failed: %s", self.serial, exc)
                # Return None so callers keep their last-good frame
                return None

    # ── Internal callbacks (called from paho thread) ──────────────────────────

    def _on_state_change(self, state: dict) -> None:
        """paho thread → schedule update on HA event loop."""
        self.hass.loop.call_soon_threadsafe(self._async_update_rooms_and_notify)

    def _on_mqtt_connected(self) -> None:
        self.hass.loop.call_soon_threadsafe(self._async_update_rooms_and_notify)

    def _on_mqtt_disconnected(self) -> None:
        _LOGGER.warning("[%s] MQTT disconnected — will reconnect", self.serial)
        self.hass.loop.call_soon_threadsafe(self._async_on_disconnected)

    def _on_mqtt_prefix_changed(self, new_prefix: str) -> None:
        """paho thread → schedule prefix persistence on HA event loop."""
        self.hass.loop.call_soon_threadsafe(
            self._async_on_prefix_changed, new_prefix
        )

    @callback
    def _async_on_prefix_changed(self, new_prefix: str) -> None:
        """Persist the corrected MQTT prefix to the config entry."""
        self._prefix = new_prefix  # use corrected prefix on next reconnect
        self.hass.config_entries.async_update_entry(
            self._config_entry,
            data={**self._config_entry.data, CONF_MQTT_PREFIX: new_prefix},
        )
        _LOGGER.info(
            "[%s] MQTT prefix updated to '%s' in config entry",
            self.serial, new_prefix,
        )

    @callback
    def _async_on_disconnected(self) -> None:
        """Runs on the HA event loop after an unexpected disconnect."""
        self._async_notify_listeners()
        if not self._shutting_down:
            if self._reconnect_task is None or self._reconnect_task.done():
                self._reconnect_task = self.hass.async_create_task(
                    self._async_reconnect()
                )

    @callback
    def _async_update_rooms_and_notify(self) -> None:
        """Check for new room names from MQTT, persist if changed, then notify."""
        if self.mqtt:
            new_rooms = self.mqtt.room_names
            if new_rooms and set(new_rooms) != set(self.cached_room_names):
                self.cached_room_names = list(new_rooms)
                self.hass.config_entries.async_update_entry(
                    self._config_entry,
                    data={**self._config_entry.data, CONF_CACHED_ROOMS: new_rooms},
                )
                _LOGGER.debug(
                    "[%s] Room names cached: %s", self.serial, new_rooms
                )
        self._async_notify_listeners()

    @callback
    def _async_notify_listeners(self) -> None:
        for listener in list(self._listeners):
            try:
                listener.async_write_ha_state()
            except Exception:
                _LOGGER.exception("[%s] Error notifying listener", self.serial)

    # ── Sequential multi-room cleaning ────────────────────────────────────────

    async def async_clean_rooms_sequential(self, room_names: list[str]) -> None:
        """Clean a list of rooms one at a time, waiting for docking between each."""
        if not room_names:
            return
        _LOGGER.info("[%s] Sequential clean starting — rooms: %s", self.serial, room_names)
        for room in room_names:
            if not self.mqtt or not self.mqtt.connected:
                _LOGGER.warning("[%s] MQTT not connected — aborting sequential clean", self.serial)
                return
            _LOGGER.info("[%s] Sequential clean → '%s'", self.serial, room)
            from .const import MODE_TO_INT
            mode_int = MODE_TO_INT.get(self.current_mode, 0)
            await self.hass.async_add_executor_job(self.mqtt.start_room, room, mode_int)
            await self._async_wait_for_clean_complete()
        _LOGGER.info("[%s] Sequential clean finished all rooms", self.serial)

    async def _async_wait_for_clean_complete(self, timeout: float = 3600.0) -> None:
        """Wait until the robot returns to dock (or times out)."""
        from .dyson_mqtt import is_docked, is_charging, is_any_cleaning

        loop = asyncio.get_event_loop()
        done_event: asyncio.Event = asyncio.Event()
        cleaning_started = False

        def _check() -> None:
            nonlocal cleaning_started
            if not self.mqtt:
                done_event.set()
                return
            state = self.mqtt.state
            if is_any_cleaning(state):
                cleaning_started = True
            elif cleaning_started and (is_docked(state) or is_charging(state)):
                done_event.set()

        class _Watcher:
            def async_write_ha_state(inner_self) -> None:
                loop.call_soon_threadsafe(_check)

        watcher = _Watcher()
        self.async_add_listener(watcher)
        try:
            await asyncio.wait_for(done_event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            _LOGGER.warning("[%s] Timed out waiting for robot to dock", self.serial)
        finally:
            self.async_remove_listener(watcher)
