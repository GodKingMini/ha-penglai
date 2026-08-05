"""Config flow for 蓬莱 集成.

借鉴 bemfa config flow：让用户在 HA「添加集成」界面独立配置：
- broker 地址（默认 wss://pl.cteaz.top/mqtt）
- 用户名 / 密码（蓬莱分配的设备凭据）
- 设备 ID（绑定标识符）
"""
from __future__ import annotations

import logging
from typing import Any

import voluptuous as vol

from homeassistant import config_entries
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResult
from homeassistant.helpers import config_validation as cv

from .const import (
    CONF_BROKER_URL,
    CONF_DEVICE_ID,
    CONF_PASSWORD,
    CONF_PING_INTERVAL,
    CONF_PING_MAX_LOST,
    CONF_TOPIC_PREFIX,
    CONF_USERNAME,
    DEFAULT_PING_INTERVAL,
    DEFAULT_PING_MAX_LOST,
    DEFAULT_TOPIC_PREFIX,
    DOMAIN,
)

_LOGGER = logging.getLogger(__name__)

DEFAULT_BROKER_URL = "wss://pl.cteaz.top/mqtt"


def _broker_url_valid(url: str) -> bool:
    return url.startswith(("wss://", "ws://", "mqtt://", "tcp://"))


class PenglaiConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Penglai config flow."""

    VERSION = 1

    async def async_step_user(self, user_input: dict[str, Any] | None = None) -> FlowResult:
        """用户手动添加（推荐路径）。"""
        errors: dict[str, str] = {}
        if user_input is not None:
            if not _broker_url_valid(user_input[CONF_BROKER_URL]):
                errors[CONF_BROKER_URL] = "invalid_url"
            else:
                await self.async_set_unique_id(user_input[CONF_DEVICE_ID])
                self._abort_if_unique_id_configured()
                return self.async_create_entry(
                    title=f"蓬莱 {user_input[CONF_DEVICE_ID]}",
                    data=user_input,
                )

        schema = vol.Schema({
            vol.Required(CONF_BROKER_URL, default=DEFAULT_BROKER_URL): str,
            vol.Required(CONF_USERNAME): str,
            vol.Required(CONF_PASSWORD): str,
            vol.Required(CONF_DEVICE_ID): str,
            vol.Optional(CONF_TOPIC_PREFIX, default=DEFAULT_TOPIC_PREFIX): str,
            vol.Optional(CONF_PING_INTERVAL, default=DEFAULT_PING_INTERVAL): cv.positive_int,
            vol.Optional(CONF_PING_MAX_LOST, default=DEFAULT_PING_MAX_LOST): cv.positive_int,
        })

        return self.async_show_form(
            step_id="user",
            data_schema=schema,
            errors=errors,
            description_placeholders={
                "default_broker": DEFAULT_BROKER_URL,
            },
        )

    async def async_step_import(self, user_input: dict[str, Any] | None = None) -> FlowResult:
        """从 YAML 导入（兼容旧配置）。"""
        return await self.async_step_user(user_input)
