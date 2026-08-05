"""Config flow for 蓬莱 集成.

双模式：
- 一键绑定（推荐）：只需填入后端签发的设备绑定密钥（32 位 hex），
  集成自动调后端 exchange API 换取每设备独立 MQTT 凭据并完成配置。
  密钥不落 HA 配置、一次性作废，安全级别高于巴法 UID 模型。
- 手动配置：broker 地址 + 用户名/密码 + 设备 ID（兼容旧路径）。
"""
from __future__ import annotations

import logging
from typing import Any

import aiohttp
import voluptuous as vol

from homeassistant import config_entries
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResult
from homeassistant.helpers import config_validation as cv

from .const import (
    CONF_API_URL,
    CONF_BROKER_URL,
    CONF_DEVICE_ID,
    CONF_PAIRING_KEY,
    CONF_PASSWORD,
    CONF_PING_INTERVAL,
    CONF_PING_MAX_LOST,
    CONF_TOPIC_PREFIX,
    CONF_USERNAME,
    DEFAULT_API_URL,
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

    # ─── 入口：选择绑定方式 ───────────────────────────────

    async def async_step_user(self, user_input: dict[str, Any] | None = None) -> FlowResult:
        """入口步骤：选择「一键绑定」或「手动配置」。"""
        if user_input is not None:
            mode = user_input.get("mode", "pairing")
            if mode == "manual":
                return await self.async_step_manual()
            return await self.async_step_pairing()

        return self.async_show_form(
            step_id="user",
            data_schema=vol.Schema({
                vol.Required("mode", default="pairing"): vol.In({
                    "pairing": "🔑 一键绑定（推荐，仅需绑定密钥）",
                    "manual": "⚙️ 手动配置（用户名/密码/设备ID）",
                }),
            }),
            description_placeholders={
                "default_api": DEFAULT_API_URL,
            },
        )

    # ─── 一键绑定 ─────────────────────────────────────────

    async def async_step_pairing(self, user_input: dict[str, Any] | None = None) -> FlowResult:
        """一键绑定：填绑定密钥 → 调后端换取 MQTT 凭据。"""
        errors: dict[str, str] = {}
        if user_input is not None:
            pairing_key = user_input[CONF_PAIRING_KEY].strip()
            api_url = (user_input.get(CONF_API_URL) or DEFAULT_API_URL).rstrip("/")

            if not pairing_key or len(pairing_key) < 32:
                errors[CONF_PAIRING_KEY] = "invalid_key"
            else:
                try:
                    data = await self._exchange(pairing_key, api_url)
                except aiohttp.ClientError as exc:
                    _LOGGER.warning("蓬莱绑定 API 调用失败: %s", exc)
                    errors[CONF_PAIRING_KEY] = "api_unreachable"
                except ValueError as exc:
                    _LOGGER.warning("蓬莱绑定失败: %s", exc)
                    errors[CONF_PAIRING_KEY] = "exchange_failed"
                else:
                    return self._create_from_exchange(data)

        schema = vol.Schema({
            vol.Required(CONF_PAIRING_KEY): str,
            vol.Optional(CONF_API_URL, default=DEFAULT_API_URL): str,
        })
        return self.async_show_form(
            step_id="pairing",
            data_schema=schema,
            errors=errors,
            description_placeholders={
                "default_api": DEFAULT_API_URL,
            },
        )

    async def _exchange(self, pairing_key: str, api_url: str) -> dict[str, Any]:
        """调用后端 POST /api/v1/pairing/exchange 换取凭据。"""
        url = f"{api_url}/api/v1/pairing/exchange"
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, json={"pairing_key": pairing_key}) as resp:
                if resp.status != 200:
                    raise ValueError(f"exchange http {resp.status}")
                payload = await resp.json()
        if not payload.get("success"):
            raise ValueError(payload.get("message", "exchange failed"))
        data = payload.get("data") or {}
        for field in ("device_id", "device_name", "broker_url", "mqtt_username", "mqtt_password", "topic_prefix"):
            if field not in data:
                raise ValueError(f"exchange missing {field}")
        return data

    def _create_from_exchange(self, data: dict[str, Any]) -> FlowResult:
        """把后端换发的凭据转换为 HA 配置条目。

        topic_prefix 形如 penglai/5 → 拆为 CONF_TOPIC_PREFIX=penglai + CONF_DEVICE_ID=5，
        与手动模式的 {prefix}/{device_id}/cmd 拼接逻辑无缝兼容。
        """
        full_prefix = data["topic_prefix"]  # 如 penglai/pl-remote
        if "/" in full_prefix:
            prefix, device_id = full_prefix.split("/", 1)
        else:
            prefix = full_prefix
            device_id = str(data["device_id"])

        entry_data = {
            CONF_BROKER_URL: data["broker_url"],
            CONF_USERNAME: data["mqtt_username"],
            CONF_PASSWORD: data["mqtt_password"],
            CONF_DEVICE_ID: device_id,
            CONF_TOPIC_PREFIX: prefix,
            CONF_PING_INTERVAL: DEFAULT_PING_INTERVAL,
            CONF_PING_MAX_LOST: DEFAULT_PING_MAX_LOST,
        }
        # 绑定密钥不落配置：HA 只存换发后的凭据
        await_unique = device_id
        self._async_abort_entries_match({CONF_DEVICE_ID: device_id})
        return self.async_create_entry(
            title=f"蓬莱 {data['device_name']}",
            data=entry_data,
        )

    # ─── 手动配置（兼容旧路径） ─────────────────────────────

    async def async_step_manual(self, user_input: dict[str, Any] | None = None) -> FlowResult:
        """手动配置：broker + 用户名/密码 + 设备 ID。"""
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
            step_id="manual",
            data_schema=schema,
            errors=errors,
            description_placeholders={
                "default_broker": DEFAULT_BROKER_URL,
            },
        )

    async def async_step_import(self, user_input: dict[str, Any] | None = None) -> FlowResult:
        """从 YAML 导入（兼容旧配置）。"""
        return await self.async_step_manual(user_input)
