"""蓬莱 集成入口.

借鉴 bemfa 集成结构：
- 独立添加集成（config flow）
- 启动即连接自建 EMQX broker（WSS）
- 订阅指令 → 执行装配指令 → 回报结果
蓬莱只做装配指令，控制类（控灯）归巴法。
"""
from __future__ import annotations

import logging

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant

from .const import DOMAIN
from .mqtt import PenglaiMqtt
from .service import PenglaiCommandService

_LOGGER = logging.getLogger(__name__)

PLATFORMS: list[Platform] = []


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Penglai from a config entry."""
    data = dict(entry.data)

    # 启动 MQTT 客户端
    mqtt = PenglaiMqtt(hass, data)
    await mqtt.start()

    # 注册指令服务
    service = PenglaiCommandService(hass, mqtt)
    mqtt.set_cmd_handler(service.handle)

    # 注册集成服务（供 HA 内部调用/脚本）
    async def async_publish_cmd(call):
        """服务: 发布蓬莱指令（脚本/自动化可调用）。"""
        cmd = call.data.get("cmd", "")
        params = call.data.get("params", {})
        payload = {"type": cmd, "params": params, "device_id": data.get("device_id", "")}
        mqtt.publish_result(payload)

    hass.services.async_register(DOMAIN, "publish_cmd", async_publish_cmd)

    # 存储实例
    hass.data.setdefault(DOMAIN, {})
    hass.data[DOMAIN][entry.entry_id] = {
        "mqtt": mqtt,
        "service": service,
    }

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    instance = hass.data[DOMAIN].pop(entry.entry_id, None)
    if instance:
        await instance["mqtt"].stop()
    return True
