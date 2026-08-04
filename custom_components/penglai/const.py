"""Constants for the Penglai (蓬莱) integration.

借鉴 bemfa 集成结构。蓬莱 = 装配指令通道（自建 EMQX broker），与巴法控灯分离。
"""
from __future__ import annotations

from homeassistant.const import Platform

DOMAIN = "penglai"
MANUFACTURER = "Penglai"

# 配置项
CONF_BROKER_URL = "broker_url"        # 例: wss://pl.cteaz.top/mqtt 或 mqtt://host:1883
CONF_USERNAME = "username"
CONF_PASSWORD = "password"
CONF_DEVICE_ID = "device_id"          # 设备标识符, 如 runfu-02-01-0203
CONF_TOPIC_PREFIX = "topic_prefix"    # 默认 penglai
CONF_PING_INTERVAL = "ping_interval"  # 心跳间隔秒, 默认 30
CONF_PING_MAX_LOST = "ping_max_lost"  # 心跳丢失阈值, 默认 3

DEFAULT_TOPIC_PREFIX = "penglai"
DEFAULT_PING_INTERVAL = 30
DEFAULT_PING_MAX_LOST = 3

# Topic 模板（借鉴 bemfa: 订阅自身 topic, 发布 {topic}/set）
TOPIC_CMD = "{prefix}/{device_id}/cmd"        # 下行指令（蓬莱后端 → HA agent）
TOPIC_RESULT = "{prefix}/{device_id}/result"  # 上行结果（HA agent → 蓬莱后端）
TOPIC_STATE = "{prefix}/{device_id}/state"    # 设备清单/状态上报（HA agent → 蓬莱后端）
TOPIC_PING = "{prefix}/ping"                  # 心跳（借鉴 bemfa 的 hassping）

# 指令类型（蓬莱装配指令）
CMD_LOGIN_HA = "login_ha"
CMD_BIND_DEVICE = "bind_device"
CMD_SET_SCENE = "set_scene"
CMD_SYNC_STATUS = "sync_status"
CMD_REBOOT_INTEGRATION = "reboot_integration"
CMD_CREATE_AUTOMATION = "create_automation"
CMD_LIST_STATES = "list_states"
CMD_PING = "ping"

# 结果状态
RESULT_OK = "success"
RESULT_FAIL = "failed"

# 消息超时（等待 result 回报）
CMD_TIMEOUT = 30

PLATFORMS: list[Platform] = []
