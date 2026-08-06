"""Constants for the 蓬莱 integration.

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

# 一键绑定（设备绑定密钥）配置项
CONF_PAIRING_KEY = "pairing_key"      # 设备绑定密钥（32 位 hex，仅绑定窗口内有效）
CONF_API_URL = "api_url"              # 蓬莱后端 API 地址，默认 https://pl.cteaz.top

DEFAULT_TOPIC_PREFIX = "penglai"
DEFAULT_PING_INTERVAL = 30
DEFAULT_PING_MAX_LOST = 3
DEFAULT_API_URL = "https://pl.cteaz.top"

# Topic 模板（借鉴 bemfa: 订阅自身 topic, 发布 {topic}/set）
TOPIC_CMD = "{prefix}/{device_id}/cmd"        # 下行指令（蓬莱后端 → HA agent）
TOPIC_RESULT = "{prefix}/{device_id}/result"  # 上行结果（HA agent → 蓬莱后端）
TOPIC_STATE = "{prefix}/{device_id}/state"    # 设备清单/状态上报（HA agent → 蓬莱后端）
TOPIC_PING = "{prefix}/ping"                  # 旧版心跳（兼容，不带指纹）
TOPIC_HEARTBEAT = "{prefix}/{device_id}/heartbeat"  # 新版心跳（带指纹，后端订阅 penglai/+/heartbeat）

# 指令类型（蓬莱装配指令）
CMD_LOGIN_HA = "login_ha"
CMD_SETUP_HAIER = "setup_haier"
CMD_CONVERT_LIGHTS = "convert_lights"
CMD_SCAN_LIGHTS = "scan_lights"
CMD_BIND_DEVICE = "bind_device"
CMD_SET_SCENE = "set_scene"
CMD_SYNC_STATUS = "sync_status"
CMD_REBOOT_INTEGRATION = "reboot_integration"
CMD_CREATE_AUTOMATION = "create_automation"
CMD_LIST_STATES = "list_states"
CMD_PING = "ping"
CMD_HEARTBEAT = "heartbeat"

# 结果状态
RESULT_OK = "success"
RESULT_FAIL = "failed"

# 消息超时（等待 result 回报）
CMD_TIMEOUT = 30

PLATFORMS: list[Platform] = []
