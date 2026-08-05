"""MQTT 连接管理 for Penglai (蓬莱) 集成.

借鉴 bemfa 集成的 BemfaMqtt 结构：
- 主动出站连接自建 EMQX broker（WSS 穿透 NAT）
- 心跳 ping 检测连接（30s 发送 / 20s 接收 / 3 次丢失判死）
- 指令订阅 + 结果发布
"""
from __future__ import annotations

import asyncio
import json
import logging
import ssl
import time
from typing import Any, Callable

import paho.mqtt.client as mqtt

from .const import (
    CONF_DEVICE_ID,
    CONF_PASSWORD,
    CONF_PING_INTERVAL,
    CONF_PING_MAX_LOST,
    CONF_TOPIC_PREFIX,
    CONF_USERNAME,
    CMD_PING,
    DEFAULT_PING_INTERVAL,
    DEFAULT_PING_MAX_LOST,
    DEFAULT_TOPIC_PREFIX,
    RESULT_FAIL,
    RESULT_OK,
    TOPIC_CMD,
    TOPIC_PING,
    TOPIC_RESULT,
    TOPIC_STATE,
)

_LOGGER = logging.getLogger(__name__)

# 心跳参数（借鉴 bemfa const.py: INTERVAL_PING_SEND/RECEIVE/MAX_PING_LOST）
INTERVAL_PING_SEND = 30       # 每 30s 发送一次心跳
INTERVAL_PING_RECEIVE = 20    # 超过 20s 未收到心跳判定丢失一次
MAX_PING_LOST = 3             # 连续丢失 3 次判定连接失效


class PenglaiMqtt:
    """Penglai MQTT 客户端（借鉴 BemfaMqtt）。"""

    def __init__(self, hass, entry_data: dict[str, Any]) -> None:
        self._hass = hass
        self._broker_url = entry_data.get("broker_url", "wss://pl.cteaz.top/mqtt")
        self._username = entry_data.get(CONF_USERNAME, "")
        self._password = entry_data.get(CONF_PASSWORD, "")
        self._device_id = entry_data.get(CONF_DEVICE_ID, "")
        self._prefix = entry_data.get(CONF_TOPIC_PREFIX, DEFAULT_TOPIC_PREFIX)
        self._ping_interval = entry_data.get(CONF_PING_INTERVAL, DEFAULT_PING_INTERVAL)
        self._ping_max_lost = entry_data.get(CONF_PING_MAX_LOST, DEFAULT_PING_MAX_LOST)

        # topic 计算
        self._topic_cmd = TOPIC_CMD.format(prefix=self._prefix, device_id=self._device_id)
        self._topic_result = TOPIC_RESULT.format(prefix=self._prefix, device_id=self._device_id)
        self._topic_state = TOPIC_STATE.format(prefix=self._prefix, device_id=self._device_id)
        self._topic_ping = TOPIC_PING.format(prefix=self._prefix)

        self._client: mqtt.Client | None = None
        self._connected = False
        self._lost_ping_count = 0
        self._last_receive_time: float | None = None
        self._ping_send_task: asyncio.Task | None = None
        self._ping_check_task: asyncio.Task | None = None
        self._cmd_handler: Callable[[dict], None] | None = None

        self._setup_client()

    def _setup_client(self) -> None:
        """初始化 paho 客户端（WSS 或 TCP）。"""
        parsed = self._parse_broker_url(self._broker_url)
        transport = "websockets" if parsed["scheme"] in ("wss", "ws") else "tcp"

        self._client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=self._device_id,
            protocol=mqtt.MQTTv311,
            transport=transport,
        )
        if parsed["scheme"] == "wss":
            self._client.ws_set_options(path=parsed.get("path", "/mqtt"))
            self._client.tls_set(cert_reqs=ssl.CERT_NONE)
            self._client.tls_insecure_set(True)
        if self._username:
            self._client.username_pw_set(self._username, self._password)

        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message = self._on_message

        self._host = parsed["host"]
        self._port = parsed["port"]

    @staticmethod
    def _parse_broker_url(url: str) -> dict:
        """解析 broker URL，支持 wss://host:port/path 和 mqtt://host:port。"""
        scheme, rest = url.split("://", 1)
        if "/" in rest:
            host_port, path = rest.split("/", 1)
            path = "/" + path
        else:
            host_port, path = rest, ""
        host, _, port_s = host_port.partition(":")
        port = int(port_s) if port_s else (443 if scheme == "wss" else 1883)
        return {"scheme": scheme, "host": host, "port": port, "path": path}

    async def start(self) -> None:
        """启动连接与心跳任务。"""
        self._connected = False
        await self._hass.async_add_executor_job(self._connect)
        self._ping_send_task = self._hass.async_create_task(self._ping_send_loop())
        self._ping_check_task = self._hass.async_create_task(self._ping_check_loop())

    def _connect(self) -> None:
        """同步连接（executor 中运行）。"""
        try:
            self._client.connect(self._host, self._port, keepalive=INTERVAL_PING_SEND * 2)
            self._client.loop_start()
        except Exception as err:  # noqa: BLE001
            _LOGGER.error("Penglai MQTT 连接失败: %s", err)

    async def stop(self) -> None:
        """停止连接与任务。"""
        for task in (self._ping_send_task, self._ping_check_task):
            if task:
                task.cancel()
        await self._hass.async_add_executor_job(self._disconnect)

    def _disconnect(self) -> None:
        if self._client:
            try:
                self._client.loop_stop()
                self._client.disconnect()
            except Exception:  # noqa: BLE001
                pass

    # ---- 回调 ----

    def _on_connect(self, client, userdata, flags, reason_code, properties=None):
        if reason_code == 0:
            self._connected = True
            self._last_receive_time = time.monotonic()
            _LOGGER.info("Penglai MQTT 已连接: %s", self._broker_url)
            # 订阅指令 topic（借鉴 bemfa: 订阅自身 topic）
            client.subscribe(self._topic_cmd, qos=1)
            # 上线发布心跳
            self.publish(self._topic_ping, json.dumps({"type": CMD_PING, "device_id": self._device_id, "ts": _now()}))
        else:
            _LOGGER.error("Penglai MQTT 连接被拒 rc=%s", reason_code)

    def _on_disconnect(self, client, userdata, reason_code, properties=None):
        self._connected = False
        _LOGGER.warning("Penglai MQTT 断开 rc=%s", reason_code)

    def _on_message(self, client, userdata, msg):
        self._last_receive_time = time.monotonic()
        self._lost_ping_count = 0
        try:
            payload = json.loads(msg.payload.decode())
        except (json.JSONDecodeError, UnicodeDecodeError):
            _LOGGER.warning("Penglai MQTT 收到非 JSON 消息: %s", msg.payload[:200])
            return
        if self._cmd_handler:
            self._cmd_handler(payload)

    # ---- 心跳（借鉴 bemfa: _ping_send_loop / _ping_check_loop）----

    async def _ping_send_loop(self) -> None:
        """每 30s 发布心跳。"""
        while True:
            await asyncio.sleep(INTERVAL_PING_SEND)
            if self._connected:
                self.publish(self._topic_ping, json.dumps({"type": CMD_PING, "device_id": self._device_id, "ts": _now()}))

    async def _ping_check_loop(self) -> None:
        """每 20s 检查心跳超时，连续 3 次丢失则重连。"""
        while True:
            await asyncio.sleep(INTERVAL_PING_RECEIVE)
            if not self._connected:
                continue
            now = time.monotonic()
            if self._last_receive_time is None:
                self._last_receive_time = now
                continue
            if now - self._last_receive_time > INTERVAL_PING_SEND:
                self._lost_ping_count += 1
                _LOGGER.warning("Penglai MQTT 心跳丢失 %d/%d", self._lost_ping_count, self._ping_max_lost)
                if self._lost_ping_count >= self._ping_max_lost:
                    _LOGGER.error("Penglai MQTT 心跳连续丢失，触发重连")
                    await self._reconnect()
            else:
                self._lost_ping_count = 0

    async def _reconnect(self) -> None:
        """断线重连。"""
        self._connected = False
        await self._hass.async_add_executor_job(self._disconnect)
        await asyncio.sleep(1)
        await self._hass.async_add_executor_job(self._connect)

    # ---- 发布/订阅 ----

    def publish(self, topic: str, payload: str, qos: int = 1) -> None:
        """发布消息（线程安全，loop 中运行）。"""
        if self._client and self._connected:
            self._client.publish(topic, payload, qos=qos)

    def publish_result(self, payload: dict) -> None:
        """发布执行结果到 result topic。"""
        self.publish(self._topic_result, json.dumps(payload))

    def publish_state(self, payload: dict) -> None:
        """发布设备状态/清单到 state topic。"""
        self.publish(self._topic_state, json.dumps(payload))

    def set_cmd_handler(self, handler: Callable[[dict], None]) -> None:
        """设置指令处理回调。"""
        self._cmd_handler = handler


def _now() -> float:
    import time
    return time.time()
