"""Penglai (蓬莱) 装配指令执行服务.

借鉴 bemfa 的服务模式：接收 MQTT 指令 → 调用 HA 服务 → 回报结果。
蓬莱只做装配指令（低频配置型），控制类指令归巴法。

v0.2 新增：
- login_ha: 平台下发 refresh_token → 自动安装/确认 haier 集成 → import flow 创建 config entry
- bind_device: 确认 haier entry 存在并重载 → 返回设备实体概览
"""
from __future__ import annotations

import asyncio
import json
import logging
import shutil
from pathlib import Path
from typing import Any

from homeassistant.config_entries import SOURCE_IMPORT
from homeassistant.core import HomeAssistant, ServiceCall

from .const import (
    CMD_BIND_DEVICE,
    CMD_CREATE_AUTOMATION,
    CMD_LIST_STATES,
    CMD_LOGIN_HA,
    CMD_PING,
    CMD_REBOOT_INTEGRATION,
    CMD_SET_SCENE,
    CMD_SYNC_STATUS,
    DOMAIN,
    RESULT_FAIL,
    RESULT_OK,
)

_LOGGER = logging.getLogger(__name__)

# 支持的装配指令 -> HA 服务调用映射
CMD_TO_HA_SERVICE: dict[str, str] = {
    # haier.login 已移除：haier 集成没有 login 服务，改走专门 handler
    CMD_SET_SCENE: "scene.turn_on",       # 场景切换（装配时验证）
    CMD_REBOOT_INTEGRATION: "homeassistant.reload_config_entry",
}

# haier 集成内置在 penglai 包 vendor/ 下，login_ha 时自动安装到 custom_components
HAIER_DOMAIN = "haier"
HAIER_VENDOR_DIR = Path(__file__).resolve().parent / "vendor" / HAIER_DOMAIN
HAIER_INSTALL_DIR = Path(__file__).resolve().parent.parent / HAIER_DOMAIN


class PenglaiCommandService:
    """装配指令处理器（借鉴 bemfa service 分发模式）。"""

    def __init__(self, hass: HomeAssistant, mqtt) -> None:
        self._hass = hass
        self._mqtt = mqtt

    def handle(self, payload: dict) -> None:
        """MQTT 指令入口（同步回调，转 async 执行）。"""
        cmd = payload.get("type") or payload.get("cmd")
        cmd_id = payload.get("id")
        params = payload.get("params", {})
        device_id = payload.get("device_id", "")
        _LOGGER.info("Penglai 收到指令: %s (id=%s)", cmd, cmd_id)

        if cmd == CMD_PING:
            self._mqtt.publish_result({"id": cmd_id, "cmd": cmd, "status": RESULT_OK, "type": "pong", "device_id": device_id})
            return

        # 指令处理放入 event loop
        asyncio.run_coroutine_threadsafe(
            self._async_execute(cmd, cmd_id, params, device_id),
            self._hass.loop,
        )

    async def _async_execute(self, cmd: str, cmd_id, params: dict, device_id: str) -> None:
        try:
            if cmd == CMD_LIST_STATES:
                result = await self._async_list_states(params)
            elif cmd == CMD_CREATE_AUTOMATION:
                result = await self._async_create_automation(params)
            elif cmd == CMD_SYNC_STATUS:
                result = await self._async_sync_status()
            elif cmd == CMD_LOGIN_HA:
                result = await self._async_login_ha(params)
            elif cmd == CMD_BIND_DEVICE:
                result = await self._async_bind_device(params)
            elif cmd in CMD_TO_HA_SERVICE:
                result = await self._async_call_ha_service(cmd, params)
            else:
                result = {"error": f"未知指令: {cmd}"}
                await self._reply(cmd_id, cmd, RESULT_FAIL, result, device_id)
                return
            await self._reply(cmd_id, cmd, RESULT_OK, result, device_id)
        except Exception as err:  # noqa: BLE001
            _LOGGER.exception("Penglai 指令执行失败: %s", cmd)
            await self._reply(cmd_id, cmd, RESULT_FAIL, {"error": str(err)}, device_id)

    async def _reply(self, cmd_id, cmd: str, status: str, result: dict, device_id: str) -> None:
        payload = {"id": cmd_id, "cmd": cmd, "status": status, "result": result, "device_id": device_id}
        self._mqtt.publish_result(payload)

    # ─────────────────────────────────────────────
    # login_ha / bind_device 专门 handler（v0.2）
    # ─────────────────────────────────────────────

    async def _async_ensure_haier_installed(self) -> dict:
        """确保 haier 集成已安装到 custom_components。

        内置 vendor/ 优先；若 HA 侧已有 haier（如用户手动装过）则跳过。
        返回: {"installed": bool, "source": "vendor"|"existing"|"error"}
        """
        if HAIER_INSTALL_DIR.joinpath("manifest.json").exists():
            return {"installed": True, "source": "existing"}

        if not HAIER_VENDOR_DIR.exists():
            return {"installed": False, "source": "error", "error": "内置 vendor/haier 缺失"}

        try:
            shutil.copytree(HAIER_VENDOR_DIR, HAIER_INSTALL_DIR)
            _LOGGER.info("haier 集成已从 vendor 安装到 %s", HAIER_INSTALL_DIR)
            # 清除 HA loader 对 custom_components 的缓存，否则 async_init 找不到新装的集成
            self._hass.data.pop("custom_components", None)
            return {"installed": True, "source": "vendor"}
        except Exception as err:  # noqa: BLE001
            _LOGGER.exception("haier 集成自动安装失败")
            return {"installed": False, "source": "error", "error": str(err)}

    async def _async_login_ha(self, params: dict) -> dict:
        """接收平台下发的海尔 refresh_token，经 haier 集成 import flow 创建 config entry。

        params: {
            "refresh_token": str,
            "client_id": str,     # 签发 token 的 appId，如 MB-UZHSH-0001
            "app_source": str,    # app / wxapp
        }
        """
        refresh_token = (params or {}).get("refresh_token", "")
        client_id = (params or {}).get("client_id", "")
        app_source = (params or {}).get("app_source", "app")

        if not refresh_token or not client_id:
            return {"error": "缺少 refresh_token 或 client_id"}

        # 1. 确保 haier 集成已安装
        install = await self._async_ensure_haier_installed()
        if not install.get("installed"):
            return {"error": f"haier 集成安装失败: {install.get('error', '未知')}"}

        # 2. 检查是否已有 haier config entry（重复登录则跳过/更新）
        existing = self._hass.config_entries.async_entries(HAIER_DOMAIN)
        if existing:
            entry = existing[0]
            return {
                "entry_exists": True,
                "entry_id": entry.entry_id,
                "title": entry.title,
                "device_count": self._async_haier_device_count(),
            }

        # 3. import flow 免交互创建 entry
        #    配置流字段: client_id / refresh_token / app_source / default_load_all_entity / ignore_device_offline
        #    haier 无自定义 async_step_import → 基类委托 async_step_user → 用 refresh_token 验证并建 entry
        try:
            result = await self._hass.config_entries.flow.async_init(
                HAIER_DOMAIN,
                context={"source": SOURCE_IMPORT},
                data={
                    "client_id": client_id,
                    "refresh_token": refresh_token,
                    "app_source": app_source,
                    "default_load_all_entity": True,
                    "ignore_device_offline": False,
                },
            )
            _LOGGER.info("haier import flow result: %s", result)
            if result.get("type") == "create_entry":
                return {
                    "entry_created": True,
                    "entry_id": result["result"].entry_id,
                    "title": result["result"].title,
                    "device_count": self._async_haier_device_count(),
                }
            if result.get("type") == "abort":
                return {"entry_aborted": True, "reason": result.get("reason"), "detail": result.get("description_placeholders")}
            return {"flow_in_progress": True, "step": result.get("step_id"), "result_type": result.get("type")}
        except Exception as err:  # noqa: BLE001
            _LOGGER.exception("haier import flow 失败")
            return {"error": f"haier import flow 失败: {err}"}

    async def _async_bind_device(self, params: dict) -> dict:
        """确认 haier 集成已加载、设备实体已注册；必要时重载 entry。

        绑定设备的实质：haier 集成 async_setup_entry 时已自动拉取设备列表并注册实体。
        此指令用于后端/前端确认绑定状态。
        """
        existing = self._hass.config_entries.async_entries(HAIER_DOMAIN)
        if not existing:
            return {"error": "未找到 haier config entry，请先执行 login_ha"}

        entry = existing[0]
        reloaded = False
        # 若集成未加载（entry 存在但 haier 域无数据），触发重载
        if HAIER_DOMAIN not in self._hass.data:
            try:
                await self._hass.config_entries.async_reload(entry.entry_id)
                reloaded = True
            except Exception as err:  # noqa: BLE001
                _LOGGER.exception("haier entry 重载失败")
                return {"error": f"haier entry 重载失败: {err}"}

        return {
            "entry_found": True,
            "entry_id": entry.entry_id,
            "title": entry.title,
            "reloaded": reloaded,
            "device_count": self._async_haier_device_count(),
        }

    def _async_haier_device_count(self) -> int:
        """读取 haier 集成加载的设备数（async_setup_entry 后写入 hass.data）。"""
        try:
            return len(self._hass.data.get(HAIER_DOMAIN, {}).get("devices", []))
        except Exception:  # noqa: BLE001
            return 0

    # ─────────────────────────────────────────────
    # 原有 handler
    # ─────────────────────────────────────────────

    async def _async_list_states(self, params: dict) -> dict:
        """读 HA 全量 states 精简上报（100 实体 ≈ 20KB）。"""
        states = self._hass.states.async_all()
        slim = [
            {
                "e": s.entity_id,
                "s": s.state,
                "a": {k: v for k, v in s.attributes.items() if k in ("friendly_name", "unit_of_measurement")},
            }
            for s in states
        ]
        return {"count": len(slim), "states": slim}

    async def _async_sync_status(self) -> dict:
        """同步设备在线状态（回报给蓬莱后端）。"""
        states = self._hass.states.async_all()
        return {
            "count": len(states),
            "ha_version": self._hass.data.get("version", ""),
        }

    async def _async_create_automation(self, params: dict) -> dict:
        """经 WebSocket API 创建自动化（高精度自动化落地）。"""
        try:
            from homeassistant.components.automation import ATTR_ALIAS, ATTR_DESCRIPTION, SERVICE_RELOAD
            from homeassistant.components.websocket_api import (
                async_create_connection,
            )
            from homeassistant.components.websocket_api.messages import (
                async_message_to_json,
            )
        except ImportError:
            # 备用：写 .storage/automations
            return {"error": "websocket 导入失败，走 storage 备用路径", "storage_path": ".storage/automations"}

        # 走 HA REST 服务创建（automation 无标准 create 服务，用 config 条目方式）
        # 简单实现：写入 automations.yaml 并 reload
        name = params.get("name", "penglai_automation")
        trigger = params.get("trigger", [])
        action = params.get("action", [])
        if not trigger or not action:
            return {"error": "trigger/action 不能为空"}

        # 用 script/automation storage 路径（兼容两种存储）
        try:
            from homeassistant.components.automation.config import (
                _async_process_config,
            )
        except ImportError:
            pass

        # 直接经 automation 服务 create? HA 无此服务；写 storage 更稳
        return await self._async_save_automation_storage(name, trigger, action)

    async def _async_save_automation_storage(self, name: str, trigger: list, action: list) -> dict:
        """写 .storage/automations 并 reload（兼容 .storage 模式）。"""
        path = self._hass.config.path(".storage", "automations")
        try:
            import os
            if os.path.exists(path):
                async with self._hass.helpers.storage.Store(self._hass, 1, "automations").async_load as _:
                    pass
            # 简化为直接写文件（HA 会监听 reload）
            data = []
            if os.path.exists(path):
                async with open(path) as f:
                    data = json.load(f).get("data", [])
            new_id = f"penglai_{len(data) + 1}"
            data.append({
                "id": new_id,
                "alias": name,
                "trigger": trigger,
                "action": action,
                "mode": "single",
            })
            with open(path, "w") as f:
                json.dump({"version": 1, "minor_version": 1, "key": "automations", "data": data}, f, indent=2)
            await self._hass.services.async_call("automation", "reload")
            return {"created": new_id, "path": path}
        except Exception as err:  # noqa: BLE001
            return {"error": str(err)}

    async def _async_call_ha_service(self, cmd: str, params: dict) -> dict:
        """调用映射的 HA 服务（借鉴 bemfa 的 service 调用模式）。"""
        service = CMD_TO_HA_SERVICE.get(cmd)
        if not service:
            return {"error": f"无服务映射: {cmd}"}
        domain, _, name = service.partition(".")
        try:
            await self._hass.services.async_call(domain, name, params.get("service_data", {}), blocking=True)
            return {"called": service}
        except Exception as err:  # noqa: BLE001
            return {"error": str(err)}
