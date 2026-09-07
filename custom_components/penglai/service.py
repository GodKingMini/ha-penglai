"""蓬莱 装配指令执行服务.

借鉴 bemfa 的服务模式：接收 MQTT 指令 → 调用 HA 服务 → 回报结果。
蓬莱只做装配指令（低频配置型），控制类指令归巴法。

v0.2 新增：
- login_ha: 平台下发 refresh_token → 自动安装/确认 haier 集成 → import flow 创建 config entry
- bind_device: 确认 haier entry 存在并重载 → 返回设备实体概览
"""
from __future__ import annotations

import asyncio
import importlib
import json
import logging
import re
import shutil
import sys
from pathlib import Path
from typing import Any

from homeassistant.config_entries import SOURCE_IMPORT, SOURCE_USER
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.helpers import device_registry, entity_registry

from .const import (
    CMD_BIND_DEVICE,
    CMD_CONVERT_LIGHTS,
    CMD_CREATE_AUTOMATION,
    CMD_FETCH_DEVICES,
    CMD_FIX_LIGHT_SYNC,
    CMD_LIST_STATES,
    CMD_LOGIN_HA,
    CMD_PING,
    CMD_REBOOT_INTEGRATION,
    CMD_SCAN_LIGHTS,
    CMD_SET_SCENE,
    CMD_SETUP_HAIER,
    CMD_SYNC_BEMFA,
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
            elif cmd == CMD_FETCH_DEVICES:
                result = await self._async_fetch_devices(params)
            elif cmd == CMD_CREATE_AUTOMATION:
                result = await self._async_create_automation(params)
            elif cmd == CMD_SYNC_STATUS:
                result = await self._async_sync_status()
            elif cmd == CMD_LOGIN_HA:
                result = await self._async_login_ha(params)
            elif cmd == CMD_SETUP_HAIER:
                result = await self._async_setup_haier(params)
            elif cmd == CMD_CONVERT_LIGHTS:
                result = await self._async_convert_lights(params)
            elif cmd == CMD_FIX_LIGHT_SYNC:
                result = await self._async_fix_light_sync(params)
            elif cmd == CMD_SYNC_BEMFA:
                result = await self._async_sync_bemfa(params)
            elif cmd == CMD_SCAN_LIGHTS:
                result = await self._async_scan_lights(params)
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

        内置 vendor/ 始终覆盖安装（dirs_exist_ok），保证旧版（缺
        async_step_import 修复）被替换；随后清除 loader 缓存使新文件生效。
        返回: {"installed": bool, "source": "vendor"|"error"}
        """
        if not HAIER_VENDOR_DIR.exists():
            return {"installed": False, "source": "error", "error": "内置 vendor/haier 缺失"}

        try:
            shutil.copytree(HAIER_VENDOR_DIR, HAIER_INSTALL_DIR, dirs_exist_ok=True)
            _LOGGER.info("haier 集成已从 vendor 安装/更新到 %s", HAIER_INSTALL_DIR)
            # 清除 HA loader 对 custom_components 的缓存，否则 async_init 找不到新装的集成
            self._hass.data.pop("custom_components", None)
            # 清除 Python 模块缓存（sys.modules）——仅覆盖磁盘文件不够，
            # importlib.import_module 会命中内存中已加载的旧 HaierConfigFlow 类，
            # 导致 "Handler HaierConfigFlow doesn't support step import" 反复出现。
            for mod_name in [
                m for m in sys.modules if m == "custom_components.haier"
                or m.startswith("custom_components.haier.")
            ]:
                sys.modules.pop(mod_name, None)
            importlib.invalidate_caches()
            _LOGGER.info("haier 模块缓存已清理 (sys.modules)")
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
        #    vendor/haier config_flow 提供 async_step_import（补默认值后转发 async_step_user）→ 免交互建 entry
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

    async def _async_setup_haier(self, params: dict) -> dict:
        """一键装配海尔：删除旧 entry（修复坏 token）→ import 重建 → 等待设备加载完成。

        合并原 login_ha + bind_device 的职责，单条指令完成海尔集成创建与验证：
        1. 确保 haier 集成已安装（vendor 最新，含 async_step_import 修复）
        2. 删除所有现有 haier config entry —— 旧 entry 的 token/refresh_token 若已
           失效或 app_source 与签发 appId 不匹配（如 "Token不是由此应用创建"），
           删除重建即彻底规避；幂等可重复执行
        3. 经 import flow 用平台新下发的 refresh_token 创建 entry
        4. 轮询等待 async_setup_entry 拉取设备，返回 device_count 作为绑定验证

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

        # 1. 确保 haier 集成已安装（vendor 强制覆盖）
        install = await self._async_ensure_haier_installed()
        if not install.get("installed"):
            return {"error": f"haier 集成安装失败: {install.get('error', '未知')}"}

        # 2. 删除旧 haier entry（幂等重建）
        removed = []
        for entry in self._hass.config_entries.async_entries(HAIER_DOMAIN):
            try:
                await self._hass.config_entries.async_remove(entry.entry_id)
                removed.append(entry.entry_id)
                _LOGGER.info("haier 旧 entry 已删除: %s (%s)", entry.title, entry.entry_id)
            except Exception as err:  # noqa: BLE001
                _LOGGER.warning("haier 旧 entry 删除失败 %s: %s", entry.entry_id, err)
        if removed:
            # 卸载后清残留数据，避免新 entry setup 读到旧 devices
            self._hass.data.pop(HAIER_DOMAIN, None)

        # 3. import flow 免交互创建 entry
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
        except Exception as err:  # noqa: BLE001
            _LOGGER.exception("haier import flow 失败")
            return {"error": f"haier import flow 失败: {err}"}

        if result.get("type") == "abort":
            return {
                "entry_aborted": True,
                "removed_entries": removed,
                "reason": result.get("reason"),
                "detail": result.get("description_placeholders"),
            }
        if result.get("type") != "create_entry":
            return {
                "flow_in_progress": True,
                "removed_entries": removed,
                "step": result.get("step_id"),
                "result_type": result.get("type"),
            }

        entry = result["result"]

        # 4. 等待 async_setup_entry 异步拉取设备（最多 30s）
        device_count = 0
        for _ in range(60):
            try:
                haier_data = self._hass.data.get(HAIER_DOMAIN)
                if haier_data and haier_data.get("devices"):
                    device_count = len(haier_data["devices"])
                    if device_count > 0:
                        break
            except Exception:  # noqa: BLE001
                pass
            await asyncio.sleep(0.5)

        return {
            "entry_created": True,
            "removed_entries": removed,
            "entry_id": entry.entry_id,
            "title": entry.title,
            "device_count": device_count,
            "setup_complete": device_count > 0,
        }

    def _async_haier_device_count(self) -> int:
        """读取 haier 集成加载的设备数（async_setup_entry 后写入 hass.data）。"""
        try:
            return len(self._hass.data.get(HAIER_DOMAIN, {}).get("devices", []))
        except Exception:  # noqa: BLE001
            return 0

    # ─────────────────────────────────────────────
    # scan_lights：扫描海尔灯 switch 候选清单（v0.4）
    # ─────────────────────────────────────────────

    def _async_switch_as_x_map(self) -> dict[str, str]:
        """构建 {源 switch entity_id: light entity_id} 映射。

        HA switch_as_x 集成（2021.12~master 共 6 版本）均不暴露 light 实体
        source_entity 属性——源实体存于 config_entry.options[CONF_ENTITY_ID]。
        蓬莱不能靠实体属性反查，必须从 config entry options + entity_registry
        关联得到转换结果（v0.5.6 修复）。
        """
        result: dict[str, str] = {}
        try:
            reg = entity_registry.async_get(self._hass)
        except Exception:  # noqa: BLE001
            return result
        for entry in self._hass.config_entries.async_entries("switch_as_x"):
            src = (entry.options or entry.data or {}).get("entity_id")
            if not src:
                continue
            for entity in reg.entities.values():
                if (
                    entity.config_entry_id == entry.entry_id
                    and entity.platform == "switch_as_x"
                    and entity.entity_id.startswith("light.")
                ):
                    result[src] = entity.entity_id
                    break
        return result

    async def _async_scan_lights(self, params: dict) -> dict:
        """扫描海尔灯相关 switch 实体，上报候选清单（供前端勾选精准转换）。

        params: {
            "keyword": str,   # 可选，过滤 friendly_name 包含关键字
            "limit": int,     # 可选，返回上限（默认 200）
        }
        返回: {
            "candidates": [{"entity_id","friendly_name","matched","already_light","state"}],
            "total_switch": int,
            "already_light": int,
        }
        候选 = 名称含灯关键字且未被排除词的 switch（同 convert_lights 规则），
        无论后缀是否命中都上报，便于前端看到全部可转换面板灯。
        """
        keyword = (params or {}).get("keyword", "")
        limit = int((params or {}).get("limit", 200) or 200)

        # 已转换映射：源 switch → light（v0.5.6 修复：HA 不暴露 source_entity 属性，
        # 改从 switch_as_x config entry options + entity_registry 关联构建）
        sax_map = self._async_switch_as_x_map()
        existing_sources = set(sax_map.keys())

        # 已有 switch_as_x config entry（entry 已建但 light 未生成也视为已转换）
        existing_entry_sources = set()
        for entry in self._hass.config_entries.async_entries("switch_as_x"):
            src = (entry.options or entry.data or {}).get("entity_id")
            if src:
                existing_entry_sources.add(src)

        candidates = []
        total_switch = 0
        for s in self._hass.states.async_all():
            eid = s.entity_id
            if not eid.startswith("switch."):
                continue
            total_switch += 1
            fn = s.attributes.get("friendly_name", "")
            if not fn:
                continue
            if any(k in fn for k in self._LIGHT_EXCLUDE_KEYWORDS):
                continue
            # 灯判定：关键词命中 或 以「灯」结尾且非状态/开关类
            is_light = any(k in fn for k in self._LIGHT_NAME_KEYWORDS) or (
                fn.endswith("灯") and not any(x in fn for x in ("指示", "状态", "开关"))
            )
            if not is_light:
                continue
            suffix_hit = bool(self._LIGHT_SWITCH_SUFFIX_RE.search(eid))
            if keyword and keyword not in fn:
                continue
            candidates.append({
                "entity_id": eid,
                "friendly_name": fn,
                "matched": is_light,
                "suffix_hit": suffix_hit,
                "already_light": eid in existing_sources or eid in existing_entry_sources,
                "state": s.state,
            })
            if len(candidates) >= limit:
                break

        return {
            "candidates": candidates,
            "total_switch": total_switch,
            "already_light": sum(1 for c in candidates if c["already_light"]),
        }

    # ─────────────────────────────────────────────
    # convert_lights：海尔灯 switch→light（v0.3 / v0.4 支持精准列表）
    # ─────────────────────────────────────────────

    # 海尔面板灯 switch 状态后缀（英文旧面板 / 拼音集成，均可带 _N 去重）
    # 灯必定绑定「开关机状态」onoffstatus（09-06 实测：物理按键翻转的是 onOffStatus，
    # 绑 alwaysonstatus 通断电会不同步）。后缀只保留开关机族，排除通断电族。
    _LIGHT_SWITCH_SUFFIX_RE = re.compile(
        r"_(onoffstatus|kai_guan_ji_zhuang_tai)(_\d+)?$"
    )
    # 灯具名称关键词（02_convert_lights.py 规则）
    _LIGHT_NAME_KEYWORDS = [
        "灯带", "射灯", "主灯", "镜灯", "柜灯", "衣帽间灯", "淋浴射灯",
        "过道射灯", "玄关灯", "儿童房灯", "餐厅主灯", "厨房灯带", "厨房射灯",
        "背景灯带", "阳台灯", "凉霸照明",
    ]
    # 排除词（09-06 修正：移除「开关机」——onoffstatus 即开关机状态，是灯的正确绑定源；
    # 旧认知「仅通断电有效」已被全屋实测推翻。「反转」保留以挡 revonoffstatus 反转实体）
    _LIGHT_EXCLUDE_KEYWORDS = [
        "指示灯", "反转", "场景", "空调", "地暖", "新风", "网关",
        "洗衣机", "干衣机", "冰箱", "窗帘", "布帘", "纱帘", "启用", "双控",
    ]

    async def _async_convert_lights(self, params: dict) -> dict:
        """海尔灯 switch → light（对应 ha_manager 02_convert_lights.py）。

        params: {
            "entity_ids": [str],  # 可选。指定要转换的 switch entity_id 列表（精准模式）
                                 # 未提供时：自动发现全部海尔面板灯 switch（一键模式）
        }
        流程：
        1. 发现海尔面板灯 switch（后缀 + 名称关键词 + 排除词）或按 entity_ids 精准选取
        2. Switch as X config flow 转 light（防重复：已转换/已建 entry 则跳过）
        """
        # ── 1. 确定目标 switch 列表 ──
        entity_ids = (params or {}).get("entity_ids") or []

        # 已转换映射：源 switch → light（v0.5.6 修复：HA 不暴露 source_entity 属性，
        # 改从 switch_as_x config entry options + entity_registry 关联构建）
        sax_map = self._async_switch_as_x_map()
        existing_sources = set(sax_map.keys())

        # 已有 switch_as_x config entry 的 source_entity（entry 已建但 light 未生成时
        # 也要防重复转换——否则每次执行都重建一个坏 entry）
        existing_entry_sources = set()
        for entry in self._hass.config_entries.async_entries("switch_as_x"):
            src = (entry.options or entry.data or {}).get("entity_id")
            if src:
                existing_entry_sources.add(src)

        targets = []
        if entity_ids:
            # 精准模式：只处理指定列表（同样应用排除词过滤 + 已转换去重）
            id_set = {e.strip() for e in entity_ids if isinstance(e, str) and e.strip()}
            for s in self._hass.states.async_all():
                eid = s.entity_id
                if eid not in id_set:
                    continue
                fn = s.attributes.get("friendly_name", "") or eid
                if any(k in fn for k in self._LIGHT_EXCLUDE_KEYWORDS):
                    skipped.append({
                        "entity_id": eid, "friendly_name": fn,
                        "reason": "名称含排除词（开关机状态等），无需转换",
                    })
                    continue
                targets.append({"entity_id": eid, "friendly_name": fn})
        else:
            # 一键模式：自动发现（后缀 + 名称关键词 + 排除词）
            for s in self._hass.states.async_all():
                eid = s.entity_id
                if not eid.startswith("switch."):
                    continue
                if not self._LIGHT_SWITCH_SUFFIX_RE.search(eid):
                    continue
                fn = s.attributes.get("friendly_name", "")
                if not fn:
                    continue
                if any(k in fn for k in self._LIGHT_EXCLUDE_KEYWORDS):
                    continue
                # 09-06：onoffstatus 的 friendly_name 是「开关机状态」，不含灯名关键词，
                # 故后缀已是 onoffstatus 即直接认可；否则退回 FN 灯具关键词判定。
                is_light = (
                    "_onoffstatus" in eid or "kai_guan_ji_zhuang_tai" in eid
                    or any(k in fn for k in self._LIGHT_NAME_KEYWORDS)
                    or (
                        fn.endswith("灯")
                        and not any(x in fn for x in ("指示", "状态", "开关"))
                    )
                )
                if not is_light:
                    continue
                targets.append({"entity_id": eid, "friendly_name": fn})

        # ── 2. Switch as X 转 light ──
        converted, skipped, failed = [], [], []
        created_entries = []  # (t, eid) 已创建 entry，统一等待后反查
        for t in targets:
            eid = t["entity_id"]
            if eid in existing_sources or eid in existing_entry_sources:
                skipped.append({**t, "reason": "已存在 light"})
                continue
            try:
                # 关键：SchemaConfigFlowHandler 一步即建 entry（schema 含 target_domain），
                # 必须 init 时就传全参数，否则 options 缺 target_domain → 实体 setup 失败。
                flow = await self._hass.config_entries.flow.async_init(
                    "switch_as_x",
                    context={"source": SOURCE_USER},
                    data={
                        "entity_id": eid,
                        "target_domain": "light",
                        "invert": False,
                    },
                )
                if flow.get("type") == "form":
                    step = await self._hass.config_entries.flow.async_configure(
                        flow["flow_id"],
                        {"target_domain": "light", "invert": False},
                    )
                else:
                    step = flow
                if step.get("type") == "create_entry":
                    # ConfigEntry 无 entity_id 属性——实体异步生成，
                    # 批量创建完统一等待（一次 async_block_till_done 覆盖全部 entry），
                    # 避免逐个等待时事件循环排空慢导致累计超时。
                    created_entries.append((t, eid))
                elif step.get("type") == "abort":
                    skipped.append({**t, "reason": step.get("reason", "abort")})
                else:
                    failed.append({**t, "reason": f"flow type={step.get('type')}"})
            except Exception as err:  # noqa: BLE001
                _LOGGER.exception("Switch as X 转换失败: %s", eid)
                failed.append({**t, "reason": str(err)})

        # ── 3. 统一等待事件循环排空（10s 上限，防实例同步设备多时长期阻塞）──
        if created_entries:
            try:
                await asyncio.wait_for(
                    self._hass.async_block_till_done(), timeout=10
                )
            except asyncio.TimeoutError:
                _LOGGER.warning(
                    "Switch as X 转换后等待事件循环排空超时(10s)，按当前状态反查实体"
                )
            for t, eid in created_entries:
                # v0.5.6 修复：不再依赖 light 实体 source_entity 属性（HA 从不暴露），
                # 直接从 config entry options + entity_registry 关联反查
                found = sax_map.get(eid)
                if found:
                    converted.append({**t, "entity_id": found})
                else:
                    failed.append({**t, "reason": "entry 已创建但未发现 light 实体"})

        return {
            "mode": "precise" if entity_ids else "auto",
            "requested": len(entity_ids) if entity_ids else len(targets),
            "discovered": len(targets),
            "converted": converted,
            "skipped": skipped,
            "failed": failed,
        }

    # ─────────────────────────────────────────────
    # fix_light_sync：存量灯一键修复（v0.5.8）
    # 背景：旧版 convert_lights 把灯绑到 alwaysonstatus（通断电）→ 物理按键不同步；
    #       且存量灯重跑 convert_lights 会被 existing_entry_sources 去重静默跳过。
    #       本 handler = 09-06 御主家手动脚本（ha_promote_a/b）验证过的两步修复：
    #       A: switchType 常开常闭开关(2) → 普通开关(1)（select_option）
    #       B: switch_as_x entry 换绑 _onoffstatus（物理按键翻转的是 onOffStatus）
    # 幂等：已绑 onoffstatus 的 entry 不在 bad_entries 内，重复执行 found=0 直接返回。
    # ─────────────────────────────────────────────

    # 坏绑定源后缀 = 通断电族（旧 convert 曾选反；绑它则灯不跟随物理按键）
    _BAD_SOURCE_RE = re.compile(
        r"_(alwaysonstatus|tong_duan_dian_zhuang_tai)(_\d+)?$"
    )
    # 对应好绑定后缀（开关机族）
    _GOOD_SOURCE_MAP = {
        "alwaysonstatus": "onoffstatus",
        "tong_duan_dian_zhuang_tai": "kai_guan_ji_zhuang_tai",
    }

    async def _async_fix_light_sync(self, params: dict) -> dict:
        """存量灯修复：A 转普通开关 + B 换绑 onoffstatus（幂等，可重复执行）。

        params: {
            "dry_run": bool,  # 可选，true=只扫描不改动
        }
        返回: {
            "found": int,              # 发现绑错设备数
            "dry_run": bool,           # 仅 dry_run 时返回
            "detail": [...],           # dry_run 时设备清单
            "step_a_switchtype": [...],# A 结果 [{device, select_entity, ok, error}]
            "step_b_rebind": [...],    # B 结果 [{device, old, new, ok, error}]
            "skipped": [...],          # 异常跳过
            "changed": int, "failed": int,
        }
        """
        dry_run = bool((params or {}).get("dry_run", False))

        # ── 1. 收集存量错误绑定：switch_as_x entry 绑通断电族源的设备 ──
        # 读 config_entries（生产权威；switch_as_x 绑定关系在 options/ data 的 entity_id，
        # 新版集成 data={}，绑定只在 options —— 两处都查兼容新旧）
        bad_entries = {}  # dev 前缀 -> {"entry": entry, "old_src": src, "src_field": "options"|"data"}
        for entry in self._hass.config_entries.async_entries("switch_as_x"):
            opts = entry.options or {}
            data = entry.data or {}
            src = opts.get("entity_id") or data.get("entity_id") or ""
            if not self._BAD_SOURCE_RE.search(src):
                continue
            dev = self._BAD_SOURCE_RE.sub("", src.replace("switch.", ""))
            if not dev:
                continue
            bad_entries[dev] = {
                "entry": entry,
                "old_src": src,
                "src_field": "options" if opts.get("entity_id") else "data",
            }

        result = {
            "found": len(bad_entries),
            "step_a_switchtype": [],
            "step_b_rebind": [],
            "skipped": [],
        }
        if dry_run:
            result["dry_run"] = True
            result["detail"] = [
                {"device": d, "old_src": v["old_src"]}
                for d, v in bad_entries.items()
            ]
            return result

        # ── 2. Step A: 逐台 select_option 转普通开关（必须 A→B 顺序，缺一不可）──
        for dev, info in bad_entries.items():
            sel = f"select.{dev}_switchtype"
            try:
                await self._hass.services.async_call(
                    "select", "select_option",
                    {"entity_id": sel, "option": "普通开关"},
                    blocking=True, timeout=15,
                )
                info["step_a_ok"] = True
                result["step_a_switchtype"].append(
                    {"device": dev, "select_entity": sel, "ok": True})
            except Exception as err:  # noqa: BLE001
                info["step_a_ok"] = False
                result["step_a_switchtype"].append(
                    {"device": dev, "select_entity": sel, "ok": False,
                     "error": str(err)})

        # ── 3. Step B: 换绑 config entry（options/ data 同源替换 → onoffstatus 族）──
        for dev, info in bad_entries.items():
            entry = info["entry"]
            src = info["old_src"]
            bad_suffix = self._BAD_SOURCE_RE.search(src).group(1)  # type: ignore[union-attr]
            good_suffix = self._GOOD_SOURCE_MAP.get(bad_suffix, "onoffstatus")
            # 同源替换（含 _N 去重后缀场景：src.replace 只动坏后缀段，_N 自然保留）
            new_src = src.replace(f"_{bad_suffix}", f"_{good_suffix}")
            try:
                if info["src_field"] == "options":
                    new_opts = dict(entry.options or {})
                    new_opts["entity_id"] = new_src
                    self._hass.config_entries.async_update_entry(
                        entry, options=new_opts)
                else:
                    new_data = dict(entry.data or {})
                    new_data["entity_id"] = new_src
                    self._hass.config_entries.async_update_entry(
                        entry, data=new_data)
                info["step_b_ok"] = True
                result["step_b_rebind"].append(
                    {"device": dev, "old": src, "new": new_src, "ok": True})
            except Exception as err:  # noqa: BLE001
                info["step_b_ok"] = False
                result["step_b_rebind"].append(
                    {"device": dev, "old": src, "new": new_src,
                     "ok": False, "error": str(err)})

        # ── 4. 等 entry 重载/实体重建后汇报（switch_as_x 监听 options 变更自动 reload）──
        if result["step_b_rebind"]:
            await asyncio.sleep(3)
        result["changed"] = sum(1 for x in result["step_b_rebind"] if x["ok"])
        result["failed"] = sum(1 for x in result["step_b_rebind"] if not x["ok"])
        return result

    async def _async_sync_bemfa(self, params: dict) -> dict:
        """同步 Light 灯具到巴法集成（仅 light 域，topic 驱动）。

        params: {
            "uid": str,    # 可选。巴法 UID（32 位 hex）。bemfa 集成未登录时自动编程式登录
            "limit": int,  # 可选。同步数量上限（默认全部 light）
        }
        流程：
        1. 确保 bemfa 集成已登录（未登录且有 uid → config flow 自动建 entry）
        2. 拉取巴法云端已有 topic（幂等去重）
        3. collect_supported_syncs() 收集全部 → 过滤仅 light 域（御主约束：只同步转换好的灯具）
        4. 逐个 async_create_sync（内部自动建 topic + MQTT 通道 + 发布当前状态）
        """
        params = params or {}
        uid = str(params.get("uid") or "").strip()
        limit = params.get("limit")

        # ── 1. 确保 bemfa 集成已登录 ──
        bemfa_data = self._hass.data.get("bemfa") or {}
        if not bemfa_data:
            if not uid or not re.fullmatch(r"[0-9a-f]{32}", uid):
                return {
                    "success": False,
                    "error": "bemfa 集成未登录，且未提供有效 uid（32 位 hex）",
                    "hint": "请在 HA 添加 bemfa 集成，或本指令带 uid 参数自动登录",
                }
            try:
                flow = await self._hass.config_entries.flow.async_init(
                    "bemfa",
                    context={"source": SOURCE_USER},
                    data={"uid": uid},
                )
                if flow.get("type") != "create_entry":
                    return {
                        "success": False,
                        "error": f"bemfa 登录失败: {flow.get('type')} {flow.get('reason', '')}",
                    }
                # 等 bemfa entry setup 完成（async_start 会连接 MQTT）
                # 注意：不能用 async_block_till_done()——HA 全局任务永不停歇（haier 等），必然超时
                for _ in range(30):  # 最多等 15s（0.5s 步进）
                    bemfa_data = self._hass.data.get("bemfa") or {}
                    if bemfa_data:
                        break
                    await asyncio.sleep(0.5)
                else:
                    await asyncio.sleep(1)
                bemfa_data = self._hass.data.get("bemfa") or {}
            except Exception as err:  # noqa: BLE001
                _LOGGER.exception("bemfa 登录异常")
                return {"success": False, "error": f"bemfa 登录异常: {err}"}

        if not bemfa_data:
            return {"success": False, "error": "bemfa 集成已创建但 service 未就绪"}

        service = next(iter(bemfa_data.values()))["service"]

        # ── 2. 拉取巴法云端已有 topic（幂等去重）──
        try:
            all_topics = await service.async_fetch_all_topics()
        except Exception as err:  # noqa: BLE001
            _LOGGER.exception("拉取巴法 topic 失败")
            return {"success": False, "error": f"拉取巴法 topic 失败: {err}"}

        # ── 3. 收集全部 syncs → 过滤仅 light 域（御主约束）──
        syncs = service.collect_supported_syncs()
        light_syncs = [s for s in syncs if s.entity_id.startswith("light.")]
        if limit and int(limit) > 0:
            light_syncs = light_syncs[: int(limit)]

        # ── 4. 逐个创建（建 topic + MQTT 通道 + 发布状态）──
        created, skipped, failed = [], [], []
        for sync in light_syncs:
            if sync.topic in all_topics:
                skipped.append({
                    "entity_id": sync.entity_id,
                    "topic": sync.topic,
                    "name": all_topics[sync.topic],
                    "reason": "topic 已存在",
                })
                continue
            try:
                name = sync.name or sync.entity_id
                await service.async_create_sync(sync, {"name": name})
                created.append({
                    "entity_id": sync.entity_id,
                    "topic": sync.topic,
                    "name": name,
                })
            except Exception as err:  # noqa: BLE001
                _LOGGER.exception("同步到巴法失败: %s", sync.entity_id)
                failed.append({
                    "entity_id": sync.entity_id,
                    "topic": sync.topic,
                    "reason": str(err),
                })

        return {
            "success": True,
            "total_light": len(light_syncs),
            "created": created,
            "skipped": skipped,
            "failed": failed,
        }

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

    async def _async_fetch_devices(self, params: dict) -> dict:
        """抓取 HA 内智能家居设备清单（实体 + 设备注册表 + 集成来源）。

        智能家居域白名单（过滤 automation/script/zone 等非设备域），
        从 entity registry 取集成来源、device registry 取设备名/厂商/型号。
        返回: {"count", "devices": [{entity_id, name, domain, state, device_class,
                                     unit, integration, device_name, manufacturer, model}]}
        """
        smart_domains = {
            "light", "switch", "climate", "cover", "fan", "humidifier",
            "media_player", "vacuum", "lock", "water_heater", "valve",
            "binary_sensor", "sensor", "select", "number", "button",
            "scene", "input_boolean", "input_number", "input_select",
        }
        ent_reg = entity_registry.async_get(self._hass)
        dev_reg = device_registry.async_get(self._hass)

        devices = []
        for s in self._hass.states.async_all():
            domain, _, entity_id = s.entity_id.partition(".")
            if domain not in smart_domains:
                continue

            entry = ent_reg.async_get(s.entity_id) if ent_reg else None
            integration = entry.platform if entry else ""
            device_name = ""
            manufacturer = ""
            model = ""
            if entry and entry.device_id and dev_reg:
                dev = dev_reg.async_get(entry.device_id)
                if dev:
                    device_name = dev.name_by_user or dev.name or ""
                    manufacturer = dev.manufacturer or ""
                    model = dev.model or ""

            devices.append({
                "entity_id": s.entity_id,
                "name": s.attributes.get("friendly_name") or entity_id,
                "domain": domain,
                "state": s.state,
                "device_class": s.attributes.get("device_class"),
                "unit": s.attributes.get("unit_of_measurement"),
                "integration": integration,
                "device_name": device_name,
                "manufacturer": manufacturer,
                "model": model,
            })

        return {"count": len(devices), "devices": devices}

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
