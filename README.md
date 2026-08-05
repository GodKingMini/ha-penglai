# 蓬莱 (Penglai) — Home Assistant 集成

> 此身此剑，皆为御主而存在。誓约已立，至死方休。

蓬莱是**装配指令通道**：在 Home Assistant 中独立添加集成，通过自建 EMQX broker（WSS）与蓬莱平台通讯，执行设备绑定、场景装配、自动化创建等**低频配置型指令**。控制类指令（如控灯）请使用巴法集成——功能分离，各司其职。

## 特性

- ✅ 独立「添加集成」入口（config flow），与巴法一样简单
- ✅ MQTT over WSS（`wss://pl.cteaz.top/mqtt`）——穿透 NAT/CGNAT，不依赖虚拟 IP
- ✅ 心跳保活：30s 发送 / 20s 检测 / 3 次丢失自动重连
- ✅ 指令-结果闭环：蓬莱下发 → agent 执行 → result 回报
- ✅ 状态上报：设备清单与 HA 状态精简推送
- ✅ 完全与 EasyTier 解耦（EasyTier 仅作备用方案）

## Topic 约定

| Topic | 方向 | 说明 |
|-------|------|------|
| `penglai/{device_id}/cmd` | 蓬莱 → HA | 装配指令（订阅） |
| `penglai/{device_id}/result` | HA → 蓬莱 | 执行结果（发布） |
| `penglai/{device_id}/state` | HA → 蓬莱 | 状态/清单上报（发布） |
| `penglai/ping` | 双向 | 心跳 |

## 指令类型

| 指令 | 说明 |
|------|------|
| `login_ha` | 海尔集成登录验证 |
| `bind_device` | 绑定设备 |
| `set_scene` | 场景切换（装配验证） |
| `sync_status` | 同步设备在线状态 |
| `create_automation` | 创建自动化 |
| `list_states` | 读取 HA 状态清单 |
| `reboot_integration` | 重载集成 |
| `ping` | 心跳应答 |

## 安装

### 方式一：冬瓜伴侣一键安装（推荐）

1. 在 [Releases 页面](https://github.com/GodKingMini/ha-penglai/releases) 下载最新版 `ha_penglai.zip`
2. 打开冬瓜伴侣（伴侣 UI），进入「文件管理」（或 SSH/终端）
3. 将 zip 解压到 HA 的 `custom_components/` 目录，确保得到 `custom_components/penglai/` 结构：

```bash
# 伴侣 UI 终端 / SSH 执行
cd /mnt/data/supervisor/homeassistant
unzip -o ha_penglai.zip -d custom_components/
```

4. 重启 HA，在「设置 → 设备与服务 → 添加集成」中搜索「蓬莱」

### 方式二：手动安装

```bash
# 将 custom_components/penglai 拷贝到 HA 的 custom_components/ 目录
cp -r custom_components/penglai /mnt/data/supervisor/homeassistant/custom_components/
```

然后重启 HA，在「设置 → 设备与服务 → 添加集成」中搜索「蓬莱」。

## 配置

| 配置项 | 默认 | 说明 |
|--------|------|------|
| Broker 地址 | `wss://pl.cteaz.top/mqtt` | EMQX WSS 端点 |
| 用户名 | - | 蓬莱分配的设备凭据 |
| 密码 | - | 蓬莱分配的设备凭据 |
| 设备 ID | - | 绑定标识符（如 `runfu-02-01-0203`） |
| Topic 前缀 | `penglai` | topic 命名空间 |

## 架构

```
蓬莱前端/后端 ──WSS──> EMQX (pl.cteaz.top:443/mqtt)
                             │
        penglai/{device}/cmd │ penglai/{device}/result
                             ▼
                   HA agent (本集成)
```

- 蓬莱后端通过 WSS 发布 `cmd` 指令
- 本集成订阅 `cmd`，执行装配指令
- 结果经 `result` topic 回报蓬莱后端
- 心跳 `penglai/ping` 保持在线检测

## 开发

```bash
# 本地语法检查
python3 -m py_compile custom_components/penglai/*.py
```

## 参考

- 结构借鉴 [bemfa HA 集成](https://github.com/bemfa/ha-bemfa)（巴法）
- EMQX 官方 Docker 部署
