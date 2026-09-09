# 🚀 OpenBot 2026

基于 **Bot API + MTProto 双引擎** 的 Telegram 综合管理框架，支持群文件扫描、批量/监听下载、Web 图形化管理、插件热加载。

- **MTProto**（核心监控层）：扫描群文件、下载、收藏夹监听、登录管理
- **Bot API**（指令交互层）：命令控制、权限管理、插件热重载
- **Web 查看器**（7777 端口）：群列表 / 文件查询 / 扫描进度 / 下载任务 / 推广转发，独立进程运行

---

## ✨ 核心特性

| 特性 | 说明 |
|---|---|
| 🧩 插件化架构 | 插件只依赖 core，互不 import，可独立安装/卸载/热重载 |
| 📥 统一下载引擎 | 命令下载（`/dl`）、转发自动下载（`/at`）、收藏夹监听，全部支持**续扫续传** |
| 🔍 群文件扫描 | 扫描全部历史媒体，分类统计（图片/视频/音乐/其它/失效，与 Telegram 计数对账），增量扫描 |
| 💾 共享扫描缓存 | `core/media_scanner.py` + `core/media_cache.py`，扫描一次、多处复用 |
| 🖥️ Web 管理 | 浏览器操作下载任务、查询群文件、发起扫描，无需发消息 |
| 📤 推广转发（v1.3） | 从来源群按文件 id 重发到目标群（绕开禁转），相册聚合/全选全部/去除重复/#标签筛选/附带文字/每 N 条插入推广/自动监听，任务可撤回/重试/删除，Web /promote.html 配置 |
| 🔄 热重载 | `/reload_plugins` 免重启同步代码；`/add_plugin` 免重启安装新插件 |
| 🛡️ 权限管控 | 超级管理员（ADMIN_ID）+ 普通管理员列表（ADMIN_LIST） |
| 📋 命令菜单自动同步 | 启动/热重载时自动同步命令菜单到 Telegram，与 help.txt 永远一致 |
| 🐳 容器化部署 | Docker / Docker-Compose 一键部署，数据持久化 |

---

## 📂 目录结构

```
openbotcs1/
├── main.py                     # 主入口（bot + 下载/扫描/监听）
├── bootstrap/
│   └── launcher.py             # 启动编排、状态汇总、安全退出
├── core/                       # 核心驱动层（只改这里，插件禁止绕过）
│   ├── client_manager.py       # Bot API 管理（PTB 轮询/代理/重连）
│   ├── mtproto_client.py       # MTProto 连接管理（会话/代理）
│   ├── command_registry.py     # 命令注册中心 + 命令菜单自动同步
│   ├── config_manager.py       # 配置读取（.env）
│   ├── validator.py            # 配置校验
│   ├── database.py             # 下载任务数据库（download_tasks.db）
│   ├── download_engine.py      # 下载引擎（全局并发/分批/续传）
│   ├── media_scanner.py        # 群文件扫描层（全量/增量/分类统计）
│   ├── media_cache.py          # 共享扫描缓存（media_cache.db，WAL）
│   ├── plugin_scanner.py       # 插件加载器（扫描/注册/热重载）
│   ├── logger.py               # 日志系统（主日志 + 控制台 + 插件分流）
│   └── utils.py                # 工具函数（is_admin 等）
├── features/                   # 插件层（只依赖 core，互不 import）
│   ├── admin/                  # 用户管理 / 插件管理 / 开发手册（/cj）
│   ├── basic/                  # 基础命令（/id /ping /status /disk ...）
│   ├── downloader/             # 统一下载引擎（/dl*）+ 转发自动下载（/at）
│   ├── help_auto/              # 帮助中心（/help）
│   ├── look/                   # 群文件查看器（Web 7777 + 扫描 + 对话同步）
│   ├── promote/                # 推广转发（v1.3：重发 + 聚合/去重/推广插入/自动监听 + promote.html 页面）
│   └── mtproto/                # MTProto 登录管理器（/mtlogin）
├── download/                   # 下载文件存储 + 数据库（运行期生成，勿提交）
├── sessions/                   # MTProto 物理会话（运行期生成，勿提交）
├── logs/                       # 运行日志（运行期生成，勿提交）
├── Dockerfile                  # 容器镜像
├── requirements.txt            # Python 依赖
├── .env.example                # 配置模板（复制为 .env 后填写）
└── .gitignore
```

---

## 🚀 快速开始

### 1. 安装依赖（Python 3.10+）

```bash
pip install -r requirements.txt
```

### 2. 配置 .env

```bash
cp .env.example .env   # Windows: copy .env.example .env
```

编辑 `.env`：

```
BOT_TOKEN=你的BotToken
API_ID=你的API_ID
API_HASH=你的API_HASH
ADMIN_ID=你的TG用户ID
ADMIN_LIST=可选,逗号分隔多个管理员ID
PROXY=http://127.0.0.1:7897   # 中国大陆必须填代理
LOG_LEVEL=INFO
```

### 3. 启动

```bash
# 主 bot（含下载/扫描/监听/命令）
python main.py

# Web 查看器（7777 端口，独立进程，可分开跑）
python features/look/look_server.py
```

启动成功后访问：**http://127.0.0.1:7777**

---

## 📚 命令大全

### 👑 用户管理（超级管理员）
| 命令 | 说明 |
|---|---|
| `/admins` | 查看当前管理团队列表及权限等级 |
| `/add_admin` | 添加普通管理员权限（超级管理员） |
| `/remove_admin` | 移除普通管理员权限（超级管理员） |
| `/ban` | 封禁用户。用法: 回复消息 /ban 或 /ban [ID] |
| `/unban` | 解除用户封禁。用法: /unban [ID] |
| `/groupinfo` | MTProto 深度探测群组/频道实时统计数据 |

### ⚡ 插件管理
| 命令 | 说明 |
|---|---|
| `/plugins` | 实时查看当前系统已加载的插件库及指令映射表 |
| `/reload_plugins` | 强制全量刷新插件目录，同步最新的代码改动 |
| `/add_plugin` | 进入"热安装"模式，60s 内发送 .py 文件即可免重启部署新功能 |
| `/cj` | 查看 OpenBot 项目开发手册 |

### 🖥️ 基础命令
| 命令 | 说明 |
|---|---|
| `/start` | 启动 Bot |
| `/ping` | 测试 Bot 响应速度 |
| `/id` | 查看当前聊天 ID 信息，/id [链接] 解析详情 |
| `/status` | 查看 Bot 运行状态（仅管理员） |
| `/disk` | 磁盘监控。用法: /disk 或 /disk [路径]（仅管理员） |
| `/python` | 远程执行命令（仅管理员） |

### 📥 统一下载引擎
| 命令 | 说明 |
|---|---|
| `/dl` | 新建频道/群组下载任务（所有类型，一次性下载） |
| `/dl_video` | 只下载视频（一次性下载） |
| `/dl_photo` | 只下载图片（一次性下载） |
| `/dl_watch` | 持续监听群/频道（所有类型，新消息自动下载） |
| `/dl_watch_video` | 持续监听群/频道，只下载视频 |
| `/dl_watch_photo` | 持续监听群/频道，只下载图片 |
| `/dl_saved` | 启动收藏夹监听（所有类型） |
| `/dl_saved_video` | 收藏夹只下载视频 |
| `/dl_saved_photo` | 收藏夹只下载图片 |
| `/dls` | 列出所有下载任务，带任务编号 |
| `/dl_stop` | 完全停止任务 #xxx（停止扫描+下载+监听） |
| `/dl_continue` | 完全恢复任务 #xxx（从断点继续） |
| `/dl_no` | 取消并删除任务 #xxx（清理数据库记录） |
| `/dl_clear` | 清理所有已完成和已取消的任务记录 |
| `/at` | 查看转发自动下载引擎状态 |

### 🗂️ 其他
| 命令 | 说明 |
|---|---|
| `/help` | 查看全部命令帮助 |
| `/look` | 群文件查看器使用说明（Web 端 http://127.0.0.1:7777） |
| `/mtlogin` | 机器人对话登录命令 |
| `/promote` | 推广转发：前往 Web http://127.0.0.1:7777/promote.html 配置（v1.3） |

---

## 🖥️ Web 查看器（群文件查看器）

独立进程 `features/look/look_server.py`，**http://127.0.0.1:7777**

### 功能
- **下载任务**：查看/管理所有下载任务（自动下载 + 频道下载 + 收藏夹）
- **群文件查询**：列出全部对话（群/频道/私聊/收藏夹），点击打开查看文件列表
  - 每个群显示：**已读取进度** + **分类统计**（🖼️图片 / 🎬视频 / 🎵音乐 / 📁文档）
  - 支持**增量扫描**（只扫新增）和**重新扫描**（全量重扫）
  - 支持按类型筛选（全部/群聊/频道/私聊/收藏夹）
- **点击下载**：在 Web 里选中文件即可创建下载任务，任务交给统一下载引擎执行

### 数据流
```
Web 页面 → look_server（7777）→ look.db 请求队列
              ↓ 轮询
          group_browse 扫描（MTProto）→ media_cache 共享缓存
              ↓ 下载请求
          downloader 下载引擎 → download_tasks.db + 下载文件
```

### 推广转发（v1.3）：**http://127.0.0.1:7777/promote.html**

- 来源群 → 目标群，内容筛选（图片/视频/音乐/其它/#标签），附带文字（保留原文/自定义）
- **相册聚合**：同媒体组聚合为相册发送，不拆散；**全选全部**（跨页保留）+ **去除重复**（按文件 ID 去重，聚合不拆）
- **推广插入**：每转发 N 次插入 1 条推广（指定内容=群链接+消息ID，或自定义文案）
- **自动监听**：来源群新消息自动转发（绕开禁转，按文件 id 重发）
- **任务管理**：详情/停止/撤回/重试/删除，进度双口径（文件数 + 转发次数）
- 独立库 `download/promote.db`，发送记录落库支持一键撤回

---

## 🧩 插件开发

### 目录约定
```
features/你的插件/
├── __init__.py      # 空文件
├── 模块.py           # 逻辑代码
└── help.txt          # 命令描述（格式：/命令 - 描述，自动同步到 Telegram 菜单）
```

### 标准模板
```python
# features/xxx/xxx_manager.py
import logging
from telegram import Update
from telegram.ext import ContextTypes, CommandHandler
from core.command_registry import register_handler

logger = logging.getLogger(__name__)
__MODULE_NAME__ = "插件中文名"

async def handle_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    manager = getattr(handle_cmd, 'manager', None) or context.bot_data.get('manager')
    await update.message.reply_text('✅ 引擎已就绪')

def register(manager):
    handle_cmd.manager = manager
    register_handler(CommandHandler('cmd', handle_cmd), __name__)
```

### 开发准则
1. **插件只依赖 core**：插件之间互不 import，各自独立（可能只装一个，也可能全装）
2. **连接统一走 core**：Bot API 用 `manager`（client_manager），MTProto 用 `manager.mtproto_client.client`
3. **扫描/下载走 core 共享**：`core/media_scanner.py` + `core/media_cache.py` + `core/download_engine.py`
4. **命令注册**：`register_handler(CommandHandler('cmd', func), __name__)`
5. **配置读取**：`manager.config.get('KEY')`；管理员校验：`core.utils.is_admin`

---

## 🐳 Docker 部署

### 1. 配置 .env（同上）

### 2. 构建镜像并启动

```bash
# 构建镜像（项目根目录）
docker build -t openbot-deploy:latest .

# 运行容器（Windows/PowerShell 写法，Linux 改路径）
docker run -d --name openbot-deploy --restart unless-stopped \
  --add-host=host.docker.internal:host-gateway -p 7777:7777 \
  --log-opt max-size=20m --log-opt max-file=3 \
  -v "C:\openbot\.env:/app/.env" \
  -v "C:\openbot\download:/app/download" -v "C:\openbot\logs:/app/logs" \
  -v "C:\openbot\sessions:/app/sessions" -v "C:\openbot\data:/app/data" \
  openbot-deploy:latest
```

### 3. 访问

```
Web 查看器: http://服务器IP:7777
```

### 数据持久化

| 挂载点 | 内容 |
|---|---|
| `/app/.env` | 配置（改配置后重启容器生效） |
| `/app/download` | 下载文件 + 数据库（look.db / media_cache.db / download_tasks.db / promote.db） |
| `/app/logs` | 运行日志 |
| `/app/sessions` | MTProto 会话（备份可免重新登录） |
| `/app/data` | 持久化数据 |

> Web 页面已内置进镜像（features/promote/promote.html），无需挂载。

---

## ⚠️ 安全须知

- **.env 绝不要提交到仓库**（含 BOT_TOKEN / API_HASH，泄露=账号被盗）
- 建议使用**私有仓库**存放本项目
- `sessions/`、`download/`、`logs/`、`*.db`（含 promote.db）为运行期数据，已在 `.gitignore` 中忽略
- 中国大陆使用必须配置 `PROXY`（HTTP 代理，如 Clash 的 7897 端口）

---

## 🛠️ 常见问题

| 问题 | 解决 |
|---|---|
| MTProto 连接失败 / 代理报错 | 检查 `.env` 中 `PROXY` 是否可达 |
| 输入 `/` 菜单不更新 | 已自动同步；改 help.txt 后 `/reload_plugins` 即可 |
| Web 打不开 | 确认 `python features/look/look_server.py` 已启动 |
| 下载卡住不动 | `/dls` 查看任务状态，`/dl_continue 任务号` 从断点续传 |
| 数据库 locked | 正常现象（WAL 模式多进程读写），稍后重试即可 |
