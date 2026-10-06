# QQ 机器人 · 免费容器部署包

把 QQ 机器人跑在 **Render / Koyeb** 等免费容器上，永久在线、无需银行卡。
机器人通过 WebSocket 常连 QQ 官方网关，调用你自建的 Ollama（`116.49.72.208:11434`）生成 AI 回复。

## 这个包做了什么
- `app.py`：QQ 长连接 + 健康检查 `/healthz` + 自唤醒（每 10 分钟 ping 自己一次，破解免费档"无流量就休眠"）
- 所有密钥/地址走**环境变量**，不写死在代码里
- 多轮对话记忆按用户隔离，且不记录聊天内容

## 必填环境变量
| 变量 | 说明 |
|------|------|
| `QQ_APP_ID` | QQ 机器人 AppID |
| `QQ_APP_SECRET` | QQ 机器人 AppSecret（**保密**） |
| `LLM_URL` | OpenAI 兼容接口 base，默认 `https://api.agnes-ai.cn/v1` |
| `LLM_MODEL` | 模型名，默认 `agnes-3.0-flash` |
| `LLM_API_KEY` | LLM 接口鉴权 Key（凡需 Bearer 鉴权的接口都要填） |
| `PORT` | 平台自动注入，健康检查端口 |
| `SELF_URL` | 部署后填本服务公网地址（如 `https://qqbot.onrender.com`），用于自唤醒 |

---

## 方式一：Render（推荐，最稳）

1. 注册 https://render.com （用 GitHub 登录，免银行卡）
2. 把本目录推到你的 GitHub 仓库
3. Render 控制台 → New → Blueprint → 选该仓库 → 创建
4. 在 Environment 填入 `QQ_APP_SECRET` 和 `LLM_URL` 等
5. 部署完成后拿到地址（如 `https://qqbot.onrender.com`），把它填回 `SELF_URL` 变量并 Save（触发重新部署）
6. 完成。免费档 750 小时/月 ≈ 单服务 24/7，自唤醒保活

> 若不想用 GitHub，也可在 Render 选 "Deploy from Docker" 上传本目录（含 Dockerfile）。

## 方式二：Koyeb（给 token，由助手从沙箱直接 CLI 部署）

1. 注册 https://koyeb.com （免银行卡）
2. 在 Settings → API → 生成 Token
3. 把 Token 发给我，我直接 `koyeb` CLI 推上去，并设好环境变量

---

## 兜底保活
若不想填 `SELF_URL`，可用外部监控保活（二选一，均免银行卡）：
- **UptimeRobot**：加一个 HTTPS 监控，5 分钟 ping 一次你的服务地址
- **cron-job.org**：加一个 1 分钟间隔的 HTTP 任务

## 回滚与日志
- Render / Koyeb 控制台都有实时日志流，搜 `[消息] 收到` 可确认机器人是否在线应答
- 修改代码推送到仓库即自动重新部署
