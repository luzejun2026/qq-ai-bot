#!/usr/bin/env python3
"""QQ 机器人 + 健康检查 + 自唤醒（适用于 Render / Koyeb 等免费容器）

- 在后台线程里维持 QQ WebSocket 长连接并调用你自建的 Ollama 回复
- 主线程监听 $PORT 提供 /healthz 健康检查，满足平台存活探针
- 若设置了 SELF_URL，每 10 分钟 ping 自己一次，破解免费档"无流量就休眠"
"""
import json
import os
import time
import threading
import requests
import websocket
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# ---------- 配置（全部来自环境变量，密钥不写死在代码里）----------
APP_ID = os.environ.get("QQ_APP_ID", "1905311626")
APP_SECRET = os.environ.get("QQ_APP_SECRET", "jO3jQ7pXGzjTEzlYL9xmbRH8zrkdXRMH")
WS_URL = os.environ.get("WS_URL", "wss://api.sgroup.qq.com/websocket")
# 群聊/C2C 事件(1<<25) + 公域群消息(1<<30)
INTENTS = (1 << 25) | (1 << 30)

LLM_URL = os.environ.get("LLM_URL", "http://116.49.72.208:11434/v1/chat/completions")
LLM_MODEL = os.environ.get("LLM_MODEL", "qwen3.8:27b-64k")

PORT = int(os.environ.get("PORT", "8080"))
# 部署后把本服务的公网地址填进来，例如 https://qqbot.onrender.com
SELF_URL = os.environ.get("SELF_URL", "").rstrip("/")

SYSTEM_PROMPT = (
    "你是 QQ 机器人，性格友好、幽默、自然。用简体中文回复，"
    "回答简短口语化（一般不超过100字），适合聊天场景，不要输出思考过程。"
)
histories = {}  # openid -> 多轮对话历史

# ---------- 日志 ----------
def log(*args):
    print(" ".join(str(a) for a in args), flush=True)


# ---------- 鉴权 ----------
def get_token():
    r = requests.post(
        "https://bots.qq.com/app/getAppAccessToken",
        json={"appId": APP_ID, "clientSecret": APP_SECRET},
        timeout=10,
    )
    d = r.json()
    return d["access_token"], int(d.get("expires_in", 7000))


# ---------- 大模型回复 ----------
def make_reply(content, openid=""):
    h = histories.setdefault(openid, [])
    h.append({"role": "user", "content": content})
    if len(h) > 20:
        del h[:-20]
    try:
        r = requests.post(
            LLM_URL,
            timeout=60,
            json={
                "model": LLM_MODEL,
                "messages": [{"role": "system", "content": SYSTEM_PROMPT}] + h,
            },
        )
        r.raise_for_status()
        msg = r.json()["choices"][0]["message"]
        text = (msg.get("content") or "").strip()
        if not text:
            text = (msg.get("reasoning") or "").strip()[-200:]
    except Exception as e:
        log("[LLM异常]", e)
        text = "我脑子（大模型服务）刚刚短路了一下，稍后再试试~"
    if text:
        h.append({"role": "assistant", "content": text})
        if len(h) > 20:
            del h[:-20]
    return text or "嗯嗯。"


def send_reply(event_type, d, text):
    token, _ = get_token()
    if event_type == "C2C_MESSAGE_CREATE":
        url = f"https://api.sgroup.qq.com/v2/users/{d['author']['user_openid']}/messages"
    elif event_type == "GROUP_AT_MESSAGE_CREATE":
        url = f"https://api.sgroup.qq.com/v2/groups/{d['group_openid']}/messages"
    else:
        return
    body = {"msg_type": 0, "msg_id": d["id"], "msg_seq": 1, "content": text}
    r = requests.post(
        url,
        headers={"Authorization": "QQBot " + token},
        json=body,
        timeout=10,
    )
    log("[回复] 已发送, status=", r.status_code)


# ---------- WebSocket 回调 ----------
ws = None
last_heartbeat = 0
heartbeat_interval = 30
last_seq = None


def heartbeat_loop():
    while True:
        time.sleep(heartbeat_interval)
        try:
            ws.send(json.dumps({"op": 1, "d": last_seq}))
            log("[心跳] op=1 已发送, seq=", last_seq)
        except Exception as e:
            log("[心跳失败]", e)
            return


def on_open(wsa):
    log("[连接] WebSocket 已建立")
    token, _ = get_token()
    identify = {
        "op": 2,
        "d": {
            "token": "QQBot " + token,
            "intents": INTENTS,
            "shard": [0, 1],
        },
    }
    wsa.send(json.dumps(identify))
    log("[鉴权] 已发送 Identify (op=2), intents=", INTENTS)
    threading.Thread(target=heartbeat_loop, daemon=True).start()


def on_message(wsa, message):
    global heartbeat_interval, last_seq
    p = json.loads(message)
    if p.get("s") is not None:
        last_seq = p["s"]
    op, t, d = p.get("op"), p.get("t"), p.get("d")
    if op == 10:
        heartbeat_interval = d.get("heartbeat_interval", 30000) / 1000
        log("[Hello] op=10 心跳间隔=", heartbeat_interval, "s")
    elif op == 0:
        if t == "READY":
            log("[就绪] READY! 用户:", json.dumps(d.get("user", {}), ensure_ascii=False))
        elif t == "RESUMED":
            log("[恢复] RESUMED")
        elif t in ("C2C_MESSAGE_CREATE", "GROUP_AT_MESSAGE_CREATE"):
            content = d.get("content", "")
            author = d.get("author", {}).get("user_openid", "?")
            log("[消息] 收到", t, "来自用户", author[:6] + "***（内容不记录）")
            try:
                send_reply(t, d, make_reply(content, author))
            except Exception as e:
                log("[回复异常]", e)
        else:
            log("[事件]", t, json.dumps(d, ensure_ascii=False)[:500])
    elif op == 11:
        log("[心跳确认] op=11")
    else:
        log("[其他] op=", op, "t=", t, message[:300])


def on_error(wsa, error):
    log("[错误]", error)


def on_close(wsa, code, reason):
    log("[断开] code=", code, "reason=", reason)


def bot_main():
    global ws
    while True:
        try:
            token, _ = get_token()
            ws = websocket.WebSocketApp(
                WS_URL + f"?sn={int(time.time()*1000)}",
                header={"Authorization": "QQBot " + token},
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            ws.run_forever(ping_interval=20)
        except Exception as e:
            log("[异常]", e)
        time.sleep(5)


# ---------- 自唤醒：每 10 分钟 ping 自己，破解免费档休眠 ----------
def self_ping_loop():
    if not SELF_URL:
        log("[自唤醒] 未设置 SELF_URL，跳过（建议改用 UptimeRobot 保活）")
        return
    while True:
        time.sleep(600)
        try:
            r = requests.get(SELF_URL + "/healthz", timeout=10)
            log("[自唤醒] GET", SELF_URL + "/healthz ->", r.status_code)
        except Exception as e:
            log("[自唤醒失败]", e)


# ---------- 健康检查 HTTP 服务 ----------
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ("/", "/healthz", "/health"):
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"ok")
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, *a):
        pass


def main():
    # 后台拉起机器人与自唤醒
    threading.Thread(target=bot_main, daemon=True).start()
    threading.Thread(target=self_ping_loop, daemon=True).start()
    srv = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    log(f"[HTTP] 健康检查监听 0.0.0.0:{PORT}")
    srv.serve_forever()


if __name__ == "__main__":
    main()
