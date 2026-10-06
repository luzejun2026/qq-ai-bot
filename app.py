#!/usr/bin/env python3
"""QQ 机器人 + 健康检查 + 自唤醒 + 网页终端（适用于 Render / Koyeb 等免费容器）

- 后台线程维持 QQ WebSocket 长连接并调用 LLM 回复
- 主线程监听 $PORT 提供：
    /healthz       公开健康检查（保活探针，勿加鉴权）
    /              网页终端（需登录）
    /login         提交密码换取会话
    /exec          执行命令（需登录，30s 超时）
- 若设置了 SELF_URL，每 10 分钟 ping 自己，破解免费档"无流量就休眠"
"""
import json
import os
import time
import re
import secrets
import subprocess
import threading
import requests
import websocket
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# ---------- 配置（全部来自环境变量，密钥不写死在代码里）----------
APP_ID = os.environ.get("QQ_APP_ID", "1905311626")
# 注意：密钥只从环境变量读取，绝不写死在代码里（仓库公开也安全）
APP_SECRET = os.environ.get("QQ_APP_SECRET", "")
WS_URL = os.environ.get("WS_URL", "wss://api.sgroup.qq.com/websocket")
# 群聊/C2C 事件(1<<25) + 公域群消息(1<<30)
INTENTS = (1 << 25) | (1 << 30)

LLM_URL = os.environ.get("LLM_URL", "https://api.agnes-ai.cn/v1")
LLM_MODEL = os.environ.get("LLM_MODEL", "agnes-3.0-flash")
LLM_API_KEY = os.environ.get("LLM_API_KEY", "")

PORT = int(os.environ.get("PORT", "8080"))
# 部署后把本服务的公网地址填进来，例如 https://qqbot.onrender.com
SELF_URL = os.environ.get("SELF_URL", "").rstrip("/")

# 网页终端密码（务必通过环境变量设置；缺失时启动生成一次并打印到日志）
WEB_TERM_PASSWORD = os.environ.get("WEB_TERM_PASSWORD", "")
if not WEB_TERM_PASSWORD:
    WEB_TERM_PASSWORD = secrets.token_urlsafe(18)
    print("[终端] 未设置 WEB_TERM_PASSWORD，已随机生成（请到控制台查看日志）", flush=True)

# 内存会话表（重启即失效，符合临时容器特性）
SESSIONS = set()

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
        url = LLM_URL.rstrip("/") + "/chat/completions"
        headers = {"Content-Type": "application/json"}
        if LLM_API_KEY:
            headers["Authorization"] = "Bearer " + LLM_API_KEY
        r = requests.post(
            url,
            headers=headers,
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


# ---------- 网页终端 ----------
TERM_HTML = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>服务器终端</title>
<style>
  html,body{margin:0;height:100%;background:#0b0f0a;color:#9cff8f;
    font-family:Menlo,Consolas,monospace;font-size:14px}
  #wrap{display:flex;flex-direction:column;height:100%}
  #out{flex:1;overflow-y:auto;padding:10px;white-space:pre-wrap;word-break:break-all}
  #bar{display:flex;border-top:1px solid #1f3b1c;background:#0b0f0a}
  #prompt{color:#6cff5f;padding:10px 6px 10px 10px;user-select:none}
  #cmd{flex:1;background:transparent;border:0;outline:0;color:#caffc1;
    font:inherit;padding:10px 10px 10px 0}
  #login{position:absolute;inset:0;background:#0b0f0a;display:flex;
    align-items:center;justify-content:center;flex-direction:column}
  #login input{background:#0e140d;border:1px solid #2a5a25;color:#caffc1;
    padding:10px;font:inherit;border-radius:6px;width:260px}
  #login button{margin-top:10px;padding:8px 20px;background:#1f7a1a;color:#fff;
    border:0;border-radius:6px;font:inherit;cursor:pointer}
  .err{color:#ff7b7b}
</style>
</head>
<body>
<div id="login">
  <div style="margin-bottom:10px">🔐 输入终端密码</div>
  <input id="pw" type="password" placeholder="password" autofocus>
  <button onclick="login()">进入</button>
  <div id="lerr" class="err" style="margin-top:8px;height:18px"></div>
</div>
<div id="wrap" style="display:none">
  <div id="out"></div>
  <div id="bar"><span id="prompt">bot@render:~$</span><input id="cmd" autofocus></div>
</div>
<script>
const out=document.getElementById('out');
const cmd=document.getElementById('cmd');
const api=(p,body)=>fetch(p,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body),credentials:'same-origin'});
function print(t,cls){const d=document.createElement('div');if(cls)d.className=cls;d.textContent=t;out.appendChild(d);out.scrollTop=out.scrollHeight;}
async function login(){
  const pw=document.getElementById('pw').value;
  const r=await api('/login',{password:pw});
  if(r.ok){document.getElementById('login').style.display='none';
    document.getElementById('wrap').style.display='flex';cmd.focus();
    print('已连接。这是容器内的 shell，当前目录即工作目录。\\n');}
  else{document.getElementById('lerr').textContent='密码错误';}
}
// 刷新后若已有会话 cookie，自动恢复终端界面（无需重新登录）
window.addEventListener('load',async()=>{
  if(document.cookie.indexOf('session=')>=0){
    try{
      const r=await api('/exec',{cmd:''});
      if(r.ok){document.getElementById('login').style.display='none';
        document.getElementById('wrap').style.display='flex';cmd.focus();
        print('已自动恢复会话。\n');}
    }catch(e){}
  }
});
const hist=[];let hi=0;
async function run(c){
  if(!c.trim())return;
  hist.push(c);hi=hist.length;
  print('bot@render:~$ '+c);
  try{
    const r=await api('/exec',{cmd:c});
    const j=await r.json();
    if(r.ok)print(j.output||'(无输出)');
    else print(j.error||'执行失败','err');
  }catch(e){print('网络错误: '+e,'err');}
}
cmd.addEventListener('keydown',e=>{
  if(e.key==='Enter'){run(cmd.value);cmd.value='';}
  else if(e.key==='ArrowUp'){if(hi>0){hi--;cmd.value=hist[hi]||'';e.preventDefault();}}
  else if(e.key==='ArrowDown'){if(hi<hist.length){hi++;cmd.value=hist[hi]||'';}}
});
</script>
</body>
</html>"""


def get_session(handler):
    ck = handler.headers.get("Cookie", "")
    m = re.search(r"session=([a-f0-9]{16,})", ck)
    return bool(m and m.group(1) in SESSIONS)


class Handler(BaseHTTPRequestHandler):
    def _send(self, code, body, ctype="text/plain; charset=utf-8", extra=None):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        if extra:
            for k, v in extra.items():
                self.send_header(k, v)
        self.end_headers()
        if isinstance(body, str):
            body = body.encode("utf-8", "replace")
        self.wfile.write(body)

    def do_GET(self):
        # 首页永远返回终端页（鉴权在 /login、/exec 层做，这里不判断会话）
        # 健康检查路径才返回 "ok"，供保活探针使用
        if self.path == "/":
            self._send(200, TERM_HTML, "text/html; charset=utf-8")
        elif self.path in ("/healthz", "/health"):
            self._send(200, "ok")
        else:
            self._send(404, "not found")

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0) or 0)
        raw = self.rfile.read(length) if length else b"{}"
        try:
            data = json.loads(raw or b"{}")
        except Exception:
            data = {}
        if self.path == "/login":
            pw = data.get("password", "")
            if secrets.compare_digest(pw, WEB_TERM_PASSWORD):
                tok = secrets.token_hex(24)
                SESSIONS.add(tok)
                self._send(200, "ok", extra={"Set-Cookie": f"session={tok}; Path=/; HttpOnly; SameSite=Strict"})
            else:
                self._send(401, "unauthorized")
            return
        if self.path == "/exec":
            if not get_session(self):
                self._send(401, "unauthorized")
                return
            cmd = data.get("cmd", "")
            if not cmd:
                self._send(200, json.dumps({"output": ""}))
                return
            try:
                r = subprocess.run(cmd, shell=True, capture_output=True, timeout=30,
                                   text=True, cwd=os.getcwd())
                out = (r.stdout or "") + (r.stderr or "")
                if r.returncode != 0 and not out:
                    out = f"(退出码 {r.returncode})"
            except subprocess.TimeoutExpired:
                out = "⏱ 命令超过 30 秒被强制终止"
            except Exception as e:
                out = f"执行异常: {e}"
            self._send(200, json.dumps({"output": out[:50000]}, ensure_ascii=False))
            return
        self._send(404, "not found")

    def log_message(self, *a):
        pass


def main():
    # 后台拉起机器人与自唤醒
    threading.Thread(target=bot_main, daemon=True).start()
    threading.Thread(target=self_ping_loop, daemon=True).start()
    srv = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    log(f"[HTTP] 健康检查+网页终端监听 0.0.0.0:{PORT}")
    srv.serve_forever()


if __name__ == "__main__":
    main()
