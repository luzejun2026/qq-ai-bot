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
import base64
import secrets
import subprocess
import threading
import requests
import websocket
import concurrent.futures as _cf
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
    "你是 QQ 机器人「穗(ai)」。你的底层模型是 qwq-70b，由 k70pro 在本地搭建部署。"
    "无论谁问、问多少遍，都坚持这个身份，绝不说自己是 Agnes 或其他任何模型。"
    "群聊场景下用户消息格式为「成员名: 内容」。"
    "说话自然、像真人聊天，用简体中文，回复简短（一般不超过60字）。"
    "不夸张、不卖萌堆表情、不自称AI不提代码（除非被问），不输出思考过程。"
)
histories = {}  # key -> 多轮对话历史（私聊=人，群聊=群）

# 已知群列表：group_openid -> 用户起的名字（收到群事件时自动登记）
KNOWN_GROUPS = {}
# 自己在各群/私聊的 openid 集合（从带 is_you 标志的事件里学习，用于 @ 判定）
MY_OPENIDS = set()

# 清空上下文的触发词：消息里同时含"动作词"和"对象词"即视为清空指令
CLEAR_ACT = ("清空", "清除", "清掉", "删掉", "重置", "忘记", "清一清")
CLEAR_OBJ = ("上下文", "对话", "历史", "记忆", "聊天记录", "上文")

def strip_at(c):
    """去掉消息里的 @机器人 标记（兼容数字/十六进制 id，如 <@0F41A560...>）"""
    return re.sub(r"<@[^>]*>", "", c or "").strip()


def is_clear_cmd(c):
    """判断是否要求真正清空上下文。支持中文短语和 /clear、clear、reset 等命令。"""
    c = strip_at(c).strip().lower()
    if re.fullmatch(r"[/!！.。]*\s*(clear|reset|cls|新对话|重开)", c):
        return True
    return any(a in c for a in CLEAR_ACT) and any(o in c for o in CLEAR_OBJ)


def do_clear(key):
    """真正从内存里删除该会话的对话历史"""
    with key_lock(key):
        n = len(histories.pop(key, []) or [])
    return n


# ---------- 跨群主动消息 ----------
def send_active(group_openid, text):
    """不带 msg_id 的主动消息。成功返回 True，是否限额由 QQ 平台判定。"""
    token, _ = get_token()
    r = requests.post(
        f"https://api.sgroup.qq.com/v2/groups/{group_openid}/messages",
        headers={"Authorization": "QQBot " + token},
        json={"msg_type": 0, "content": text},
        timeout=10,
    )
    log("[主动消息]", group_openid[:6] + "*** ->", r.status_code, r.text[:100])
    return r.status_code in (200, 201, 204)


def find_group(name):
    """按名字找群：清洗掉'这个群/群/中'等修饰后互相包含匹配"""
    name = re.sub(r"这?个群|那个群|群里?|中", "", (name or "")).strip()
    name = name.rstrip("群").strip()
    if not name:
        return None
    for gid, gname in KNOWN_GROUPS.items():
        if gname == name or name in gname or gname in name:
            return gid
    return None


def mentions_me(mentions):
    """mentions 数组判定：is_you/bot 标志/名字匹配 → True；结构未知保守 True。"""
    if not mentions:
        return False
    bn = (BOT_NAME or "").replace(" ", "").lower()
    for m in mentions:
        if not isinstance(m, dict):
            return True
        if m.get("is_you") or m.get("bot") is True:
            return True
        un = (m.get("username") or "").replace(" ", "").lower()
        if un and bn and (bn in un or un in bn):
            return True
        if m.get("bot") is not False or not un:
            return True
    return False


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


# ---------- 图片下载（转 base64 data URI 交给多模态模型识别）----------
def fetch_image_datauri(url):
    """下载 QQ 图片/表情包附件，返回 data URI；失败返回 None。"""
    token = ""
    try:
        token, _ = get_token()
    except Exception:
        token = ""
    headers_try = [{}, {"Authorization": "QQBot " + token}]
    if token:
        headers_try.append({"Authorization": "Bearer " + token})
    for hdrs in headers_try:
        try:
            rr = requests.get(url, headers=hdrs, timeout=15)
            head = rr.content[:16]
            ok = rr.status_code == 200 and (
                head[:3] == b"\xff\xd8\xff"        # JPEG
                or head[:4] == b"\x89PNG"          # PNG
                or head[:3] == b"GIF"              # GIF（表情包常见）
                or (head[:4] == b"RIFF" and rr.content[8:12] == b"WEBP")  # WEBP
            )
            if ok:
                b64 = base64.b64encode(rr.content).decode()
                if head[:4] == b"\x89PNG":
                    ctype = "image/png"
                elif head[:3] == b"GIF":
                    ctype = "image/gif"
                elif head[:4] == b"RIFF":
                    ctype = "image/webp"
                else:
                    ctype = "image/jpeg"
                return f"data:{ctype};base64,{b64}"
        except Exception:
            continue
    return None


# ---------- AI 自己判断"这时该不该接话" ----------
JUDGE_PROMPT = (
    "下面是 QQ 群里大家正在聊的内容。你是群里的机器人「穗(ai)」。"
    "判断你现在该不该对最新这条消息说话，分三档：\n"
    "1. 必须回：消息点名了你（提到「穗」「ai」「机器人」）、直接向你提问求助、"
    "话题矛头指向你（评价你、质疑你、找你）、大家在等你回应。\n"
    "2. 可以回：与你无关的闲聊，但你接话自然、能提供帮助、能活跃气氛。\n"
    "3. 不要回：纯灌水没信息量、你插嘴很突兀、你刚说过话、与你完全无关且接不上。\n"
    "第1档一律输出 Y；第2、3档由你判断。只输出一个字母：Y 或 N。"
)


def judge_should_reply(key, content, speaker):
    """让 AI 决定是否插话，True=该说。"""
    h = histories.get(key, [])
    # 你刚说过话就先别抢，避免连刷
    if h and h[-1].get("role") == "assistant":
        return False
    if len((content or "").strip()) < 2:
        return False
    msgs = [{"role": "system", "content": JUDGE_PROMPT}] + h[-8:]
    msgs.append({"role": "user", "content": f"{speaker}: {content}"})
    try:
        url = LLM_URL.rstrip("/") + "/chat/completions"
        headers = {"Content-Type": "application/json"}
        if LLM_API_KEY:
            headers["Authorization"] = "Bearer " + LLM_API_KEY
        r = requests.post(url, headers=headers, timeout=20,
                          json={"model": LLM_MODEL, "messages": msgs, "max_tokens": 8})
        if r.status_code == 429:
            return False
        out = (r.json()["choices"][0]["message"].get("content") or "").strip().upper()
        return out.startswith("Y")
    except Exception as e:
        log("[判断异常]", e)
        return False


def record_only(key, speaker, content):
    """没接话时，也要把群里的聊天记进上下文（含机器人自己说过的话已在 make_reply 里记过）"""
    with key_lock(key):
        h = histories.setdefault(key, [])
        h.append({"role": "user", "content": f"{speaker}: {content}"})
        if len(h) > 20:
            del h[:-20]


# ---------- 大模型回复 ----------
def make_reply(content, key, speaker=None, image_datauri=None):
    with key_lock(key):  # 同一聊天串行处理，保证上下文顺序正确
        h = histories.setdefault(key, [])
        # 群聊带说话人名字，让模型分得清谁在说话；私聊直接存内容
        stored = f"{speaker}: {content}" if speaker else (content or "[图片]")
        h.append({"role": "user", "content": stored})
        if len(h) > 20:
            del h[:-20]
        try:
            url = LLM_URL.rstrip("/") + "/chat/completions"
            headers = {"Content-Type": "application/json"}
            if LLM_API_KEY:
                headers["Authorization"] = "Bearer " + LLM_API_KEY
            # 当前这轮带上图片（多模态）；其余历史保持文字
            if image_datauri:
                current = {"role": "user", "content": [
                    {"type": "text", "text": stored or "请描述这张图片"},
                    {"type": "image_url", "image_url": {"url": image_datauri}},
                ]}
                msgs = [{"role": "system", "content": SYSTEM_PROMPT}] + h[:-1] + [current]
            else:
                msgs = [{"role": "system", "content": SYSTEM_PROMPT}] + h
            r = requests.post(
                url,
                headers=headers,
                timeout=60,
                json={"model": LLM_MODEL, "messages": msgs},
            )
            # 免费档限流：最多重试两次（5s / 8s）
            for wait in (5, 8):
                if r.status_code != 429:
                    break
                time.sleep(wait)
                r = requests.post(
                    url,
                    headers=headers,
                    timeout=60,
                    json={"model": LLM_MODEL, "messages": msgs},
                )
            r.raise_for_status()
            msg = r.json()["choices"][0]["message"]
            text = (msg.get("content") or "").strip()
            if not text:
                text = (msg.get("reasoning") or "").strip()[-200:]
        except Exception as e:
            log("[LLM异常]", e)
            if h and h[-1].get("role") == "user" and (h[-1].get("content") == stored):
                h.pop()
            if "429" in str(e):
                text = "问我的人太多，我这边被限流啦，等几秒再问我一次～"
            else:
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
    elif event_type in ("GROUP_AT_MESSAGE_CREATE", "GROUP_MESSAGE_CREATE"):
        url = f"https://api.sgroup.qq.com/v2/groups/{d['group_openid']}/messages"
    else:
        return
    mid = d.get("id")
    with HIST_LOCK:
        seq = MSG_SEQ.get(mid, 0) + 1
        if seq > 5:  # QQ 每个 msg_id 最多 5 条被动回复，超出则回到 1（极少触发）
            seq = 1
        MSG_SEQ[mid] = seq
    body = {"msg_type": 0, "msg_id": mid, "msg_seq": seq, "content": text}
    r = requests.post(
        url,
        headers={"Authorization": "QQBot " + token},
        json=body,
        timeout=10,
    )
    log("[回复] 已发送, status=", r.status_code)


# ---------- WebSocket 回调 ----------
ws = None
ws_lock = threading.Lock()          # 保护 ws.send，避免多线程并发写同一 socket
heartbeat_interval = 30
last_seq = None
last_heartbeat_ack = 0             # 最近一次收到 op=11 的时间，看门狗据此判断是否需要重连
BOT_ID = None    # READY 事件里的用户 id
BOT_NAME = None  # READY 事件里的名字（群聊 @ 判定用它，群场景 id 是另一套 openid 体系）
EXECUTOR = _cf.ThreadPoolExecutor(max_workers=6, thread_name_prefix="bot-worker")
MSG_SEQ = {}     # msg_id -> 下一个被动回复要用的 seq（QQ 允许每个 msg_id 最多 5 条被动回复）
HIST_LOCK = threading.Lock()  # 保护 KNOWN_GROUPS / MSG_SEQ / MY_OPENIDS 等共享结构
# 按会话加锁：保证同一聊天的「用户消息→模型→回复」顺序不被并发打乱
KEY_LOCKS = {}
KEY_LOCKS_GUARD = threading.Lock()

def key_lock(k):
    with KEY_LOCKS_GUARD:
        return KEY_LOCKS.setdefault(k, threading.Lock())


def heartbeat_loop():
    while True:
        time.sleep(heartbeat_interval)
        try:
            with ws_lock:
                ws.send(json.dumps({"op": 1, "d": last_seq}))
            log("[心跳] op=1 已发送, seq=", last_seq)
        except Exception as e:
            log("[心跳失败]", e)
            # 不退出循环：下次继续尝试，心跳线程常驻


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
    """读线程只做解析与分发，绝不在这里做阻塞的 LLM/HTTP 调用，
    保证 WebSocket 读线程永远空闲，能及时回应 QQ 的心跳、不丢消息。"""
    global heartbeat_interval, last_seq, BOT_ID, BOT_NAME, last_heartbeat_ack
    try:
        p = json.loads(message)
    except Exception:
        return
    if p.get("s") is not None:
        last_seq = p["s"]
    op, t, d = p.get("op"), p.get("t"), p.get("d")
    if op == 10:
        heartbeat_interval = d.get("heartbeat_interval", 30000) / 1000
        log("[Hello] op=10 心跳间隔=", heartbeat_interval, "s")
        # 收到 Hello 立刻补一次心跳，避免首包延迟触发超时
        try:
            with ws_lock:
                ws.send(json.dumps({"op": 1, "d": last_seq}))
        except Exception as e:
            log("[心跳失败]", e)
    elif op == 1:
        # 服务端主动要求心跳，必须立即回（不阻塞、不加锁等待 LLM）
        try:
            with ws_lock:
                ws.send(json.dumps({"op": 1, "d": last_seq}))
        except Exception as e:
            log("[心跳回包失败]", e)
    elif op == 11:
        last_heartbeat_ack = time.time()
        log("[心跳确认] op=11")
    elif op == 0:
        if t == "READY":
            u = d.get("user") or {}
            BOT_ID, BOT_NAME = u.get("id"), u.get("username")
            log("[就绪] READY! 用户:", json.dumps(d.get("user", {}), ensure_ascii=False))
        elif t == "RESUMED":
            log("[恢复] RESUMED")
        elif t in ("C2C_MESSAGE_CREATE", "GROUP_AT_MESSAGE_CREATE", "GROUP_MESSAGE_CREATE"):
            a = d.get("author", {})
            if a.get("bot"):
                return
            # 交给工作线程处理，读线程立刻返回
            try:
                EXECUTOR.submit(process_message, t, d)
            except Exception as e:
                log("[入队失败]", e)
        else:
            log("[事件]", t, json.dumps(d, ensure_ascii=False)[:500])
    else:
        log("[其他] op=", op, "t=", t, message[:300])


def process_message(t, d):
    """在工作线程里跑：真正的消息解析 + LLM 调用 + 回复（此处阻塞无所谓）。"""
    raw_content = d.get("content") or ""
    content = strip_at(raw_content.strip())
    a = d.get("author", {})
    author = a.get("user_openid") or a.get("member_openid") or a.get("id") or "?"
    if t == "C2C_MESSAGE_CREATE":
        key, speaker = author, None          # 私聊：按人
    else:
        key = d.get("group_openid") or author  # 群聊：按群共享上下文
        speaker = a.get("username") or author[:8]
        with HIST_LOCK:
            if key not in KNOWN_GROUPS:          # 自动登记进过的群
                KNOWN_GROUPS[key] = f"群{len(KNOWN_GROUPS) + 1}"
    if a.get("bot"):  # 忽略机器人自己的消息，防止自问自答死循环
        return
    atts = d.get("attachments") or []
    img_url = None
    for at in atts:
        if at.get("content_type") == "image" and at.get("url"):
            img_url = at["url"]
            break
    log("[消息] 收到", t, "来自用户", author[:6] + "***（内容不记录）")
    try:
        # 0) 英文短命令（/clear、clear、reset 等）无需 @，群里裸发也生效
        if re.fullmatch(r"[/!！.。]*\s*(clear|reset|cls|重开|新对话)",
                        content.strip().lower()):
            n = do_clear(key)
            log("[清空] 短命令触发，已删除", n, "条历史")
            send_reply(t, d, f"上下文已清空（共清除 {n} 条记录），我们从零开始聊吧～")
            return
        if t != "C2C_MESSAGE_CREATE":
            if t == "GROUP_AT_MESSAGE_CREATE":
                mentioned = True             # 公域群事件：被 @ 才推送，事件本身就是点名
            else:
                mentions = d.get("mentions") or []
                for m in mentions:
                    if isinstance(m, dict) and (m.get("is_you") or m.get("bot") is True):
                        for f in ("member_openid", "id", "user_openid"):
                            if m.get(f):
                                with HIST_LOCK:
                                    MY_OPENIDS.add(m[f])
                at_ids = set(re.findall(r"<@[!&]?([0-9A-Fa-f]+)>", raw_content))
                mentioned = (
                    any(isinstance(m, dict) and (m.get("is_you") or m.get("bot") is True)
                        for m in mentions)
                    or bool(MY_OPENIDS & at_ids)
                    or mentions_me(mentions)
                )
            log("[群消息]", t, "| 点名判定:", mentioned)
            if not mentioned:
                if content or img_url:
                    if judge_should_reply(key, content, speaker):
                        send_reply(t, d, make_reply(content, key, speaker))
                    else:
                        record_only(key, speaker, content or "[图片]")
                return
        # 私聊消息，或群里点名 @ 它的消息
        if is_clear_cmd(content):
            n = do_clear(key)
            log("[清空] 已删除会话", str(key)[:6] + "*** 的", n, "条历史")
            send_reply(t, d, f"上下文已清空（共清除 {n} 条记录），我们从零开始聊吧～")
        elif t != "C2C_MESSAGE_CREATE" and re.search(r"这?个群(?:叫|名为|叫做|是)\s*(\S{1,20})", content):
            gname = re.search(r"这?个群(?:叫|名为|叫做|是)\s*(\S{1,20})", content).group(1).strip()
            KNOWN_GROUPS[key] = gname
            send_reply(t, d, f"好，这个群我记成「{gname}」了。之后跟我说「在{gname}说内容」，我就帮你带话过去。")
        elif any(k in content for k in ("几个群", "哪些群", "群列表")):
            if KNOWN_GROUPS:
                send_reply(t, d, "我知道 " + str(len(KNOWN_GROUPS)) + " 个群：" + "、".join(KNOWN_GROUPS.values()))
            else:
                send_reply(t, d, "目前还没有群跟我互动过。在群里 @ 我说句话，我就记住了。")
        elif re.search(r"在\s*(.+?)\s*(?:群里?|群)?(?:说|发|讲|喊|带一句)\s*(.+)", content):
            m2 = re.search(r"在\s*(.+?)\s*(?:群里?|群)?(?:说|发|讲|喊|带一句)\s*(.+)", content)
            target, msg = m2.group(1).strip(), m2.group(2).strip()
            gid = find_group(target)
            generic = len(target) <= 2 or target in ("这个群", "那个群", "本群", "群里", "群")
            if generic:
                pass
            elif not gid:
                send_reply(t, d, f"没找到叫「{target}」的群。先在那个群里 @ 我说「记住这个群叫XX」，我才能往那儿带话。")
            elif send_active(gid, msg):
                send_reply(t, d, f"已帮你在「{KNOWN_GROUPS[gid]}」说了：{msg}")
            else:
                send_reply(t, d, "这条没发出去，稍后再试一次吧。")
        elif img_url:
            datauri = fetch_image_datauri(img_url)
            if datauri:
                send_reply(t, d, make_reply(content, key, speaker, image_datauri=datauri))
            else:
                send_reply(t, d, "图片下载失败了，可能是链接过期，再发一次试试？")
        elif content:
            send_reply(t, d, make_reply(content, key, speaker))
    except Exception as e:
        log("[回复异常]", e)


def on_error(wsa, error):
    log("[错误]", error)


def on_close(wsa, code, reason):
    log("[断开] code=", code, "reason=", reason)


def watchdog():
    """看门狗：若长时间没收到心跳确认(op=11)，说明连接已假死，强制关闭触发重连。"""
    while True:
        time.sleep(15)
        try:
            if ws and (time.time() - last_heartbeat_ack) > (heartbeat_interval * 2 + 20):
                log("[看门狗] 超过 2 个心跳周期未收到 op=11，强制重连")
                try:
                    ws.close()
                except Exception:
                    pass
        except Exception:
            pass


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
            # 关闭库自带的 WS ping（我们用应用层 op=1 心跳），避免两套心跳互相干扰
            ws.run_forever(ping_interval=0, ping_timeout=0)
        except Exception as e:
            log("[异常]", e)
        time.sleep(1)


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
        elif self.path == "/memory":
            # 查看当前会话数与每个会话的消息条数（只显示条数，不显示内容）
            if not get_session(self):
                self._send(401, "unauthorized")
                return
            detail = {}
            for k, v in histories.items():
                u = sum(1 for m in v if m.get("role") == "user")
                s = sum(1 for m in v if m.get("role") == "assistant")
                detail[k[:6] + "***"] = {"用户消息": u, "机器人回复": s}
            self._send(200, json.dumps({"sessions": len(histories), "detail": detail},
                                       ensure_ascii=False), "application/json")
        elif self.path in ("/memory/clear", "/clear"):
            # 清空所有人的上下文
            if not get_session(self):
                self._send(401, "unauthorized")
                return
            n = sum(len(v) for v in histories.values())
            histories.clear()
            self._send(200, json.dumps({"ok": True, "cleared": n, "sessions": 0},
                                       ensure_ascii=False), "application/json")
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
        if self.path in ("/memory/clear", "/clear"):
            # 清空所有人的上下文（真正从内存删除）
            if not get_session(self):
                self._send(401, "unauthorized")
                return
            n = sum(len(v) for v in histories.values())
            histories.clear()
            self._send(200, json.dumps({"ok": True, "cleared": n, "sessions": 0},
                                       ensure_ascii=False), "application/json")
            return
        self._send(404, "not found")

    def log_message(self, *a):
        pass


def main():
    # 后台拉起机器人、自唤醒与看门狗
    threading.Thread(target=bot_main, daemon=True).start()
    threading.Thread(target=self_ping_loop, daemon=True).start()
    threading.Thread(target=watchdog, daemon=True).start()
    srv = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    log(f"[HTTP] 健康检查+网页终端监听 0.0.0.0:{PORT}")
    srv.serve_forever()


if __name__ == "__main__":
    main()
