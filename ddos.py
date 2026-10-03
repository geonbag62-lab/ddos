#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DDOS v3.1 (Performance Boosted)
- 소규모 네트워크 부하(스트레스) 테스트 도구 (GUI + CLI)
- 본인 소유이거나 테스트 허가를 받은 대상에만 사용하세요.

실행 방법:
  python ddos.py                                   -> GUI 모드 (기본)
  python ddos.py --cli -t IP -p PORT --protocol udp --size 4000 --time 30 --rate 200 -> 명령줄 모드
"""

import argparse
import math
import os
import random
import socket
import sys
import threading
import time
import tkinter.font as tkfont

try:
    import tkinter as tk
    from tkinter import messagebox
except ImportError:
    tk = None

# ─────────────────────────────────────────────────────────────
# 공통 엔진
# ─────────────────────────────────────────────────────────────
stop_event = threading.Event()
stats_lock = threading.Lock()
stats = {"packets": 0, "bytes": 0, "errors": 0, "reconnects": 0}

def reset_stats():
    with stats_lock:
        stats["packets"] = 0
        stats["bytes"] = 0
        stats["errors"] = 0
        stats["reconnects"] = 0

def snapshot():
    with stats_lock:
        return dict(stats)

def _bump(p, b, e, r=0):
    with stats_lock:
        stats["packets"] += p
        stats["bytes"] += b
        stats["errors"] += e
        stats["reconnects"] += r

def _flush(state, last_flush, final=False):
    """로컬 카운터를 공유 통계에 반영 (0.5초 간격 or 종료 시)."""
    now = time.perf_counter()
    if final or now - last_flush[0] >= 0.5:
        p, b, e, r = state
        if p or b or e or r:
            _bump(p, b, e, r)
            state[0] = state[1] = state[2] = state[3] = 0
        last_flush[0] = now

def udp_worker(target, port, payload, rate_per_thread):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024 * 1024)
    except OSError:
        pass

    state = [0, 0, 0, 0]  # packets, bytes, errors, reconnects
    last_flush = [time.perf_counter()]
    payload_len = len(payload)

    # 배치 단위 계산 (OS time.sleep 한계 극복)
    if rate_per_thread > 0:
        batch_size = max(1, int(rate_per_thread * 0.002))
        batch_interval = batch_size / rate_per_thread
    else:
        batch_size = 50
        batch_interval = 0

    next_send = time.perf_counter()

    while not stop_event.is_set():
        p_cnt = 0
        e_cnt = 0
        for _ in range(batch_size):
            try:
                sock.sendto(payload, (target, port))
                p_cnt += 1
            except OSError:
                e_cnt += 1

        state[0] += p_cnt
        state[1] += p_cnt * payload_len
        state[2] += e_cnt

        if batch_interval > 0:
            next_send += batch_interval
            delay = next_send - time.perf_counter()
            if delay > 0:
                time.sleep(delay)
            else:
                next_send = time.perf_counter()

        _flush(state, last_flush)

    sock.close()
    _flush(state, last_flush, final=True)

def tcp_worker(target, port, payload, rate_per_thread):
    state = [0, 0, 0, 0]
    last_flush = [time.perf_counter()]
    payload_len = len(payload)
    sock = None

    if rate_per_thread > 0:
        batch_size = max(1, int(rate_per_thread * 0.002))
        batch_interval = batch_size / rate_per_thread
    else:
        batch_size = 20
        batch_interval = 0

    next_send = time.perf_counter()

    while not stop_event.is_set():
        if sock is None:
            try:
                sock = socket.create_connection((target, port), timeout=3)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                try:
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024 * 1024)
                except OSError:
                    pass
            except OSError:
                state[2] += 1
                time.sleep(0.2)
                continue

        p_cnt = 0
        e_cnt = 0
        for _ in range(batch_size):
            try:
                sock.sendall(payload)
                p_cnt += 1
            except OSError:
                e_cnt += 1
                try:
                    sock.close()
                except OSError:
                    pass
                sock = None
                state[3] += 1
                break

        state[0] += p_cnt
        state[1] += p_cnt * payload_len
        state[2] += e_cnt

        if batch_interval > 0:
            next_send += batch_interval
            delay = next_send - time.perf_counter()
            if delay > 0:
                time.sleep(delay)
            else:
                next_send = time.perf_counter()

        _flush(state, last_flush)

    if sock is not None:
        try:
            sock.close()
        except OSError:
            pass
    _flush(state, last_flush, final=True)

def http_worker(target, port, size, rate_per_thread):
    pad = "X" * max(0, size)
    # HTTP 패킷 사전 구축 (루프 내 인코딩 오버헤드 제거)
    req_template = (
        f"GET / HTTP/1.1\r\n"
        f"Host: {target}\r\n"
        f"User-Agent: load-test\r\n"
        f"X-Pad: {pad}\r\n"
        f"Connection: keep-alive\r\n\r\n"
    ).encode("utf-8", "ignore")
    req_len = len(req_template)

    state = [0, 0, 0, 0]
    last_flush = [time.perf_counter()]
    sock = None

    if rate_per_thread > 0:
        batch_size = max(1, int(rate_per_thread * 0.002))
        batch_interval = batch_size / rate_per_thread
    else:
        batch_size = 20
        batch_interval = 0

    next_send = time.perf_counter()

    while not stop_event.is_set():
        if sock is None:
            try:
                sock = socket.create_connection((target, port), timeout=3)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            except OSError:
                state[2] += 1
                time.sleep(0.2)
                continue

        p_cnt = 0
        e_cnt = 0
        for _ in range(batch_size):
            try:
                sock.sendall(req_template)
                p_cnt += 1
            except OSError:
                e_cnt += 1
                try:
                    sock.close()
                except OSError:
                    pass
                sock = None
                state[3] += 1
                break

        state[0] += p_cnt
        state[1] += p_cnt * req_len
        state[2] += e_cnt

        if batch_interval > 0:
            next_send += batch_interval
            delay = next_send - time.perf_counter()
            if delay > 0:
                time.sleep(delay)
            else:
                next_send = time.perf_counter()

        _flush(state, last_flush)

    if sock is not None:
        try:
            sock.close()
        except OSError:
            pass
    _flush(state, last_flush, final=True)

WORKERS = {"udp": udp_worker, "tcp": tcp_worker, "http": http_worker}

def validate_cfg(target, port, protocol, size, duration, rate, threads, force=False):
    """설정 검사 (CLI용). 문제가 없으면 None, 있으면 오류 메시지를 반환."""
    try:
        port = int(port)
        size = int(size)
        threads = int(threads)
        duration = float(duration)
        rate = float(rate)
    except (TypeError, ValueError):
        return "포트/크기/스레드는 정수, 시간과 속도는 숫자로 입력하세요"

    if not str(target).strip():
        return "대상 주소를 입력하세요"
    if not (1 <= port <= 65535):
        return "포트는 1~65535 범위여야 합니다"

    parts = str(target).split(".")
    if not force and len(parts) == 4 and parts[3] in ("0", "255"):
        return "브로드캐스트/네트워크 주소로 보입니다. 개별 호스트 IP를 입력하세요"

    if protocol == "udp" and not (1 <= size <= 65507):
        return "UDP 패킷 크기는 1~65507 bytes"
    if not (1 <= size <= 1048576):
        return "패킷 크기는 1~1048576 bytes"
    if not (1 <= threads <= 64):
        return "스레드 수는 1~64"
    if duration <= 0:
        return "지속 시간은 0보다 커야 합니다"
    if rate <= 0:
        return "속도(pps)는 0보다 커야 합니다"

    return None

def spawn_workers(target, port, protocol, size, rate, threads):
    """워커 스레드 시작. 스레드 리스트를 반환."""
    stop_event.clear()
    rate_per_thread = rate / max(1, threads) if rate > 0 else 0.0

    if protocol == "http":
        worker_fn = lambda i: WORKERS[protocol](target, port, size, rate_per_thread)
    else:
        payload = os.urandom(size)
        worker_fn = lambda i: WORKERS[protocol](target, port, payload, rate_per_thread)

    result = []
    for i in range(threads):
        t = threading.Thread(target=worker_fn, args=(i,), daemon=True)
        t.start()
        result.append(t)
    return result

def auto_threads(rate):
    """강도에 맞춘 적정 동시 작업 수."""
    return max(2, min(32, round(rate / 250)))

# ─────────────────────────────────────────────────────────────
# 색상 팔레트 — GitHub 다크
# ─────────────────────────────────────────────────────────────
C_BG = "#0d1117"
C_CARD = "#161b22"
C_CARD2 = "#1c2128"
C_LINE = "#30363d"
C_LINE_HI = "#3d444d"
C_FG = "#e6edf3"
C_DIM = "#8b949e"
C_FAINT = "#6e7681"
C_ACCENT = "#58a6ff"
C_DEEP = "#1f6feb"
C_GREEN = "#3fb950"
C_GREEN_HI = "#56d364"
C_BTN_GREEN = "#238636"
C_BTN_GREEN_HV = "#29903b"
C_RED = "#f85149"
C_PURPLE = "#a371f7"
C_AMBER = "#d29922"
C_LOG_BG = "#010409"
C_TRACK = "#21262d"

def _mix(c1, c2, t):
    """두 #rrggbb 색 혼합 (t=0 -> c1, t=1 -> c2)."""
    c1 = c1.lstrip("#")
    c2 = c2.lstrip("#")
    r = round(int(c1[0:2], 16) * (1 - t) + int(c2[0:2], 16) * t)
    g = round(int(c1[2:4], 16) * (1 - t) + int(c2[2:4], 16) * t)
    b = round(int(c1[4:6], 16) * (1 - t) + int(c2[4:6], 16) * t)
    return f"#{r:02x}{g:02x}{b:02x}"

def round_rect(cv, x1, y1, x2, y2, r, **kw):
    """캔버스용 둥근 사각형 폴리곤."""
    pts = [
        x1 + r, y1, x2 - r, y1, x2, y1, x2, y1 + r,
        x2, y2 - r, x2, y2, x2 - r, y2, x1 + r, y2,
        x1, y2, x1, y2 - r, x1, y1 + r, x1, y1
    ]
    return cv.create_polygon(pts, smooth=True, **kw)

# ─────────────────────────────────────────────────────────────
# 커스텀 위젯
# ─────────────────────────────────────────────────────────────
class RoundedButton(tk.Canvas):
    def __init__(self, master, text, command=None, bg=C_CARD, fg=C_FG, hover_bg=None, font=("Segoe UI", 10, "bold"), padx=16, pady=8, radius=9):
        self._text = text
        self._command = command
        self._bg = bg
        self._fg = fg
        self._hover_bg = hover_bg or _mix(bg, "#ffffff", 0.12)
        self._radius = radius
        self._padx = padx
        self._pady = pady
        self._font = font
        self._enabled = True
        self._hovering = False
        super().__init__(master, highlightthickness=0, bd=0, bg=master["bg"], cursor="hand2")
        self._apply_size()
        self.bind("<Button-1>", self._click)
        self.bind("<Enter>", lambda e: self._set_hover(True))
        self.bind("<Leave>", lambda e: self._set_hover(False))

    def _apply_size(self):
        f = tkfont.Font(font=self._font)
        lines = self._text.split("\n")
        w = max(f.measure(s) for s in lines) + 2 * self._padx
        h = f.metrics("linespace") * len(lines) + 2 * self._pady
        self.configure(width=w, height=h)
        self._bw, self._bh = w, h

    def _render(self, bg, fg):
        self.delete("all")
        round_rect(self, 1, 1, self._bw - 2, self._bh - 2, self._radius, fill=bg, outline=bg)
        self.create_text(self._bw / 2, self._bh / 2, text=self._text, fill=fg, font=self._font, justify="center")

    def _paint(self):
        if not self._enabled:
            self._render("#21262d", "#484f58")
        elif self._hovering:
            self._render(self._hover_bg, self._fg)
        else:
            self._render(self._bg, self._fg)

    def _set_hover(self, on):
        self._hovering = on
        if self._enabled:
            self._paint()

    def _click(self, _):
        if self._enabled and self._command:
            self._command()

    def set_enabled(self, on):
        self._enabled = on
        self.configure(cursor="hand2" if on else "arrow")
        self._paint()

    def set_style(self, text=None, bg=None, fg=None, hover_bg=None):
        if text is not None and text != self._text:
            self._text = text
            self._apply_size()
        if bg is not None:
            self._bg = bg
            self._hover_bg = hover_bg or _mix(bg, "#ffffff", 0.12)
        if fg is not None:
            self._fg = fg
        self._paint()

class NeonSlider(tk.Canvas):
    PAD = 12

    def __init__(self, master, value=0, command=None, width=240):
        super().__init__(master, width=width, height=30, bg=master["bg"], highlightthickness=0)
        self._value = value
        self._command = command
        self._hover = False
        self.bind("<Button-1>", self._press)
        self.bind("<B1-Motion>", self._drag)
        self.bind("<ButtonRelease-1>", lambda e: self._set_hover(False))
        self.bind("<Enter>", lambda e: (setattr(self, "_hover", True), self._draw()))
        self.bind("<Leave>", lambda e: (setattr(self, "_hover", False), self._draw()))
        self.bind("<Configure>", lambda e: self._draw())
        self._draw()

    def _frac(self):
        return self._value / 1000.0

    def _x_to_val(self, x):
        w = max(1, self.winfo_width() - 2 * self.PAD)
        frac = min(1.0, max(0.0, (x - self.PAD) / w))
        return round(frac * 1000)

    def _press(self, e):
        self.set(self._x_to_val(e.x), fire=True)

    def _drag(self, e):
        self.set(self._x_to_val(e.x), fire=True)

    def get(self):
        return self._value

    def set(self, v, fire=False):
        self._value = int(min(1000, max(0, v)))
        self._draw()
        if fire and self._command:
            self._command(self._value)

    def _draw(self):
        self.delete("all")
        w = self.winfo_width()
        if w <= 1:
            w = int(self["width"])
        cy = 15
        tr = 4
        x1, x2 = self.PAD, w - self.PAD
        round_rect(self, x1, cy - tr, x2, cy + tr, tr, fill=C_TRACK, outline=C_TRACK)
        fx = x1 + (x2 - x1) * self._frac()
        if fx > x1 + tr:
            round_rect(self, x1, cy - tr, fx, cy + tr, tr, fill=C_DEEP, outline=C_DEEP)
        r = 9 if self._hover else 8
        self.create_oval(fx - r, cy - r, fx + r, cy + r, fill="#ffffff", outline=C_ACCENT if self._hover else C_LINE, width=2)

class GlowEntry(tk.Entry):
    def __init__(self, master, width=10, placeholder="", mono=True, textvariable=None, initial=""):
        self._ph = placeholder
        self._has_var = textvariable is not None
        super().__init__(
            master,
            width=width,
            bg="#010409",
            fg=C_FG,
            insertbackground=C_FG,
            relief="flat",
            highlightthickness=1,
            highlightbackground=C_LINE,
            highlightcolor=C_DEEP,
            font=("Consolas", 11) if mono else ("Segoe UI", 10),
            textvariable=textvariable,
        )
        if initial:
            self.insert(0, initial)
        elif self._ph and not self._has_var:
            self.bind("<FocusIn>", self._focus_in, add="+")
            self.bind("<FocusOut>", self._focus_out, add="+")
            self._show_ph()
        self.bind("<FocusIn>", lambda e: self.configure(highlightbackground=C_DEEP, highlightcolor=C_DEEP), add="+")
        self.bind("<FocusOut>", lambda e: self.configure(highlightbackground=C_LINE, highlightcolor=C_LINE), add="+")

    def _show_ph(self):
        if not self.get():
            self.configure(fg=C_FAINT)
            self.insert(0, self._ph)
            self._ph_on = True

    def _focus_in(self, _=None):
        if getattr(self, "_ph_on", False):
            self.delete(0, "end")
            self.configure(fg=C_FG)
            self._ph_on = False

    def _focus_out(self, _=None):
        if not self.get():
            self._show_ph()

    def real_get(self):
        v = self.get()
        if getattr(self, "_ph_on", False) or (self._ph and v == self._ph):
            return ""
        return v

class StatusPill(tk.Frame):
    def __init__(self, master):
        super().__init__(master, bg=master["bg"])
        self._dot = tk.Canvas(self, width=10, height=10, bg=master["bg"], highlightthickness=0)
        self._dot.pack(side="left", padx=(0, 6))
        self._lbl = tk.Label(self, text="대기 중", bg=master["bg"], fg=C_DIM, font=("Segoe UI", 9, "bold"))
        self._lbl.pack(side="left")
        self._pulse = False
        self._on = True
        self._color = C_FAINT

    def set(self, color, text, pulse=False):
        self._color = color
        self._pulse = pulse
        self._on = True
        self._lbl.configure(text=text, fg=color)
        self._draw_dot()

    def _draw_dot(self):
        self._dot.delete("all")
        self._dot.create_oval(1, 1, 9, 9, fill=self._color if self._on else _mix(self._color, C_BG, 0.6), outline="")

    def tick(self):
        if self._pulse:
            self._on = not self._on
            self._draw_dot()

# ─────────────────────────────────────────────────────────────
# GUI
# ─────────────────────────────────────────────────────────────
METHODS = [
    ("udp", "빠르게", "연결 없이 그냥 던지는 방식. 가볍고 빨라요"),
    ("tcp", "연결형", "상대와 연결을 맺은 뒤 보내는 방식"),
    ("http", "웹요청", "웹서버에 페이지 요청을 계속 보내는 방식"),
]
RATE_MIN = 10
RATE_MAX = 20000

def slider_to_rate(pos):
    rate = RATE_MIN * (RATE_MAX / RATE_MIN) ** (pos / 1000)
    if rate >= 1000:
        return int(round(rate, -2))
    if rate >= 100:
        return int(round(rate, -1))
    return max(1, int(round(rate)))

def rate_to_slider(rate):
    rate = max(RATE_MIN, min(RATE_MAX, rate))
    return round(1000 * math.log(rate / RATE_MIN) / math.log(RATE_MAX / RATE_MIN))

class StressGUI:
    def __init__(self, root):
        self.root = root
        root.title("load-test · 부하 테스트")
        root.geometry("700x840")
        root.minsize(600, 720)
        root.configure(bg=C_BG)

        self.workers = []
        self.running = False
        self.stopping = False
        self.start_time = None
        self.duration = 0.0
        self.last_p = 0
        self.last_b = 0
        self.last_log = 0.0
        self._syncing = False
        self._anim_id = None
        self._shimmer_phase = 0.0

        self._build()
        self._center()
        root.protocol("WM_DELETE_WINDOW", self._on_close)

    def _card(self, parent, stripe=None, **pack):
        outer = tk.Frame(parent, bg=C_BG)
        outer.pack(**pack)
        f = tk.Frame(outer, bg=C_CARD, highlightbackground=C_LINE, highlightthickness=1)
        f.pack(fill="x")
        if stripe:
            tk.Frame(f, bg=stripe, height=2).pack(fill="x", padx=1, pady=(1, 0))
        return f

    def _sec(self, parent, text):
        f = tk.Frame(parent, bg=parent["bg"])
        f.pack(anchor="w", padx=14, pady=(12, 3))
        tk.Label(f, text="//", bg=f["bg"], fg=C_FAINT, font=("Consolas", 9, "bold")).pack(side="left")
        tk.Label(f, text=" " + text, bg=f["bg"], fg=C_DIM, font=("Segoe UI", 9, "bold")).pack(side="left")
        return f

    def _label(self, parent, text, color=C_FG, size=10, bold=False, mono=False):
        return tk.Label(parent, text=text, bg=parent["bg"], fg=color, font=("Consolas" if mono else "Segoe UI", size, "bold" if bold else "normal"))

    def _build(self):
        head = tk.Frame(self.root, bg=C_BG)
        head.pack(fill="x", padx=16, pady=(14, 0))
        for col in (C_RED, C_AMBER, C_GREEN):
            self._label(head, "●", color=col, size=8).pack(side="left", padx=(0, 3))
        self._label(head, "  load-test", color=C_FG, size=12, bold=True, mono=True).pack(side="left", padx=(8, 0))

        ver = tk.Frame(head, bg=C_CARD, highlightbackground=C_LINE, highlightthickness=1)
        ver.pack(side="left", padx=(8, 0))
        self._label(ver, " v3.1 ", color=C_ACCENT, size=8, bold=True, mono=True).pack(padx=5, pady=1)

        self.status_pill = StatusPill(head)
        self.status_pill.pack(side="right")
        self._label(self.root, "내 서버나 허가받은 곳만 테스트하세요", color=C_AMBER, size=9).pack(anchor="w", padx=16, pady=(4, 0))

        # 대상 카드
        c1 = self._card(self.root, stripe=C_ACCENT, fill="x", padx=16, pady=(10, 0))
        self._sec(c1, "대상")
        tgt = tk.Frame(c1, bg=C_CARD)
        tgt.pack(fill="x", padx=14, pady=(0, 4))
        self.ent_target = GlowEntry(tgt, width=24, placeholder="예: 192.168.0.10")
        self.ent_target.pack(side="left", ipady=5)
        self._label(tgt, " : ", color=C_FAINT, size=12, mono=True).pack(side="left")
        self.ent_port = GlowEntry(tgt, width=7, initial="80")
        self.ent_port.pack(side="left", ipady=5)
        self.lbl_route = self._label(tgt, "", color=C_FAINT, size=9, mono=True)
        self.lbl_route.pack(side="left", padx=(10, 0))
        self.ent_target.bind("<KeyRelease>", lambda e: self._update_route())
        self._update_route()

        # 방식 카드
        c2 = self._card(self.root, stripe=C_PURPLE, fill="x", padx=16, pady=(8, 0))
        self._sec(c2, "방식")
        seg = tk.Frame(c2, bg=C_CARD)
        seg.pack(anchor="w", padx=14, pady=(0, 2))
        self.var_method = tk.StringVar(value="udp")
        self.method_buttons = []
        for key, name, _ in METHODS:
            b = RoundedButton(
                seg, name, command=lambda k=key: self._pick_method(k), bg=C_CARD2, fg=C_DIM, hover_bg="#22272e", font=("Segoe UI", 10, "bold"), padx=18, pady=6
            )
            b.pack(side="left", padx=(0, 6))
            self.method_buttons.append((key, b))
        self.lbl_method_hint = tk.Label(c2, text="", bg=C_CARD, fg=C_FAINT, font=("Segoe UI", 9))
        self.lbl_method_hint.pack(anchor="w", padx=16, pady=(0, 6))
        self._pick_method("udp")

        # 강도 카드
        c3 = self._card(self.root, stripe=C_AMBER, fill="x", padx=16, pady=(8, 0))
        self._sec(c3, "강도 · 시간")
        row1 = tk.Frame(c3, bg=C_CARD)
        row1.pack(fill="x", padx=14, pady=(0, 2))
        self.var_rate = tk.StringVar(value="100")
        self.var_rate.trace_add("write", self._on_rate_typed)
        self.ent_rate = GlowEntry(row1, width=7, textvariable=self.var_rate)
        self.ent_rate.pack(side="left", ipady=5)
        self._label(row1, "  회/초", color=C_FAINT, size=9).pack(side="left")
        self.sld = NeonSlider(row1, value=rate_to_slider(100), command=self._on_slider_moved)
        self.sld.pack(side="left", padx=(14, 0), fill="x", expand=True)

        row2 = tk.Frame(c3, bg=C_CARD)
        row2.pack(fill="x", padx=14, pady=(4, 2))
        self.ent_time = GlowEntry(row2, width=7, initial="10")
        self.ent_time.pack(side="left", ipady=5)
        self._label(row2, "  초 동안", color=C_FAINT, size=9).pack(side="left")

        row3 = tk.Frame(c3, bg=C_CARD)
        row3.pack(fill="x", padx=14, pady=(2, 2))
        self.ent_size = GlowEntry(row3, width=7, initial="4000")
        self.ent_size.pack(side="left", ipady=5)
        self._label(row3, "  크기 (바이트) · 4000이면 보통 크기", color=C_FAINT, size=9).pack(side="left")

        self.lbl_est = self._label(c3, "", color=C_FAINT, size=9, mono=True)
        self.lbl_est.pack(anchor="e", padx=16, pady=(0, 4))
        self.ent_time.bind("<KeyRelease>", lambda e: self._update_est())
        self.ent_size.bind("<KeyRelease>", lambda e: self._update_est())
        self.var_rate.trace_add("write", lambda *a: self._update_est())
        self._update_est()

        chips = tk.Frame(c3, bg=C_CARD)
        chips.pack(anchor="w", padx=14, pady=(4, 10))
        self._label(chips, "프리셋 ", color=C_FAINT, size=9).pack(side="left")
        for text, rate, secs in (("살짝 · 10초", 100, 10), ("보통 · 30초", 500, 30), ("강하게 · 1분", 2000, 60)):
            RoundedButton(
                chips,
                text,
                command=lambda r=rate, s=secs: self._preset(r, s),
                bg=C_CARD2,
                fg=C_ACCENT,
                hover_bg="#1c2a3d",
                font=("Segoe UI", 9, "bold"),
                padx=12,
                pady=5,
                radius=8,
            ).pack(side="left", padx=(6, 0))

        # 시작/중지
        ctrl = tk.Frame(self.root, bg=C_BG)
        ctrl.pack(fill="x", padx=16, pady=(14, 0))
        self.btn_start = RoundedButton(
            ctrl, "▶   시작", command=self.start, bg=C_BTN_GREEN, fg="#ffffff", hover_bg=C_BTN_GREEN_HV, font=("Segoe UI", 12, "bold"), padx=20, pady=10, radius=10
        )
        self.btn_start.pack(side="left", fill="x", expand=True)
        self.btn_stop = RoundedButton(
            ctrl, "■", command=self.stop, bg="#21262d", fg=C_RED, hover_bg="#301b1e", font=("Segoe UI", 12, "bold"), padx=22, pady=10, radius=10
        )
        self.btn_stop.pack(side="left", padx=(8, 0))
        self.btn_stop.set_enabled(False)

        # 통계 카드
        sgrid = tk.Frame(self.root, bg=C_BG)
        sgrid.pack(fill="x", padx=16, pady=(14, 0))
        sgrid.columnconfigure((0, 1, 2, 3), weight=1, uniform="s")
        self.stat_labels = {}
        stat_meta = (("pkts", "보낸 횟수", C_ACCENT), ("pps", "초당", C_PURPLE), ("band", "전송량", C_GREEN), ("fail", "실패", C_RED))
        for i, (key, name, col) in enumerate(stat_meta):
            cell = tk.Frame(sgrid, bg=C_CARD, highlightbackground=C_LINE, highlightthickness=1)
            cell.grid(row=0, column=i, sticky="nsew", padx=(0 if i == 0 else 6, 0))
            tk.Frame(cell, bg=col, height=2).pack(fill="x", padx=1, pady=(1, 0))
            self._label(cell, name, color=C_FAINT, size=8).pack(anchor="w", padx=10, pady=(7, 0))
            val = self._label(cell, "0", color=C_FG if key != "pps" else col, size=15, bold=True, mono=True)
            val.pack(anchor="w", padx=10, pady=(1, 8))
            self.stat_labels[key] = val

        # 진행 바
        pbwrap = tk.Frame(self.root, bg=C_BG)
        pbwrap.pack(fill="x", padx=16, pady=(12, 0))
        prow = tk.Frame(pbwrap, bg=C_BG)
        prow.pack(fill="x")
        self.canvas_pb = tk.Canvas(prow, height=12, bg=C_BG, highlightthickness=0)
        self.canvas_pb.pack(side="left", fill="x", expand=True)
        self.canvas_pb.bind("<Configure>", lambda e: self._draw_pb())
        self.lbl_pct = self._label(prow, "0%", color=C_FAINT, size=9, mono=True)
        self.lbl_pct.pack(side="right", padx=(8, 0))
        self._pb_frac = 0.0

        # 로그
        logcard = self._card(self.root, fill="both", expand=True, padx=16, pady=(12, 0))
        loghead = tk.Frame(logcard, bg=C_CARD)
        loghead.pack(fill="x", padx=12, pady=(8, 2))
        self._label(loghead, "//", color=C_FAINT, size=9, bold=True).pack(side="left")
        self._label(loghead, " 기록", color=C_DIM, size=9, bold=True).pack(side="left")
        self.txt_log = tk.Text(
            logcard,
            height=8,
            state="disabled",
            wrap="none",
            bg=C_LOG_BG,
            fg=C_DIM,
            relief="flat",
            borderwidth=0,
            highlightthickness=0,
            insertbackground=C_FG,
            font=("Consolas", 9),
        )
        from tkinter import ttk as _ttk

        sb = _ttk.Scrollbar(logcard, orient="vertical", style="Dark.Vertical.TScrollbar", command=self.txt_log.yview)
        self.txt_log.configure(yscrollcommand=sb.set)
        sb.pack(side="right", fill="y", padx=(0, 2), pady=(0, 8))
        self.txt_log.pack(fill="both", expand=True, padx=10, pady=(0, 8))

        for tag, color in (
            ("info", C_DIM),
            ("accent", C_ACCENT),
            ("ok", C_GREEN),
            ("warn", C_AMBER),
            ("err", C_RED),
            ("sum", C_FG),
        ):
            self.txt_log.tag_configure(tag, foreground=color)

        # 푸터
        foot = tk.Frame(self.root, bg=C_CARD, highlightbackground=C_LINE, highlightthickness=1)
        foot.pack(fill="x", side="bottom")
        self.foot_left = tk.Label(foot, text=" ready", bg=C_CARD, fg=C_FAINT, font=("Consolas", 9))
        self.foot_left.pack(side="left", padx=10, pady=4)
        tk.Label(foot, text="Enter 시작 · Esc 중지", bg=C_CARD, fg=C_FAINT, font=("Consolas", 9)).pack(side="right", padx=10, pady=4)

        self._style_scrollbars()
        self.root.bind("<Return>", lambda e: self.start() if not self.running else None)
        self.root.bind("<Escape>", lambda e: self.stop() if self.running else None)

    def _style_scrollbars(self):
        from tkinter import ttk

        style = ttk.Style(self.root)
        try:
            style.theme_use("clam")
        except tk.TclError:
            pass
        style.configure("Dark.Vertical.TScrollbar", background=C_CARD, troughcolor=C_LOG_BG, bordercolor=C_LOG_BG, arrowcolor=C_DIM)

    def _center(self):
        self.root.update_idletasks()
        w, h = self.root.winfo_width(), self.root.winfo_height()
        x = (self.root.winfo_screenwidth() - w) // 2
        y = (self.root.winfo_screenheight() - h) // 3
        self.root.geometry(f"+{x}+{y}")

    def _update_route(self):
        t = self.ent_target.real_get().strip()
        if t and self.ent_port.get():
            self.lbl_route.configure(text=f"→  {t}:{self.ent_port.get()}")
        else:
            self.lbl_route.configure(text="")

    def _pick_method(self, key):
        self.var_method.set(key)
        for k, b in self.method_buttons:
            on = k == key
            b.set_style(bg=C_DEEP if on else C_CARD2, fg="#ffffff" if on else C_DIM, hover_bg=C_DEEP if on else "#22272e")
        self.lbl_method_hint.configure(text="↳ " + next(h for k, _, h in METHODS if k == key))

    def _on_slider_moved(self, _):
        if self._syncing:
            return
        self._syncing = True
        self.var_rate.set(str(slider_to_rate(self.sld.get())))
        self._syncing = False

    def _on_rate_typed(self, *_):
        if self._syncing:
            return
        try:
            rate = int(self.var_rate.get())
        except ValueError:
            return
        if rate <= 0:
            return
        self._syncing = True
        self.sld.set(rate_to_slider(rate))
        self._syncing = False

    def set_rate(self, rate):
        rate = int(max(1, rate))
        self._syncing = True
        self.var_rate.set(str(rate))
        self.sld.set(rate_to_slider(rate))
        self._syncing = False

    def _update_est(self):
        try:
            rate = int(self.var_rate.get())
            secs = float(self.ent_time.get())
            size = int(self.ent_size.get())
            mb = rate * size * secs / 1024 / 1024
            self.lbl_est.configure(text=f"예상 약 {mb:,.1f} MB")
        except (ValueError, AttributeError):
            self.lbl_est.configure(text="")

    def _preset(self, rate, secs):
        self.set_rate(rate)
        self.ent_time.delete(0, "end")
        self.ent_time.insert(0, str(secs))
        self._update_est()

    def _draw_pb(self):
        cv = self.canvas_pb
        cv.delete("all")
        w = cv.winfo_width()
        if w <= 1:
            return
        h = 12
        round_rect(cv, 0, 0, w, h, 6, fill=C_TRACK, outline=C_TRACK)
        fw = int(w * self._pb_frac)
        if fw > 12:
            round_rect(cv, 0, 0, fw, h, 6, fill=C_DEEP, outline=C_DEEP)

        band = 26
        x0 = int((self._shimmer_phase % (fw + band)) - band)
        if fw > band + 10:
            cv.create_rectangle(max(0, x0), 2, min(fw - 2, x0 + band), h - 2, fill=_mix(C_DEEP, "#79b8ff", 0.55), width=0)

    def _log(self, line, tag="info"):
        self.txt_log.configure(state="normal")
        self.txt_log.insert("end", line + "\n", tag)
        self.txt_log.see("end")
        self.txt_log.configure(state="disabled")

    def _set_foot(self, text, color):
        self.foot_left.configure(text=f" {text}", fg=color)

    def _check(self):
        target = self.ent_target.real_get().strip()
        if not target:
            return "서버 주소를 입력해 주세요"
        try:
            port = int(self.ent_port.get())
            if not (1 <= port <= 65535):
                return "포트는 1~65535 사이 숫자예요"
        except ValueError:
            return "포트는 숫자로 입력해 주세요"

        parts = target.split(".")
        if len(parts) == 4 and parts[3] in ("0", "255"):
            return "주소를 다시 확인해 주세요. 기기 하나하나의 주소가 필요해요\n(예: 192.168.0.10)"

        try:
            size = int(self.ent_size.get())
            if not (1 <= size <= 65507):
                return "크기는 1~65507 사이로 해주세요"
        except ValueError:
            return "크기는 숫자로 입력해 주세요"

        try:
            duration = float(self.ent_time.get())
            if not (0 < duration <= 86400):
                return "시간은 1초 이상, 24시간 이내로 해주세요"
        except ValueError:
            return "시간은 숫자로 입력해 주세요 (초 단위)"

        try:
            rate = int(self.var_rate.get())
            if rate < 1:
                return "강도는 1 이상으로 해주세요"
        except ValueError:
            return "강도는 숫자예요. 슬라이더를 쓰거나 숫자를 입력해 주세요"

        return None

    def start(self):
        if self.running:
            return
        err = self._check()
        if err:
            messagebox.showwarning("확인 필요", err)
            return

        target = self.ent_target.real_get().strip()
        port = int(self.ent_port.get())
        protocol = self.var_method.get()
        size = int(self.ent_size.get())
        self.duration = float(self.ent_time.get())
        rate = int(self.var_rate.get())
        threads = auto_threads(rate)
        mname = next(n for k, n, _ in METHODS if k == protocol)

        reset_stats()
        self.last_p = self.last_b = 0
        self.last_log = time.perf_counter()
        self.start_time = time.perf_counter()
        self.stopping = False
        self.workers = spawn_workers(target, port, protocol, size, rate, threads)
        self.running = True

        est_mb = rate * size * self.duration / 1024 / 1024
        self.btn_start.set_enabled(False)
        self.btn_stop.set_enabled(True)
        self.status_pill.set(C_GREEN, "실행 중", pulse=True)
        self._set_foot("running", C_GREEN)
        self._log(f"▶ {target}:{port} · {mname} · {self.duration:.0f}초 · " f"초당 {rate}번 · 크기 {size}", "accent")
        self._log(f"  총 {threads}개 작업이 돌아가요 · 예상 전송량 약 {est_mb:.1f} MB", "info")

        self._anim_loop()
        self.root.after(200, self._poll)

    def stop(self):
        if self.running and not self.stopping:
            stop_event.set()
            self.stopping = True
            self.status_pill.set(C_AMBER, "멈추는 중", pulse=False)
            self._set_foot("stopping", C_AMBER)
            self._log("■  멈추는 중...", "warn")

    def _anim_loop(self):
        if not self.running:
            return
        self.status_pill.tick()
        self._shimmer_phase += 8
        self._draw_pb()
        self._anim_id = self.root.after(70, self._anim_loop)

    def _poll(self):
        if not self.running:
            return

        now = time.perf_counter()
        elapsed = now - self.start_time
        self._pb_frac = min(1.0, elapsed / self.duration) if self.duration else 0
        self.lbl_pct.configure(text=f"{int(self._pb_frac * 100)}%")

        snap = snapshot()
        if now - self.last_log >= 1.0:
            dt = max(0.001, now - self.last_log)
            pps = (snap["packets"] - self.last_p) / dt
            mbs = (snap["bytes"] - self.last_b) / dt / 1024 / 1024

            self.stat_labels["pkts"].configure(text=f"{snap['packets']:,}")
            self.stat_labels["pps"].configure(text=f"{pps:.0f}")
            self.stat_labels["band"].configure(text=f"{mbs:.2f} MB/s")

            fail = snap["errors"] + snap["reconnects"]
            self.stat_labels["fail"].configure(text=str(fail), fg=C_RED if fail else C_FG)

            remain = max(0.0, self.duration - elapsed)
            self._log(f"[{elapsed:6.1f}초] {snap['packets']:>9,}번 · " f"초당 {pps:>5.0f} · {mbs:>6.2f} MB/s · 실패 {fail} · " f"{remain:.0f}초 남음")

            self.last_p = snap["packets"]
            self.last_b = snap["bytes"]
            self.last_log = now

        if not self.stopping and elapsed >= self.duration:
            self.stop()

        if self.stopping and all(not t.is_alive() for t in self.workers):
            self._finalize()
            return

        self.root.after(200, self._poll)

    def _finalize(self):
        snap = snapshot()
        elapsed = max(0.001, time.perf_counter() - self.start_time)
        total_mb = snap["bytes"] / 1024 / 1024

        self._log("─" * 46, "info")
        self._log(f"끝! {elapsed:.0f}초 동안 {snap['packets']:,}번 보냈어요 " f"(총 {total_mb:.1f} MB)", "sum")
        self._log(f"평균 초당 {snap['packets'] / elapsed:.0f}번 · " f"실패 {snap['errors'] + snap['reconnects']}회", "ok")

        self._pb_frac = 1.0
        self._draw_pb()
        self.lbl_pct.configure(text="100%")

        self.running = False
        self.stopping = False
        self.btn_start.set_enabled(True)
        self.btn_stop.set_enabled(False)
        self.status_pill.set(C_ACCENT, "완료", pulse=False)
        self._set_foot("done", C_ACCENT)

    def _on_close(self):
        stop_event.set()
        self.root.destroy()

def gui_main():
    if tk is None:
        print("이 환경에는 tkinter가 없습니다. --cli 모드를 사용하세요.")
        sys.exit(1)
    try:
        import ctypes
        ctypes.windll.shcore.SetProcessDpiAwareness(1)
    except Exception:
        pass
    root = tk.Tk()
    StressGUI(root)
    root.mainloop()

# ─────────────────────────────────────────────────────────────
# CLI 모드 (--cli)
# ─────────────────────────────────────────────────────────────
def cli_main():
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass

    ap = argparse.ArgumentParser(description="소규모 네트워크 부하 테스트 도구 (승인된 대상에만 사용)")
    ap.add_argument("-t", "--target", required=True, help="대상 IP/호스트명")
    ap.add_argument("-p", "--port", type=int, required=True, help="대상 포트")
    ap.add_argument("--protocol", choices=["udp", "tcp", "http"], default="udp", help="프로토콜 (기본: udp)")
    ap.add_argument("--size", type=int, default=4000, help="패킷 크기 bytes (기본: 4000)")
    ap.add_argument("--time", type=float, default=10.0, help="지속 시간 초 (기본: 10)")
    ap.add_argument("--rate", type=float, default=100.0, help="초당 패킷 수, 전체 스레드 합계 (기본: 100)")
    ap.add_argument("--threads", type=int, default=2, help="동시 스레드 수 (기본: 2)")
    ap.add_argument("--force", action="store_true", help="브로드캐스트/네트워크 주소 검사 건너뜀")
    args = ap.parse_args()

    err = validate_cfg(args.target, args.port, args.protocol, args.size, args.time, args.rate, args.threads, force=args.force)
    if err:
        print(f"오류: {err}")
        sys.exit(1)

    print(
        f"대상: {args.target}:{args.port} | 프로토콜: {args.protocol.upper()} | "
        f"크기: {args.size}B | 시간: {args.time:.0f}초 | 속도: {args.rate:.0f}pps | "
        f"스레드: {args.threads}"
    )
    print("중지: Ctrl+C\n")

    start = time.perf_counter()
    reset_stats()
    threads = spawn_workers(args.target, args.port, args.protocol, args.size, args.rate, args.threads)
    last_p = last_b = 0

    while time.perf_counter() - start < args.time:
        time.sleep(1.0)
        snap = snapshot()
        now_time = time.perf_counter() - start
        print(
            f"[{now_time:6.1f}초] 누적 {snap['packets']:>8} 패킷 | "
            f"{(snap['packets'] - last_p):>5} pps | "
            f"{(snap['bytes'] - last_b) / 1024:>9.1f} KB/s | 오류 {snap['errors']}"
        )
        last_p, last_b = snap["packets"], snap["bytes"]

    stop_event.set()
    for t in threads:
        t.join(timeout=5)

    snap = snapshot()
    elapsed = time.perf_counter() - start
    print("\n=== 결과 ===")
    print(f"실행 시간   : {elapsed:.1f}초")
    print(f"전송 패킷   : {snap['packets']:,} 개")
    print(f"전송 데이터 : {snap['bytes'] / 1024 / 1024:.2f} MB")
    print(f"평균 속도   : {snap['packets'] / elapsed:.1f} pps, " f"{snap['bytes'] / elapsed / 1024:.1f} KB/s")
    print(f"오류/재접속 : {snap['errors']} / {snap['reconnects']}")

def main():
    if "--cli" in sys.argv:
        sys.argv.remove("--cli")
        cli_main()
    else:
        gui_main()

if __name__ == "__main__":
    main()