#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
load-test v3.2 (정밀 페이싱 · 사전 DNS 확인 · 안정성 개선)
- 소규모 네트워크 부하(스트레스) 테스트 도구 (GUI + CLI)
- 본인 소유이거나 테스트 허가를 받은 대상에만 사용하세요.

실행 방법:
  python ddos.py                                   -> GUI 모드 (기본)
  python ddos.py --cli -t IP -p PORT --protocol udp --size 4000 --time 30 --rate 200 -> 명령줄 모드
  python ddos.py --cli -t IP -p PORT --protocol udp --rate 4000 --threads 4 --processes 2
                                                   -> 멀티프로세스 모드 (고pps 구간)

v3.1 -> v3.2 주요 변경점:
 1) Windows 타이머 해상도 15.6ms -> 1ms (timeBeginPeriod): 기존에는 time.sleep(2ms)가
    실제로는 ~15.6ms씩 걸려 실제 전송률이 목표의 수 % 수준으로 떨어지던 핵심 원인 수정
 2) 하이브리드 페이서(sleep+spin) + 20ms 배치 윈도우: pps 정확도 대폭 개선
 3) 대상 주소 1회 사전 확인(DNS): 도메인 입력 시 매 패킷마다 DNS 조회되던
    치명적 성능 문제 수정
 4) HTTP: 크기 7KB 초과 시 헤더 대신 POST 본문 사용(서버의 대형 헤더 4xx 거절 방지),
    요청 파이프라이닝으로 처리량 향상
 5) TCP/HTTP: 응답 수신(drain) 추가 - 수신 버퍼 정체/연결 리셋 방지, EOF 감지 시 재접속
 6) UDP: 송신 버퍼 포화(WSAEWOULDBLOCK/WSAENOBUFS) 시 백오프
 7) CLI --processes 옵션(1~8): 멀티프로세스로 GIL 우회, 고pps 구간 확장
 8) 재접속 백오프 지터, SO_RCVBUF 확대, tkinter 임포트 가드 등 안정성 보강
"""

import argparse
import errno
import math
import os
import random
import socket
import sys
import threading
import time
import multiprocessing as mp

try:
    import tkinter as tk
    import tkinter.font as tkfont
    from tkinter import messagebox
except ImportError:  # GUI 미사용 환경(CLI 전용)
    tk = None
    tkfont = None

# ─────────────────────────────────────────────────────────────
# 공통 엔진
# ─────────────────────────────────────────────────────────────
stop_event = threading.Event()
stats_lock = threading.Lock()
stats = {"packets": 0, "bytes": 0, "errors": 0, "reconnects": 0}

MP_SINK = None  # 멀티프로세스 모드에서 부모가 읽는 공유 카운터
MP_STOP = None  # 멀티프로세스 모드 정지 이벤트

PACING_WINDOW = 0.02    # 페이싱 배치 윈도우(초). 20ms마다 한 배치 전송
PIPELINE_CAP = 262144   # 한 번에 합쳐 보낼 최대 바이트

_SOFT = set()
for _name in ("EWOULDBLOCK", "EAGAIN", "ENOBUFS", "EINTR", "EINPROGRESS"):
    _v = getattr(errno, _name, None)
    if _v is not None:
        _SOFT.add(_v)
SOFT_ERRNOS = frozenset(_SOFT | {10035, 10055})  # WSAEWOULDBLOCK, WSAENOBUFS


class _GlobalSink:
    __slots__ = ()

    def add(self, p, b, e, r):
        with stats_lock:
            stats["packets"] += p
            stats["bytes"] += b
            stats["errors"] += e
            stats["reconnects"] += r


class _SharedSink:
    """멀티프로세스 모드: 자식 프로세스의 카운터를 공유 메모리에 반영."""
    __slots__ = ("c", "lock")

    def __init__(self, counters, lock):
        self.c = counters
        self.lock = lock

    def add(self, p, b, e, r):
        with self.lock:
            c = self.c
            if p:
                c[0].value += p
            if b:
                c[1].value += b
            if e:
                c[2].value += e
            if r:
                c[3].value += r


GLOBAL_SINK = _GlobalSink()


def reset_stats():
    with stats_lock:
        for k in stats:
            stats[k] = 0


def snapshot():
    with stats_lock:
        out = dict(stats)
    if MP_SINK is not None:
        with MP_SINK.lock:
            c = MP_SINK.c
            out["packets"] += c[0].value
            out["bytes"] += c[1].value
            out["errors"] += c[2].value
            out["reconnects"] += c[3].value
    return out


def _flush(state, last_flush, sink, final=False):
    """로컬 카운터를 sink에 반영 (0.5초 간격 or 종료 시)."""
    now = time.perf_counter()
    if final or now - last_flush[0] >= 0.5:
        p, b, e, r = state
        if p or b or e or r:
            sink.add(p, b, e, r)
            state[0] = state[1] = state[2] = state[3] = 0
        last_flush[0] = now


class Pacer:
    """하이브리드 정밀 페이서.

    - 20ms 배치 윈도우로 묶어 한 번에 batch개 전송
    - 대기가 3ms 이상이면 sleep, 나머지 미세 구간은 spin으로 마감
      (Windows 기본 sleep 최소 단위 ~15.6ms 문제 회피)
    """

    __slots__ = ("rate", "batch", "interval", "next")

    def __init__(self, rate_per_thread):
        self.rate = float(rate_per_thread)
        if self.rate > 0:
            self.batch = max(1, int(round(self.rate * PACING_WINDOW)))
            self.interval = self.batch / self.rate
        else:
            self.batch = 50   # 무제한 모드: 반복당 50개
            self.interval = 0.0
        self.next = time.perf_counter()

    def wait_slot(self):
        if self.interval <= 0:
            return
        now = time.perf_counter()
        delay = self.next - now
        if delay > 0.003:
            time.sleep(delay - 0.002)
        if time.perf_counter() < self.next:
            while time.perf_counter() < self.next:
                time.sleep(0)
        now = time.perf_counter()
        if now - self.next >= self.interval * 2:
            # 한 슬롯 이상 늦어졌으면 재동기화 (버스트 급증 방지)
            self.next = now
        else:
            self.next += self.interval


def _drain_nb(sock, rounds=4):
    """소켓 수신 버퍼 비우기(논블로킹). 서버가 닫았으면(EOF) False 반환."""
    ok = True
    try:
        sock.setblocking(False)
        for _ in range(rounds):
            try:
                data = sock.recv(65536)
                if not data:
                    ok = False
                    break
            except BlockingIOError:
                break
            except OSError:
                ok = False
                break
    except OSError:
        ok = False
    finally:
        try:
            sock.setblocking(True)
        except OSError:
            pass
    return ok


def _connect(ip, port):
    sock = socket.create_connection((ip, port), timeout=3)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024 * 1024)
    except OSError:
        pass
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 256 * 1024)
    except OSError:
        pass
    return sock


def _jitter():
    """재접속 백오프 지터 0.1~0.3초."""
    return 0.1 + random.random() * 0.2


def udp_worker(ip, port, payload, rate_per_thread, stop_ev=None, sink=None):
    sink = sink or GLOBAL_SINK
    stop = stop_ev if stop_ev is not None else stop_event
    pacer = Pacer(rate_per_thread)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024 * 1024)
    except OSError:
        pass
    addr = (ip, port)
    send = sock.sendto
    payload_len = len(payload)
    state = [0, 0, 0, 0]
    last_flush = [time.perf_counter()]

    while not stop.is_set():
        batch = pacer.batch
        p_cnt = e_cnt = 0
        for _ in range(batch):
            try:
                send(payload, addr)
                p_cnt += 1
            except OSError as ex:
                e_cnt += 1
                if ex.errno in SOFT_ERRNOS:
                    # 송신 버퍼 포화 등: 잠깐 숨 돌리고 계속
                    time.sleep(0.001)

        state[0] += p_cnt
        state[1] += p_cnt * payload_len
        state[2] += e_cnt
        pacer.wait_slot()
        _flush(state, last_flush, sink)

    sock.close()
    _flush(state, last_flush, sink, final=True)


def tcp_worker(ip, port, payload, rate_per_thread, stop_ev=None, sink=None):
    sink = sink or GLOBAL_SINK
    stop = stop_ev if stop_ev is not None else stop_event
    pacer = Pacer(rate_per_thread)
    payload_len = len(payload)
    merged = max(1, min(pacer.batch if pacer.batch else 20, PIPELINE_CAP // max(1, payload_len)))
    payload_big = payload * merged if merged > 1 else payload
    state = [0, 0, 0, 0]
    last_flush = [time.perf_counter()]
    sock = None

    while not stop.is_set():
        if sock is None:
            try:
                sock = _connect(ip, port)
            except OSError:
                state[2] += 1
                time.sleep(_jitter())
                continue

        sent = 0
        while sent < pacer.batch and not stop.is_set():
            n = min(merged, pacer.batch - sent)
            buf = payload_big if n == merged else payload * n
            try:
                sock.sendall(buf)
                sent += n
            except OSError:
                try:
                    sock.close()
                except OSError:
                    pass
                sock = None
                state[3] += 1
                break

        state[0] += sent
        state[1] += sent * payload_len
        if sock is not None:
            # 응답을 읽어 수신 버퍼/서버 송신 윈도우가 막히지 않게 한다
            if not _drain_nb(sock):
                try:
                    sock.close()
                except OSError:
                    pass
                sock = None
                state[3] += 1
        pacer.wait_slot()
        _flush(state, last_flush, sink)

    if sock is not None:
        try:
            sock.close()
        except OSError:
            pass
    _flush(state, last_flush, sink, final=True)


def http_worker(ip, port, size, rate_per_thread, stop_ev=None, sink=None, host=None):
    sink = sink or GLOBAL_SINK
    stop = stop_ev if stop_ev is not None else stop_event
    host = host or ip  # Host 헤더는 원래 입력값(가상호스트) 사용, 접속은 IP로
    ts = int(time.time())

    if size <= 7000:
        # 크기가 작으면 기존처럼 X-Pad 헤더로 패딩 (전체 요청 길이 ≈ size)
        head = (
            f"GET /?_={ts} HTTP/1.1\r\n"
            f"Host: {host}\r\n"
            f"User-Agent: load-test/3.2\r\n"
            f"Accept: */*\r\n"
            f"Connection: keep-alive\r\n"
        )
        pad_len = max(0, size - len(head) - len("X-Pad: ") - 4)
        req = (head + "X-Pad: " + "X" * pad_len + "\r\n\r\n").encode("utf-8", "ignore")
    else:
        # 크기가 크면 헤더가 서버 한도(보통 8KB)를 넘어 400 거절되므로 본문으로 전달
        body = "X" * size
        req = (
            f"POST /?_={ts} HTTP/1.1\r\n"
            f"Host: {host}\r\n"
            f"User-Agent: load-test/3.2\r\n"
            f"Accept: */*\r\n"
            f"Content-Type: application/x-www-form-urlencoded\r\n"
            f"Content-Length: {size}\r\n"
            f"Connection: keep-alive\r\n\r\n{body}"
        ).encode("utf-8", "ignore")

    req_len = len(req)
    pipeline = max(1, min(8, PIPELINE_CAP // max(1, req_len)))
    req_big = req * pipeline if pipeline > 1 else req
    pacer = Pacer(rate_per_thread)
    state = [0, 0, 0, 0]
    last_flush = [time.perf_counter()]
    sock = None

    while not stop.is_set():
        if sock is None:
            try:
                sock = _connect(ip, port)
            except OSError:
                state[2] += 1
                time.sleep(_jitter())
                continue

        sent = 0
        while sent < pacer.batch and not stop.is_set():
            n = min(pipeline, pacer.batch - sent)
            buf = req_big if n == pipeline else req * n
            try:
                sock.sendall(buf)
                sent += n
            except OSError:
                try:
                    sock.close()
                except OSError:
                    pass
                sock = None
                state[3] += 1
                break

        state[0] += sent
        state[1] += sent * req_len
        if sock is not None and not _drain_nb(sock):
            try:
                sock.close()
            except OSError:
                pass
            sock = None
            state[3] += 1
        pacer.wait_slot()
        _flush(state, last_flush, sink)

    if sock is not None:
        try:
            sock.close()
        except OSError:
            pass
    _flush(state, last_flush, sink, final=True)


def resolve_target(target):
    """대상 주소를 시작 시 1회만 확인한다.

    (소켓 API에 도메인 문자열을 그대로 넘기면 sendto/connect 때마다
     getaddrinfo DNS 조회가 발생해 속도가 크게 떨어짐)
    """
    t = str(target).strip()
    try:
        infos = socket.getaddrinfo(t, None, socket.AF_INET, socket.SOCK_STREAM)
        if infos:
            return infos[0][4][0]
    except OSError:
        pass
    return t


_timer_boosted = False


def boost_timer_resolution():
    """Windows 타이머 해상도를 1ms로 올린다 (sleep 정밀도 개선)."""
    global _timer_boosted
    if os.name == "nt" and not _timer_boosted:
        try:
            import ctypes
            ctypes.windll.winmm.timeBeginPeriod(1)
            _timer_boosted = True
        except Exception:
            pass


def restore_timer_resolution():
    global _timer_boosted
    if os.name == "nt" and _timer_boosted:
        try:
            import ctypes
            ctypes.windll.winmm.timeEndPeriod(1)
        except Exception:
            pass


def stop_all():
    """스레드/프로세스 워커를 모두 정지시킨다."""
    stop_event.set()
    if MP_STOP is not None:
        MP_STOP.set()


def _make_thread(protocol, ip, port, payload, size, rpt, stop_ev, sink, host):
    if protocol == "udp":
        return threading.Thread(target=udp_worker, args=(ip, port, payload, rpt, stop_ev, sink), daemon=True)
    if protocol == "tcp":
        return threading.Thread(target=tcp_worker, args=(ip, port, payload, rpt, stop_ev, sink), daemon=True)
    return threading.Thread(target=http_worker, args=(ip, port, size, rpt, stop_ev, sink, host), daemon=True)


def spawn_workers(target, port, protocol, size, rate, threads, processes=1, resolved_ip=None):
    """워커 시작. 스레드(또는 프로세스) 객체 리스트를 반환.

    processes > 1 이면 멀티프로세스 모드: 자식 프로세스마다 threads개 스레드 실행,
    카운터는 공유 메모리(mp.Value)로 집계된다.
    """
    global MP_SINK, MP_STOP
    stop_event.clear()
    MP_SINK = None
    MP_STOP = None
    ip = resolved_ip or resolve_target(target)
    processes = max(1, int(processes))
    threads = max(1, int(threads))
    total = processes * threads
    rate_per_thread = rate / total if rate > 0 else 0.0
    payload = os.urandom(size) if protocol in ("udp", "tcp") else None

    if processes == 1:
        result = []
        for _ in range(threads):
            t = _make_thread(protocol, ip, port, payload, size, rate_per_thread, None, None, target)
            t.start()
            result.append(t)
        return result

    MP_STOP = mp.Event()
    counters = [mp.Value("Q", 0, lock=False) for _ in range(4)]
    lock = mp.Lock()
    MP_SINK = _SharedSink(counters, lock)
    procs = []
    for _ in range(processes):
        p = mp.Process(
            target=_proc_main,
            args=(ip, port, protocol, payload, size, rate_per_thread, threads, MP_STOP, counters, lock, target),
            daemon=True,
        )
        p.start()
        procs.append(p)
    return procs


def _proc_main(ip, port, protocol, payload, size, rate_per_thread, threads, stop_ev, counters, lock, host):
    """멀티프로세스 모드 자식 진입점 (Windows spawn 안전: 모듈 레벨 함수)."""
    sink = _SharedSink(counters, lock)
    ts = []
    for _ in range(threads):
        t = _make_thread(protocol, ip, port, payload, size, rate_per_thread, stop_ev, sink, host)
        t.start()
        ts.append(t)
    for t in ts:
        t.join()


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


def auto_threads(rate):
    """강도에 맞춘 적정 동시 작업 수."""
    return max(2, min(64, round(rate / 250)))

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
        self._label(ver, " v3.2 ", color=C_ACCENT, size=8, bold=True, mono=True).pack(padx=5, pady=1)

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

        ip = resolve_target(target)

        reset_stats()
        self.last_p = self.last_b = 0
        self.last_log = time.perf_counter()
        self.start_time = time.perf_counter()
        self.stopping = False
        self.workers = spawn_workers(target, port, protocol, size, rate, threads, processes=1, resolved_ip=ip)
        self.running = True

        est_mb = rate * size * self.duration / 1024 / 1024
        self.btn_start.set_enabled(False)
        self.btn_stop.set_enabled(True)
        self.status_pill.set(C_GREEN, "실행 중", pulse=True)
        self._set_foot("running", C_GREEN)
        self._log(f"▶ {target}:{port} · {mname} · {self.duration:.0f}초 · " f"초당 {rate}번 · 크기 {size}", "accent")
        self._log(f"  총 {threads}개 작업이 돌아가요 · 예상 전송량 약 {est_mb:.1f} MB", "info")
        self._log(f"  확인된 주소(IP): {ip}", "info")

        self._anim_loop()
        self.root.after(200, self._poll)

    def stop(self):
        if self.running and not self.stopping:
            stop_all()
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
        stop_all()
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
    boost_timer_resolution()
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
    ap.add_argument("--rate", type=float, default=100.0, help="초당 패킷 수, 전체 합계 (기본: 100)")
    ap.add_argument("--threads", type=int, default=2, help="동시 스레드 수 (기본: 2)")
    ap.add_argument("--processes", type=int, default=1, help="프로세스 수 1~8 (기본: 1. 2 이상이면 멀티프로세스로 GIL 우회)")
    ap.add_argument("--force", action="store_true", help="브로드캐스트/네트워크 주소 검사 건너뜀")
    args = ap.parse_args()

    if not (1 <= args.processes <= 8):
        print("오류: --processes는 1~8 범위로 입력하세요")
        sys.exit(1)

    err = validate_cfg(args.target, args.port, args.protocol, args.size, args.time, args.rate, args.threads, force=args.force)
    if err:
        print(f"오류: {err}")
        sys.exit(1)

    ip = resolve_target(args.target)
    shown = args.target if ip == args.target.strip() else f"{args.target} (→ {ip})"

    print(
        f"대상: {shown}:{args.port} | 프로토콜: {args.protocol.upper()} | "
        f"크기: {args.size}B | 시간: {args.time:.0f}초 | 속도: {args.rate:.0f}pps | "
        f"스레드: {args.threads} × 프로세스 {args.processes}"
    )
    print("중지: Ctrl+C\n")

    boost_timer_resolution()
    start = time.perf_counter()
    reset_stats()
    workers = spawn_workers(args.target, args.port, args.protocol, args.size, args.rate, args.threads, processes=args.processes, resolved_ip=ip)
    last_p = last_b = 0
    try:
        while time.perf_counter() - start < args.time:
            time.sleep(1.0)
            snap = snapshot()
            now_time = time.perf_counter() - start
            print(
                f"[{now_time:6.1f}초] 누적 {snap['packets']:>8} 패킷 | "
                f"{(snap['packets'] - last_p):>5} pps | "
                f"{(snap['bytes'] - last_b) / 1024:>9.1f} KB/s | "
                f"오류 {snap['errors']} / 재접속 {snap['reconnects']}"
            )
            last_p, last_b = snap["packets"], snap["bytes"]
    except KeyboardInterrupt:
        print("\n사용자 중지 요청")
    finally:
        stop_all()
        for w in workers:
            w.join(timeout=5)
        restore_timer_resolution()

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
