# mini_stress_pro

> 소규모 네트워크 부하(Load) 테스트 도구 · GUI + CLI

`ddos.py`는 Python으로 작성된 간단한 네트워크 부하 테스트 도구입니다.  
GUI 또는 CLI에서 테스트 대상, 프로토콜, 전송 크기, 실행 시간, 초당 전송량 등을 설정하고 실행 결과를 실시간으로 확인할 수 있습니다.

> ⚠️ **중요:** 반드시 본인이 소유하거나 명시적인 테스트 허가를 받은 시스템에서만 사용하세요.  
> 허가 없이 다른 서버·네트워크·서비스에 트래픽을 발생시키는 행위는 서비스 장애를 유발하거나 관련 법률/약관을 위반할 수 있습니다.

---

## Features

- 🖥️ **GUI 모드**
  - Tkinter 기반 다크 테마 인터페이스
  - 대상 주소 / 포트 설정
  - UDP / TCP / HTTP 방식 선택
  - 전송 속도 조절
  - 테스트 지속 시간 설정
  - 전송 데이터 크기 설정
  - 진행률 및 실시간 통계
  - 오류 / 재접속 횟수 표시
  - 테스트 로그 제공

- 💻 **CLI 모드**
  - 터미널에서 테스트 설정
  - 대상 / 포트 / 프로토콜 / 크기 / 시간 / 속도 / 스레드 설정
  - 실행 중 실시간 통계 출력
  - 종료 후 결과 요약

- 📊 **측정 정보**
  - 전송 횟수
  - 초당 전송 횟수(pps)
  - 전송 데이터량
  - 오류 수
  - TCP 재접속 수
  - 평균 전송 속도

---

## Supported Protocols

| Protocol | Description |
|---|---|
| UDP | 연결 없이 UDP 데이터를 전송하는 테스트 방식 |
| TCP | TCP 연결을 생성한 후 데이터를 전송하는 방식 |
| HTTP | HTTP GET 요청을 반복하여 웹 서버의 부하 테스트에 사용할 수 있는 방식 |

이 프로젝트는 네트워크 성능 및 부하 테스트를 위한 용도로 작성되었습니다.

---

## Requirements

- Python 3.x
- Tkinter (GUI 사용 시)

표준 라이브러리를 중심으로 작성되어 별도의 Python 패키지 설치 없이 실행할 수 있습니다.

---

## Run

### GUI

```bash
python ddos.py
```

기본 실행 방식은 GUI입니다.

### CLI

```bash
python ddos.py --cli
```

CLI에서는 다음 옵션을 사용할 수 있습니다.

```text
-t, --target       테스트 대상 IP / 호스트명
-p, --port         대상 포트
--protocol         udp / tcp / http
--size             전송 데이터 크기(bytes)
--time             테스트 시간(초)
--rate             초당 전송량
--threads          동시 작업 수
--force            브로드캐스트/네트워크 주소 검사 우회
```

예시:

```bash
python ddos.py --cli -t 192.168.0.10 -p 80 --protocol tcp --size 1024 --time 10 --rate 100 --threads 2
```

위 예시는 **자신이 관리하거나 테스트 승인을 받은 장비**를 대상으로 사용해야 합니다.

---

## GUI

실행하면 다음과 같은 항목을 설정할 수 있습니다.

```text
Target
  └─ IP / Host
  └─ Port

Method
  ├─ UDP
  ├─ TCP
  └─ HTTP

Load / Time
  ├─ Rate
  ├─ Duration
  └─ Payload Size

Statistics
  ├─ Packets
  ├─ PPS
  ├─ Bandwidth
  └─ Errors
```

실행 중에는 진행률과 로그가 실시간으로 갱신됩니다.

---

## Architecture

```text
ddos.py
│
├── Configuration
│   ├── target
│   ├── port
│   ├── protocol
│   ├── payload size
│   ├── rate
│   └── duration
│
├── Workers
│   ├── UDP worker
│   ├── TCP worker
│   └── HTTP worker
│
├── Statistics
│   ├── packets
│   ├── bytes
│   ├── errors
│   └── reconnects
│
└── Interface
    ├── Tkinter GUI
    └── CLI
```

각 테스트 방식은 별도의 worker thread에서 실행되며, 공유 통계 객체를 통해 전송량과 오류를 집계합니다.

---

## Safety

이 프로그램은 **부하 테스트 / 성능 측정 목적**으로 사용하는 것을 전제로 합니다.

### 허용되는 테스트 환경

- 본인이 소유한 서버
- 본인이 관리하는 로컬 네트워크
- 명시적인 테스트 허가를 받은 서버
- 별도의 실험용 VM / 테스트 환경

### 사용하면 안 되는 환경

- 허가받지 않은 타인의 서버
- 공용 서비스
- 학교 / 회사 / 기관 네트워크
- 인터넷상의 무관한 서버
- 서비스 장애를 발생시키는 목적의 대상

특히 테스트를 시작하기 전에 **대상 주소와 포트를 반드시 확인하세요.**

---

## Project Status

**Version:** `3.1`

현재 구현된 기능:

- [x] GUI
- [x] CLI
- [x] UDP 테스트
- [x] TCP 테스트
- [x] HTTP 테스트
- [x] Rate control
- [x] Multi-thread worker
- [x] 실시간 통계
- [x] 실행 로그
- [x] 진행률 표시
- [x] 오류 / 재접속 통계

---

## License


코드의 사용 여부와 관계없이 **대상 시스템의 소유권 또는 명시적인 테스트 권한을 먼저 확인하는 것을 원칙으로 합니다.**
