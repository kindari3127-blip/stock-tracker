"""Claude Code CLI 래퍼 — Max 플랜 전용. Anthropic SDK 폴백 없음.

설계:
- `claude -p --output-format json --model <m>` 비대화형 호출
- system 은 --system-prompt-file 로 진짜 system 으로 전달 (코딩 보조 디폴트 영향 최소)
- user_prompt 만 stdin (Windows commandline 32KB 한도 회피)
- 한도 초과 키워드 감지 시 ClaudeCLIQuotaExceeded
- 그 외 실패는 ClaudeCLIError
- 호출자는 두 예외를 잡아 작업 중단 (SDK 폴백 안 함, Max 플랜 한도 회복 대기)

용도:
    from _claude_cli import call_claude_cli, ClaudeCLIError
    try:
        text = call_claude_cli(user_msg, system=sys_msg, model="claude-haiku-4-5")
    except ClaudeCLIError:
        # Max 플랜 전용 — 작업 중단
        raise

비용: 메모리 규칙 준수 — Anthropic 호출은 _claude_cli.py + Max 플랜만, SDK 직접 호출 금지.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
from typing import Optional


_CLAUDE_PATH: Optional[str] = None
_CLAUDE_PATH_RESOLVED = False


def _resolve_claude() -> Optional[str]:
    """claude CLI 실행 파일 절대 경로. Windows 의 .cmd/.bat 도 자동 탐색."""
    global _CLAUDE_PATH, _CLAUDE_PATH_RESOLVED
    if not _CLAUDE_PATH_RESOLVED:
        _CLAUDE_PATH = shutil.which("claude")
        _CLAUDE_PATH_RESOLVED = True
    return _CLAUDE_PATH


class ClaudeCLIError(Exception):
    """CLI 호출 실패 (네트워크·파싱·미설치 등)."""


class ClaudeCLIQuotaExceeded(ClaudeCLIError):
    """5시간 한도 또는 플랜 한도 초과."""


_QUOTA_KEYWORDS = (
    "rate limit",
    "rate-limit",
    "quota",
    "usage limit",
    "5-hour",
    "5 hour",
    "session limit",
    "plan limit",
    "subscription limit",
    "limit reached",
    "사용 한도",
    "한도 초과",
)


def call_claude_cli(
    user_prompt: str,
    *,
    system: Optional[str] = None,
    model: str = "claude-sonnet-4-6",
    timeout: int = 1200,
    extra_args: Optional[list[str]] = None,
) -> str:
    """Claude Code CLI 한 번 호출 → 응답 텍스트.

    Raises:
        ClaudeCLIQuotaExceeded: 5시간/플랜 한도 초과.
        ClaudeCLIError: 그 외 실패 (미설치, 타임아웃, 파싱 실패 등).
    """
    claude_exe = _resolve_claude()
    if not claude_exe:
        raise ClaudeCLIError("claude CLI 미설치 또는 PATH 없음")

    cmd = [
        claude_exe,
        "-p",
        "--output-format", "json",
        "--model", model,
        "--no-session-persistence",
    ]
    if extra_args:
        cmd.extend(extra_args)

    env = os.environ.copy()
    # CLI 가 ANTHROPIC_API_KEY 를 발견하면 그쪽으로 빠질 수 있어 격리
    env.pop("ANTHROPIC_API_KEY", None)

    import tempfile
    tmp_in_fd, tmp_in_path = tempfile.mkstemp(suffix='.txt', prefix='claude_in_')
    tmp_out_fd, tmp_out_path = tempfile.mkstemp(suffix='.json', prefix='claude_out_')
    os.close(tmp_out_fd)

    tmp_sys_path: Optional[str] = None
    if system:
        tmp_sys_fd, tmp_sys_path = tempfile.mkstemp(suffix='.txt', prefix='claude_sys_')
        with os.fdopen(tmp_sys_fd, 'w', encoding='utf-8', newline='') as f:
            f.write(system)
        cmd.extend(["--system-prompt-file", tmp_sys_path])

    stderr_text = ""
    try:
        with os.fdopen(tmp_in_fd, 'w', encoding='utf-8', newline='') as f:
            f.write(user_prompt)
        with open(tmp_in_path, 'rb') as fin, open(tmp_out_path, 'wb') as fout:
            result = subprocess.run(
                cmd,
                stdin=fin,
                stdout=fout,
                stderr=subprocess.PIPE,
                text=False,
                timeout=timeout,
                env=env,
            )
        stderr_text = (result.stderr or b"").decode('utf-8', errors='replace')
        with open(tmp_out_path, 'r', encoding='utf-8', errors='replace') as fout:
            stdout = fout.read().strip()
    except subprocess.TimeoutExpired as e:
        raise ClaudeCLIError(f"CLI 타임아웃 ({timeout}s)") from e
    except FileNotFoundError as e:
        raise ClaudeCLIError("claude CLI 미설치 또는 PATH 없음") from e
    finally:
        for p in (tmp_in_path, tmp_out_path, tmp_sys_path):
            if p:
                try:
                    os.unlink(p)
                except Exception:
                    pass

    if result.returncode != 0:
        stderr_lower = stderr_text.lower()
        if any(kw in stderr_lower for kw in _QUOTA_KEYWORDS):
            raise ClaudeCLIQuotaExceeded(stderr_text[:500])
        raise ClaudeCLIError(
            f"CLI 실패 (rc={result.returncode}): {stderr_text[:500]}"
        )

    if not stdout:
        raise ClaudeCLIError("CLI 응답이 비어 있음")

    # stdout 디버그 백업 — 프로젝트 루트의 tmp/
    _ts = ""
    try:
        from datetime import datetime as _dt
        _debug_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'tmp')
        os.makedirs(_debug_dir, exist_ok=True)
        _ts = _dt.now().strftime('%y%m%d_%H%M%S')
        with open(os.path.join(_debug_dir, f'last_cli_stdout_{_ts}.json'),
                  'w', encoding='utf-8') as _df:
            _df.write(stdout)
    except Exception:
        pass

    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError as e:
        first_brace = stdout.find('{')
        if first_brace >= 0:
            try:
                _decoder = json.JSONDecoder()
                payload, _ = _decoder.raw_decode(stdout[first_brace:])
            except json.JSONDecodeError:
                raise ClaudeCLIError(
                    f"CLI JSON 파싱 실패: {e} | "
                    f"응답 머리: {stdout[:300]} | 끝: ...{stdout[-200:]} | "
                    f"디버그: tmp/last_cli_stdout_{_ts}.json"
                ) from e
        else:
            raise ClaudeCLIError(
                f"CLI 응답에 JSON 객체 없음: {e} | "
                f"응답 머리: {stdout[:300]} | 끝: ...{stdout[-200:]}"
            ) from e

    text = payload.get("result")
    if not text:
        msgs = payload.get("messages") or payload.get("message")
        if msgs:
            try:
                last = msgs[-1] if isinstance(msgs, list) else msgs
                content = last.get("content") if isinstance(last, dict) else None
                if isinstance(content, list):
                    parts = [c.get("text", "") for c in content if c.get("type") == "text"]
                    text = "\n".join(parts).strip()
                elif isinstance(content, str):
                    text = content.strip()
            except Exception:
                pass
        if text:
            return text
        if payload.get("is_error"):
            err_msg = str(payload.get("error") or payload)
            if any(kw in err_msg.lower() for kw in _QUOTA_KEYWORDS):
                raise ClaudeCLIQuotaExceeded(err_msg[:500])
            raise ClaudeCLIError(f"CLI 응답 오류: {err_msg[:500]}")
        raise ClaudeCLIError(
            f"CLI result 필드 없음 (응답 키: {list(payload.keys())[:10]}): {stdout[:300]}"
        )

    return text


def extract_json_object(text: str) -> dict:
    """LLM 응답 텍스트에서 첫 valid JSON 객체 추출.

    코드블록(```), 머리말, 꼬리말이 섞여 있어도 첫 { 부터 매칭되는 } 까지
    raw_decode 로 안전 추출. 실패 시 마지막 } 까지 잘라 재시도.
    """
    if not text:
        raise ValueError("빈 응답")
    start = text.find("{")
    if start < 0:
        raise ValueError(f"JSON 객체 없음: {text[:200]}")
    decoder = json.JSONDecoder()
    try:
        obj, _ = decoder.raw_decode(text[start:])
        return obj
    except json.JSONDecodeError:
        end = text.rfind("}")
        if end > start:
            return json.loads(text[start:end + 1])
        raise


def is_cli_available() -> bool:
    claude_exe = _resolve_claude()
    if not claude_exe:
        return False
    try:
        result = subprocess.run(
            [claude_exe, "--version"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False
