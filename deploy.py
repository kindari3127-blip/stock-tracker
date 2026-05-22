# -*- coding: utf-8 -*-
"""
gh-pages 자동 배포 (orphan + force-push으로 history 1개 유지).
- .deploy/ 별도 worktree에 빌드 결과만 복사
- commit --amend + push -f → 항상 1개 commit
- 매번 호출해도 history 안 늘어남
"""
import os
import shutil
import stat
import subprocess
import sys
from datetime import datetime
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

HERE = Path(__file__).parent
# OneDrive 동기화 폴더 안에 두면 Files On-Demand reparse point / ReadOnly 잠금으로
# shutil.rmtree 가 PermissionError(WinError 5) 를 던져 매번 배포가 실패한다.
# → 홈 디렉터리의 OneDrive 밖 경로로 분리.
DEPLOY = Path.home() / ".stock-tracker-deploy"
REPO = "https://github.com/kindari3127-blip/stock-tracker.git"
BRANCH = "gh-pages"

STATIC_FILES = ["report.html", "explore.html", "manifest.json", "sw.js", "icon.svg"]


def _run(args, cwd, check=True):
    return subprocess.run(args, cwd=cwd, check=check, capture_output=True, text=True, encoding="utf-8")


def copy_files():
    """빌드 결과 + 정적 파일 + data/*.json → .deploy/ 그리고 .deploy/v2/."""
    ts = datetime.now().strftime("%Y%m%d%H%M")
    v2 = DEPLOY / "v2"
    v2.mkdir(exist_ok=True)

    for f in STATIC_FILES:
        src = HERE / f
        if not src.exists():
            continue
        if f == "sw.js":
            content = src.read_text(encoding="utf-8")
            content = content.replace('"20260429"', f'"{ts}"')
            (DEPLOY / f).write_text(content, encoding="utf-8")
            (v2 / f).write_text(content, encoding="utf-8")
        else:
            shutil.copy(src, DEPLOY / f)
            shutil.copy(src, v2 / f)

    src_data = HERE / "data"
    for target in (DEPLOY / "data", v2 / "data"):
        target.mkdir(exist_ok=True)
        if src_data.exists():
            for f in src_data.iterdir():
                if f.is_file() and f.suffix == ".json":
                    shutil.copy(f, target / f.name)

    (DEPLOY / ".nojekyll").touch()
    (v2 / ".nojekyll").touch()


def _rm_handler(func, path, exc_info):
    try:
        os.chmod(path, stat.S_IWRITE)
        func(path)
    except Exception:
        pass


def clean_deploy():
    """기존 파일 정리 (.git 제외)."""
    if not DEPLOY.exists():
        return
    for f in DEPLOY.iterdir():
        if f.name == ".git":
            continue
        if f.is_dir():
            shutil.rmtree(f, onerror=_rm_handler)
        else:
            try:
                f.unlink()
            except PermissionError:
                os.chmod(f, stat.S_IWRITE)
                f.unlink()


def _propagate_user_config():
    email = _run(["git", "config", "user.email"], cwd=HERE).stdout.strip()
    name = _run(["git", "config", "user.name"], cwd=HERE).stdout.strip()
    if email:
        _run(["git", "config", "user.email", email], cwd=DEPLOY)
    if name:
        _run(["git", "config", "user.name", name], cwd=DEPLOY)


def init_deploy():
    """최초 1회: .deploy 초기화 + orphan gh-pages 생성 + force push."""
    if DEPLOY.exists():
        shutil.rmtree(DEPLOY)
    DEPLOY.mkdir()
    _run(["git", "init", "-b", BRANCH], cwd=DEPLOY)
    _propagate_user_config()
    _run(["git", "remote", "add", "origin", REPO], cwd=DEPLOY)
    copy_files()
    _run(["git", "add", "-A"], cwd=DEPLOY)
    _run(["git", "commit", "-m", "init gh-pages"], cwd=DEPLOY)
    _run(["git", "push", "-f", "origin", BRANCH], cwd=DEPLOY)
    print(f"초기화 완료: {BRANCH} 브랜치 생성·푸시")


def deploy():
    """매번: 파일 갱신 → commit --amend → force push (history 1개 유지)."""
    if not (DEPLOY / ".git").exists():
        init_deploy()
        return

    clean_deploy()
    copy_files()

    _run(["git", "add", "-A"], cwd=DEPLOY)
    status = _run(["git", "status", "--porcelain"], cwd=DEPLOY).stdout.strip()
    if not status:
        print("변경 없음 — 푸시 생략")
        return

    msg = f"deploy {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    _run(["git", "commit", "--amend", "-m", msg], cwd=DEPLOY)
    _run(["git", "push", "-f", "origin", BRANCH], cwd=DEPLOY)
    print(f"푸시 완료: {msg}")


if __name__ == "__main__":
    deploy()
