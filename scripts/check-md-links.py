#!/usr/bin/env python3
"""Check relative links and #anchors in every tracked Markdown file.

Usage: scripts/check-md-links.py   (run from anywhere; exits 1 on broken links)
"""
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(subprocess.check_output(["git", "rev-parse", "--show-toplevel"], text=True).strip())
files = subprocess.check_output(["git", "ls-files", "-z", "--", "*.md"], cwd=ROOT).decode().split("\0")
files = [f for f in files if f and (ROOT / f).exists()]

LINK = re.compile(r"(?<!\!)\[[^\]]*\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)|!\[[^\]]*\]\(([^)\s]+)\)")
REFDEF = re.compile(r"^\s*\[[^\]]+\]:\s*(\S+)", re.M)


def slug(h: str) -> str:
    h = re.sub(r"`", "", h.strip().lower())
    h = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", h)
    h = re.sub(r"[^\w\- ]", "", h)
    return h.replace(" ", "-")


def anchors(path: Path) -> set:
    out, seen, fence = set(), {}, False
    for line in path.read_text(errors="replace").splitlines():
        if line.lstrip().startswith("```"):
            fence = not fence
        if fence:
            continue
        m = re.match(r"#{1,6}\s+(.*?)\s*#*$", line)
        if m:
            s = slug(m.group(1))
            n = seen.get(s, 0)
            seen[s] = n + 1
            out.add(s if n == 0 else f"{s}-{n}")
    for m in re.finditer(r'<a\s+(?:name|id)="([^"]+)"', path.read_text(errors="replace")):
        out.add(m.group(1))
    return out


cache, bad, total = {}, [], 0
for f in files:
    text = (ROOT / f).read_text(errors="replace")
    text = re.sub(r"```.*?```", "", text, flags=re.S)
    text = re.sub(r"`[^`\n]*`", "", text)
    targets = [a or b for a, b in LINK.findall(text)] + REFDEF.findall(text)
    targets += re.findall(r'(?:href|src|srcset)="([^"]+)"', text)
    for t in targets:
        if re.match(r"^([a-z][a-z0-9+.-]*:|//)", t, re.I):
            continue
        total += 1
        target, _, frag = t.partition("#")
        dest = (ROOT / f).parent / target if target else ROOT / f
        dest = Path(str(dest.resolve()))
        try:
            dest.relative_to(ROOT)
        except ValueError:
            bad.append((f, t, "escapes repo"))
            continue
        if not dest.exists():
            bad.append((f, t, "missing file"))
            continue
        if frag and dest.suffix == ".md" and dest.is_file():
            a = cache.setdefault(dest, anchors(dest))
            if frag.lower() not in a:
                bad.append((f, t, "missing anchor"))
for f, t, why in bad:
    print(f"{f}: {t} ({why})")
print(f"checked {total} relative links in {len(files)} files, {len(bad)} broken")
sys.exit(1 if bad else 0)
