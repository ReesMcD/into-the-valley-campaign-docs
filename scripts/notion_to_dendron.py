#!/usr/bin/env python3
"""
Notion (markdown export) -> Dendron vault migration.

Designed for this repo layout:
  - notion_export/   (Notion export root)
  - notes/           (Dendron vault root)

This script:
  - maps Notion .md paths -> Dendron note IDs (dot hierarchy)
  - converts markdown:
      - adds YAML frontmatter
      - rewrites internal links to Dendron wikilinks [[id|alias]]
      - centralizes assets under notes/assets/images/notion/...
  - writes migration report + broken links ledger
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import os
import re
import shutil
import sys
import time
import unicodedata
import urllib.parse
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple


NOTION_ID_RE = re.compile(r"^(?P<title>.*)\s(?P<id>[0-9a-f]{32})$")

MD_EXT = ".md"
ASSET_EXTS = {".png", ".jpg", ".jpeg", ".webp", ".gif", ".pdf", ".svg", ".mp4", ".csv"}
IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".webp", ".gif", ".svg"}


def slugify(seg: str) -> str:
    seg = seg.strip()
    seg = seg.replace("&", " and ")
    seg = seg.replace("/", " ")
    seg = seg.replace("\\", " ")
    # normalize unicode quotes, accents, etc.
    seg = unicodedata.normalize("NFKD", seg)
    seg = "".join(ch for ch in seg if not unicodedata.combining(ch))
    seg = seg.lower()
    # replace apostrophes/quotes with nothing
    seg = seg.replace("’", "")
    seg = seg.replace("'", "")
    # keep alnum, spaces, hyphen
    seg = re.sub(r"[^a-z0-9\s\-]", " ", seg)
    seg = re.sub(r"\s+", " ", seg).strip()
    seg = seg.replace(" ", "-")
    seg = re.sub(r"-{2,}", "-", seg).strip("-")
    return seg or "untitled"


def split_notion_title_and_id(name: str) -> Tuple[str, Optional[str]]:
    """
    Given a file/folder base name (no extension), return (title, notionId?).
    """
    m = NOTION_ID_RE.match(name.strip())
    if not m:
        return name.strip(), None
    return m.group("title").strip(), m.group("id")


def epoch_ms_from_mtime(path: Path) -> int:
    try:
        return int(path.stat().st_mtime * 1000)
    except FileNotFoundError:
        return int(time.time() * 1000)


def yaml_quote(s: str) -> str:
    # minimal yaml escaping: use JSON encoding which is valid YAML for scalars
    return json.dumps(s, ensure_ascii=False)


@dataclasses.dataclass(frozen=True)
class NoteMapping:
    notion_rel_md: str  # path relative to notion_export, as posix
    note_id: str
    title: str
    notion_id: Optional[str]


@dataclasses.dataclass
class MigrationState:
    mappings_by_relpath: Dict[str, NoteMapping]
    mappings_by_note_id: Dict[str, NoteMapping]
    collisions: List[Tuple[str, List[str]]]
    broken_links: List[Dict[str, str]]
    copied_assets: List[Dict[str, str]]


def iter_notion_markdown_files(notion_root: Path) -> Iterable[Path]:
    for p in notion_root.rglob("*.md"):
        # ignore macOS metadata
        if p.name == ".DS_Store":
            continue
        yield p


def iter_notion_asset_files(notion_root: Path) -> Iterable[Path]:
    for p in notion_root.rglob("*"):
        if p.is_dir():
            continue
        if p.suffix.lower() in ASSET_EXTS:
            yield p


def rel_to_posix(root: Path, path: Path) -> str:
    return path.relative_to(root).as_posix()


def build_note_id_from_relpath(notion_rel_md: str) -> Tuple[str, str, Optional[str]]:
    """
    Given relpath like 'Realm of the Forsaken/The Realm/Items/Spellcraft Gems 3ec....md'
    returns (note_id, title, notion_id)
    """
    rel = Path(notion_rel_md)
    parts = list(rel.parts)
    assert parts[-1].endswith(MD_EXT)

    # folders -> segments
    segs: List[str] = []
    for folder in parts[:-1]:
        title, _nid = split_notion_title_and_id(folder)
        segs.append(slugify(title))

    file_base = rel.name[: -len(MD_EXT)]
    title, notion_id = split_notion_title_and_id(file_base)
    note_id = ".".join(segs + [slugify(title)])
    return note_id, title, notion_id


def build_mappings(notion_root: Path) -> MigrationState:
    by_relpath: Dict[str, NoteMapping] = {}
    id_to_relpaths: Dict[str, List[str]] = {}

    for md in iter_notion_markdown_files(notion_root):
        rel = rel_to_posix(notion_root, md)
        note_id, title, notion_id = build_note_id_from_relpath(rel)
        id_to_relpaths.setdefault(note_id, []).append(rel)
        by_relpath[rel] = NoteMapping(notion_rel_md=rel, note_id=note_id, title=title, notion_id=notion_id)

    collisions: List[Tuple[str, List[str]]] = []
    resolved_by_relpath: Dict[str, NoteMapping] = {}
    by_note_id: Dict[str, NoteMapping] = {}

    for note_id, relpaths in id_to_relpaths.items():
        if len(relpaths) == 1:
            m = by_relpath[relpaths[0]]
            resolved_by_relpath[m.notion_rel_md] = m
            by_note_id[m.note_id] = m
            continue

        collisions.append((note_id, sorted(relpaths)))
        # disambiguate by appending a short suffix derived from Notion ID (or hash of relpath)
        for rel in relpaths:
            m = by_relpath[rel]
            suffix = (m.notion_id or re.sub(r"[^a-z0-9]+", "", rel.lower()))[-6:]
            new_id = f"{note_id}-{suffix}"
            m2 = NoteMapping(notion_rel_md=m.notion_rel_md, note_id=new_id, title=m.title, notion_id=m.notion_id)
            resolved_by_relpath[m2.notion_rel_md] = m2
            by_note_id[m2.note_id] = m2

    return MigrationState(
        mappings_by_relpath=resolved_by_relpath,
        mappings_by_note_id=by_note_id,
        collisions=collisions,
        broken_links=[],
        copied_assets=[],
    )


LINK_RE = re.compile(r"(?P<bang>!)?\[(?P<label>[^\]]*)\]\((?P<target>[^)]+)\)")


def is_external_link(target: str) -> bool:
    t = target.strip()
    return t.startswith("http://") or t.startswith("https://") or t.startswith("mailto:")


def decode_notion_url_path(p: str) -> str:
    # Notion exports include %20 etc
    return urllib.parse.unquote(p)


def resolve_target_path(md_file: Path, raw_target: str) -> Optional[Path]:
    """
    Resolve a markdown link target (relative) to an absolute path on disk.
    Returns None if it's not a file-path-like target.
    """
    target = raw_target.strip()
    if is_external_link(target):
        return None

    # strip anchors
    target_no_anchor = target.split("#", 1)[0]
    target_no_anchor = decode_notion_url_path(target_no_anchor)

    # remove surrounding <...> if present
    if target_no_anchor.startswith("<") and target_no_anchor.endswith(">"):
        target_no_anchor = target_no_anchor[1:-1]

    # skip empty
    if not target_no_anchor:
        return None

    # treat as file path
    return (md_file.parent / target_no_anchor).resolve()


def target_asset_dest(notion_root: Path, notes_root: Path, asset_abs: Path) -> Path:
    rel = asset_abs.relative_to(notion_root)
    # mirror path under assets/images/notion/<rel_dir>/<filename>
    return notes_root / "assets" / "images" / "notion" / rel


def copy_asset(notion_root: Path, notes_root: Path, asset_abs: Path, state: MigrationState) -> str:
    dest = target_asset_dest(notion_root, notes_root, asset_abs)
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(asset_abs, dest)
    web_path = "/" + dest.relative_to(notes_root).as_posix()
    state.copied_assets.append(
        {
            "src": asset_abs.as_posix(),
            "dest": dest.as_posix(),
            "webPath": web_path,
        }
    )
    return web_path


META_LINE_RE = re.compile(r"^(?P<key>[A-Za-z][A-Za-z0-9 '\-&’]+):\s*(?P<value>.+)$")


def parse_metadata_block(lines: List[str]) -> Tuple[Dict[str, str], int]:
    """
    Parse consecutive 'Key: Value' lines immediately after the first title header.
    Returns (metadata, index_of_first_non_meta_line)
    """
    meta: Dict[str, str] = {}
    i = 0
    # skip leading blank lines
    while i < len(lines) and not lines[i].strip():
        i += 1

    # Expect first heading '# Title' (Notion export)
    if i < len(lines) and lines[i].lstrip().startswith("# "):
        i += 1
        # consume blank line after title if present
        if i < len(lines) and not lines[i].strip():
            i += 1
    else:
        # no standard title header -> no metadata block assumption
        return {}, 0

    # parse key/value lines until blank line
    while i < len(lines):
        line = lines[i].rstrip("\n")
        if not line.strip():
            i += 1
            break
        m = META_LINE_RE.match(line)
        if not m:
            break
        key = m.group("key").strip()
        value = m.group("value").strip()
        meta[key] = value
        i += 1

    return meta, i


def split_tags(raw: str) -> List[str]:
    parts = [p.strip() for p in raw.split(",")]
    parts = [p for p in parts if p]
    # slugify tags? Dendron supports hashtags; we'll keep original words but trim
    return parts


def parse_value_as_link(value: str) -> Optional[Tuple[str, str]]:
    """
    Handle patterns like 'Red Chaos (../../Organizations/Red%20Chaos ... .md)'
    Returns (label, target) or None.
    """
    # Avoid lossy parsing for comma-separated lists like:
    # 'A (a.md), B (b.md), C (c.md)'
    if "," in value:
        return None
    if value.count("(") != 1 or value.count(")") != 1:
        return None
    m = re.match(r"^(?P<label>.+?)\s*\((?P<target>[^)]+\.md)\)\s*$", value)
    if not m:
        return None
    return m.group("label").strip(), m.group("target").strip()


def rewrite_markdown(
    *,
    text: str,
    md_abs: Path,
    notion_root: Path,
    notes_root: Path,
    state: MigrationState,
    embed_image_web_path: Optional[str],
) -> str:
    lines = text.splitlines(keepends=True)
    meta, body_start = parse_metadata_block(lines)

    # title is always first '# ' line if present
    title = None
    for ln in lines:
        if ln.lstrip().startswith("# "):
            title = ln.lstrip()[2:].strip()
            break

    # Remove leading title line from body (we'll use frontmatter title)
    body_lines = lines[body_start:]
    body_text = "".join(body_lines)

    # Rewrite markdown links/images
    def repl(m: re.Match) -> str:
        bang = m.group("bang")
        label = m.group("label")
        target = m.group("target")

        if is_external_link(target):
            return m.group(0)

        resolved = resolve_target_path(md_abs, target)
        if resolved is None:
            return m.group(0)

        # If target is md, convert to wikilink
        if resolved.suffix.lower() == MD_EXT:
            try:
                rel = rel_to_posix(notion_root, resolved)
            except ValueError:
                # outside notion export
                return m.group(0)
            mapping = state.mappings_by_relpath.get(rel)
            if not mapping:
                state.broken_links.append(
                    {
                        "source": rel_to_posix(notion_root, md_abs),
                        "target": rel,
                        "label": label,
                        "reason": "missing_mapping_for_md",
                    }
                )
                return f"{label} (BROKEN_LINK: {target})"
            return f"[[{mapping.note_id}|{label}]]"

        # If target is an asset, copy and rewrite
        if resolved.suffix.lower() in ASSET_EXTS:
            if not resolved.exists():
                state.broken_links.append(
                    {
                        "source": rel_to_posix(notion_root, md_abs),
                        "target": resolved.as_posix(),
                        "label": label,
                        "reason": "missing_asset_file",
                    }
                )
                return f"{label} (MISSING_ASSET: {target})"
            web_path = copy_asset(notion_root, notes_root, resolved, state)
            if bang:
                return f"![{label}]({web_path})"
            return f"[{label}]({web_path})"

        return m.group(0)

    body_text = LINK_RE.sub(repl, body_text)

    # Construct frontmatter
    # Find mapping for this note
    rel = rel_to_posix(notion_root, md_abs)
    mapping = state.mappings_by_relpath[rel]

    created = epoch_ms_from_mtime(md_abs)
    updated = created

    fm_lines: List[str] = []
    fm_lines.append("---\n")
    fm_lines.append(f"id: {mapping.note_id}\n")
    fm_lines.append(f"title: {yaml_quote(mapping.title)}\n")
    fm_lines.append("desc: ''\n")
    fm_lines.append(f"updated: {updated}\n")
    fm_lines.append(f"created: {created}\n")
    if mapping.notion_id:
        fm_lines.append(f"notionId: {yaml_quote(mapping.notion_id)}\n")

    # Tags
    raw_tags = meta.get("Tags")
    if raw_tags:
        tags = split_tags(raw_tags)
        if tags:
            fm_lines.append("tags:\n")
            for t in tags:
                fm_lines.append(f"  - {yaml_quote(t)}\n")

    # Other metadata (excluding Image and Tags)
    for k, v in meta.items():
        if k in {"Tags", "Image"}:
            continue
        as_link = parse_value_as_link(v)
        if as_link:
            _lbl, tgt = as_link
            resolved = resolve_target_path(md_abs, tgt)
            if resolved and resolved.suffix.lower() == MD_EXT and resolved.exists():
                try:
                    rel_tgt = rel_to_posix(notion_root, resolved)
                    mapping_tgt = state.mappings_by_relpath.get(rel_tgt)
                except ValueError:
                    mapping_tgt = None
                if mapping_tgt:
                    fm_lines.append(f"{slugify(k).replace('-', '')}: {yaml_quote(mapping_tgt.note_id)}\n")
                    continue
        # fallback raw value
        fm_lines.append(f"{slugify(k).replace('-', '')}: {yaml_quote(v)}\n")

    fm_lines.append("---\n")

    out_parts: List[str] = []
    out_parts.append("".join(fm_lines))

    if embed_image_web_path:
        out_parts.append(f"\n![]({embed_image_web_path})\n")

    # If original content started with a top-level heading, we already removed it;
    # keep the rest as-is (post metadata), with rewritten links.
    # Ensure a leading newline before the first content block for readability.
    if body_text and not body_text.startswith("\n"):
        out_parts.append("\n")
    out_parts.append(body_text.rstrip() + "\n")

    return "".join(out_parts)


def convert_all_notes(notion_root: Path, notes_root: Path, state: MigrationState) -> Dict[str, str]:
    """
    Returns a dict of note_id -> output markdown text (not written yet).
    """
    out: Dict[str, str] = {}

    for md_abs in iter_notion_markdown_files(notion_root):
        rel = rel_to_posix(notion_root, md_abs)
        mapping = state.mappings_by_relpath[rel]
        text = md_abs.read_text(encoding="utf-8", errors="replace")

        # Handle Notion 'Image: <relative>' metadata specially: convert to embedded image at top
        lines = text.splitlines(keepends=True)
        meta, _body_start = parse_metadata_block(lines)
        embed_image_web_path = None
        if "Image" in meta:
            raw_image = meta["Image"].strip()
            # treat as a relative file path
            img_abs = resolve_target_path(md_abs, raw_image)
            if img_abs and img_abs.suffix.lower() in ASSET_EXTS:
                if img_abs.exists():
                    embed_image_web_path = copy_asset(notion_root, notes_root, img_abs, state)
                else:
                    state.broken_links.append(
                        {
                            "source": rel,
                            "target": decode_notion_url_path(raw_image),
                            "label": "Image",
                            "reason": "missing_image_file",
                        }
                    )

        out_text = rewrite_markdown(
            text=text,
            md_abs=md_abs,
            notion_root=notion_root,
            notes_root=notes_root,
            state=state,
            embed_image_web_path=embed_image_web_path,
        )
        out[mapping.note_id] = out_text

    return out


def ensure_index_notes_for_folders(notion_root: Path, state: MigrationState, notes_by_id: Dict[str, str]) -> None:
    """
    Ensure every folder path implies a hierarchy note (index).
    If a folder doesn't have a corresponding Notion md, create a minimal index note.
    """
    # Build set of existing note ids
    existing = set(notes_by_id.keys())

    # Build folder-derived note ids by walking directories
    for d in notion_root.rglob("*"):
        if not d.is_dir():
            continue
        rel = rel_to_posix(notion_root, d)
        if rel == ".":
            continue
        parts = [slugify(split_notion_title_and_id(p)[0]) for p in Path(rel).parts]
        folder_note_id = ".".join(parts)
        if folder_note_id in existing:
            continue
        # Create minimal index note
        created = epoch_ms_from_mtime(d)
        title = split_notion_title_and_id(d.name)[0].strip()
        fm = (
            "---\n"
            f"id: {folder_note_id}\n"
            f"title: {yaml_quote(title)}\n"
            "desc: ''\n"
            f"updated: {created}\n"
            f"created: {created}\n"
            "---\n"
        )
        notes_by_id[folder_note_id] = fm + "\n## Contents\n\n"


def write_notes(notes_root: Path, notes_by_id: Dict[str, str]) -> None:
    for note_id, content in notes_by_id.items():
        dest = notes_root / f"{note_id}.md"
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(content, encoding="utf-8")


def build_contents_indexes(notes_by_id: Dict[str, str]) -> None:
    """
    For notes that are hierarchy parents, add/replace a '## Contents' section listing direct children.
    Only for notes that were created as minimal indices or already have a Contents heading.
    """
    all_ids = sorted(notes_by_id.keys())
    children_by_parent: Dict[str, List[str]] = {}
    for nid in all_ids:
        if "." in nid:
            parent = nid.rsplit(".", 1)[0]
            children_by_parent.setdefault(parent, []).append(nid)

    for parent, children in children_by_parent.items():
        text = notes_by_id.get(parent)
        if not text:
            continue
        if "## Contents" not in text:
            continue

        # Replace everything after '## Contents' with generated list (keep anything before)
        before, _, after = text.partition("## Contents")
        # Preserve the heading itself
        new = before + "## Contents\n\n"
        for c in children:
            alias = c.split(".")[-1].replace("-", " ").title()
            new += f"- [[{c}|{alias}]]\n"
        new += "\n"
        notes_by_id[parent] = new


def write_migration_reports(notes_root: Path, state: MigrationState, notes_by_id: Dict[str, str]) -> None:
    report_lines: List[str] = []
    report_lines.append("---\n")
    report_lines.append("id: migration.report\n")
    report_lines.append('title: "Migration Report"\n')
    report_lines.append("desc: ''\n")
    now = int(time.time() * 1000)
    report_lines.append(f"updated: {now}\n")
    report_lines.append(f"created: {now}\n")
    report_lines.append("---\n\n")
    report_lines.append("## Summary\n\n")
    report_lines.append(f"- Notes written: {len(notes_by_id)}\n")
    report_lines.append(f"- Assets copied: {len(state.copied_assets)}\n")
    report_lines.append(f"- Broken references: {len(state.broken_links)}\n")
    report_lines.append(f"- Note ID collisions resolved: {len(state.collisions)}\n")
    report_lines.append("\n")

    if state.collisions:
        report_lines.append("## Collisions\n\n")
        for note_id, relpaths in state.collisions:
            report_lines.append(f"- `{note_id}`:\n")
            for rp in relpaths:
                report_lines.append(f"  - `{rp}`\n")
        report_lines.append("\n")

    (notes_root / "migration.report.md").write_text("".join(report_lines), encoding="utf-8")

    broken_lines: List[str] = []
    broken_lines.append("---\n")
    broken_lines.append("id: migration.broken-links\n")
    broken_lines.append('title: "Migration Broken Links"\n')
    broken_lines.append("desc: ''\n")
    broken_lines.append(f"updated: {now}\n")
    broken_lines.append(f"created: {now}\n")
    broken_lines.append("---\n\n")
    broken_lines.append("## Broken references\n\n")
    if not state.broken_links:
        broken_lines.append("None detected.\n")
    else:
        for b in state.broken_links:
            broken_lines.append(
                f"- Source: `{b.get('source','')}` | Label: {yaml_quote(b.get('label',''))} | "
                f"Target: `{b.get('target','')}` | Reason: `{b.get('reason','')}`\n"
            )
    broken_lines.append("\n")
    (notes_root / "migration.broken-links.md").write_text("".join(broken_lines), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", required=True, help="Absolute path to repo root")
    ap.add_argument("--notion-dir", default="notion_export", help="Notion export directory (relative to repo)")
    ap.add_argument("--notes-dir", default="notes", help="Dendron vault directory (relative to repo)")
    ap.add_argument("--clean-prefix", default="realm-of-the-forsaken", help="Delete existing notes with this prefix before writing")
    args = ap.parse_args()

    repo = Path(args.repo).resolve()
    notion_root = (repo / args.notion_dir).resolve()
    notes_root = (repo / args.notes_dir).resolve()

    if not notion_root.exists():
        print(f"Notion export dir not found: {notion_root}", file=sys.stderr)
        return 2
    if not notes_root.exists():
        print(f"Notes dir not found: {notes_root}", file=sys.stderr)
        return 2

    # Clean old generated notes (safety: only matching prefix, not root.md)
    prefix = args.clean_prefix
    for p in notes_root.glob(f"{prefix}*.md"):
        p.unlink()

    # Ensure assets base exists
    (notes_root / "assets" / "images" / "notion").mkdir(parents=True, exist_ok=True)

    state = build_mappings(notion_root)

    notes_by_id = convert_all_notes(notion_root, notes_root, state)
    ensure_index_notes_for_folders(notion_root, state, notes_by_id)
    build_contents_indexes(notes_by_id)
    write_notes(notes_root, notes_by_id)
    write_migration_reports(notes_root, state, notes_by_id)

    print(f"Wrote {len(notes_by_id)} notes into {notes_root}")
    print(f"Copied {len(state.copied_assets)} assets into {notes_root/'assets/images/notion'}")
    print(f"Broken references: {len(state.broken_links)}")
    print(f"Collisions resolved: {len(state.collisions)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

