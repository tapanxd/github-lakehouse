#!/usr/bin/env python3
"""
Blocks only production-mode permission assignments declared in the Databricks bundle.

Updated policy:
- Allow permissions in development targets.
- Block permissions only when they are defined under targets whose `mode` is `production` in the top-level
    `databricks.yml` (e.g., `targets.<name with production mode>.permissions`).

Notes:
- This script now ONLY inspects `databricks.yml` and ignores files under `resources/` to avoid false positives
    when developing locally. If you need to enforce policy on resource files for specific environments, wire this
    check into environment-aware CI that runs only for production deployments.

On any violation, the script prints a clear error and exits with a non-zero code.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any, List, Tuple

try:
    import yaml  # type: ignore
except Exception as exc:  # pragma: no cover
    print("ERROR: PyYAML is required. Please install with `pip install pyyaml`.\nDetails:", exc)
    sys.exit(2)


REPO_ROOT = Path(__file__).resolve().parent.parent


def is_bundle_yaml(path: Path) -> bool:
    if not path.suffix.lower() in {".yml", ".yaml"}:
        return False
    # Only enforce for DAB bundle files
    rel = path.relative_to(REPO_ROOT)
    return rel.as_posix() == "databricks.yml" or rel.as_posix().startswith("resources/")


def iter_yaml_files(root: Path) -> List[Path]:
    files: List[Path] = []
    for p in root.rglob("*.yml"):
        if ".github/" in p.as_posix():
            continue
        if is_bundle_yaml(p):
            files.append(p)
    for p in root.rglob("*.yaml"):
        if ".github/" in p.as_posix():
            continue
        if is_bundle_yaml(p):
            files.append(p)
    # De-duplicate
    files = sorted({p.resolve() for p in files})
    return files


def find_permissions_nodes(data: Any, path: List[str] | None = None) -> List[Tuple[str, Any]]:
    path = path or []
    violations: List[Tuple[str, Any]] = []
    if isinstance(data, dict):
        for k, v in data.items():
            key_str = str(k)
            new_path = path + [key_str]
            if key_str == "permissions":
                violations.append((".".join(new_path), v))
            violations.extend(find_permissions_nodes(v, new_path))
    elif isinstance(data, list):
        for idx, item in enumerate(data):
            violations.extend(find_permissions_nodes(item, path + [f"[{idx}]"]))
    return violations


def main() -> int:
    root = REPO_ROOT
    files = iter_yaml_files(root)
    all_violations: List[Tuple[Path, str, Any]] = []
    for f in files:
        try:
            txt = f.read_text(encoding="utf-8")
        except Exception as e:
            print(f"WARNING: Skipping unreadable file {f}: {e}")
            continue

        # Support single-doc; if multi-doc, scan each
        try:
            docs = list(yaml.safe_load_all(txt))
        except yaml.YAMLError as e:
            print(f"WARNING: Skipping unparsable YAML {f}: {e}")
            continue

        for doc in docs:
            if doc is None:
                continue
            # Only consider permissions defined beneath production-mode targets
            if not isinstance(doc, dict):
                continue
            targets = doc.get("targets", {}) if isinstance(doc.get("targets", {}), dict) else {}
            for target_name, target_cfg in targets.items():
                if not isinstance(target_cfg, dict):
                    continue
                mode = str(target_cfg.get("mode", "")).strip().lower()
                if mode != "production":
                    continue  # allow permissions in non-production targets
                # Find any permissions anywhere under this production target
                violations = find_permissions_nodes(target_cfg, path=["targets", str(target_name)])
                for path_str, value in violations:
                    all_violations.append((f, path_str, value))

    if all_violations:
        print("\n❌ Policy violation: 'permissions' blocks are not allowed under production-mode targets in databricks.yml.\n")
        for f, path_str, value in all_violations:
            print(f"- File: {f.relative_to(root)}")
            print(f"  Path: {path_str}")
            try:
                preview = yaml.safe_dump(value, sort_keys=False).strip()
                if preview:
                    for line in preview.splitlines():
                        print(f"    {line}")
            except Exception:
                pass
            print()
        print("Please remove these 'permissions' sections from production targets as they are managed outside of bundles by terraform.")
        return 1

    print("✅ No 'permissions' blocks found in Databricks bundle YAML files.")
    return 0


if __name__ == "__main__":
    sys.exit(main())