#!/usr/bin/env python3
"""Generate scip_pb2 bindings under build/scip from a discovered scip.proto."""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
from pathlib import Path
from urllib.request import urlopen

DEFAULT_BUILD_SUBDIR = Path("build") / "scip"
DEFAULT_SCIP_BIN = "scip"
SCIP_PROTO_FILENAME = "scip.proto"


def scip_cli_version(scip_bin: str) -> str:
    """Return the scip CLI version string.

    Parameters
    ----------
    scip_bin:
        Path or name of the scip CLI binary.

    Returns
    -------
    str
        The CLI version string (e.g. "v0.6.1").
    """
    proc = subprocess.run(
        [scip_bin, "--version"],
        capture_output=True,
        text=True,
        check=True,
    )
    return proc.stdout.strip()


def download_to(url: str, target: Path) -> None:
    """Download a URL to a local file.

    Parameters
    ----------
    url:
        The URL to download.
    target:
        Destination file path.
    """
    with urlopen(url, timeout=30) as resp:
        data = resp.read()
    target.write_bytes(data)


def ensure_build_dir(repo_root: Path, build_subdir: Path) -> Path:
    """Ensure the build/scip directory exists.

    Parameters
    ----------
    repo_root:
        Repository root path.
    build_subdir:
        Relative build subdirectory (defaults to build/scip).

    Returns
    -------
    pathlib.Path
        Resolved build directory path.
    """
    build_dir = (repo_root / build_subdir).resolve()
    build_dir.mkdir(parents=True, exist_ok=True)
    return build_dir


def resolve_scip_proto_path(
    *,
    scip_bin: str,
    build_dir: Path,
) -> Path:
    """Resolve scip.proto under build/scip, downloading if needed.

    If SCIP_PROTO_PATH is set and exists, it is copied into build/scip/scip.proto.

    Parameters
    ----------
    scip_bin:
        scip CLI path.
    build_dir:
        Build directory where scip.proto must live.

    Returns
    -------
    pathlib.Path
        Path to build/scip/scip.proto.
    """
    target = build_dir / SCIP_PROTO_FILENAME
    if target.exists():
        return target

    env_override = os.getenv("SCIP_PROTO_PATH")
    if env_override:
        override_path = Path(env_override)
        if override_path.exists():
            target.write_bytes(override_path.read_bytes())
            return target

    version = scip_cli_version(scip_bin)
    url = f"https://raw.githubusercontent.com/sourcegraph/scip/{version}/scip.proto"
    download_to(url, target)
    return target


def generate_scip_pb2(proto_path: Path, out_dir: Path) -> None:
    """Run protoc to generate scip_pb2 bindings in build/scip.

    Parameters
    ----------
    proto_path:
        Path to scip.proto.
    out_dir:
        Output directory for generated bindings.
    """
    subprocess.run(
        [
            "protoc",
            f"-I{proto_path.parent}",
            f"--python_out={out_dir}",
            f"--pyi_out={out_dir}",
            str(proto_path),
        ],
        check=True,
    )


def parse_args(argv: list[str]) -> argparse.Namespace:
    """Parse CLI arguments.

    Parameters
    ----------
    argv:
        CLI arguments.

    Returns
    -------
    argparse.Namespace
        Parsed arguments.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", default=".", help="Repository root directory.")
    parser.add_argument("--scip-bin", default=DEFAULT_SCIP_BIN, help="scip CLI binary.")
    parser.add_argument(
        "--build-subdir",
        default=str(DEFAULT_BUILD_SUBDIR),
        help="Relative build directory for SCIP artifacts.",
    )
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    """Run scip.proto discovery and code generation.

    Parameters
    ----------
    argv:
        CLI arguments.

    Returns
    -------
    int
        Exit code.
    """
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    logger = logging.getLogger(__name__)
    args = parse_args(argv)
    repo_root = Path(args.repo_root).resolve()
    build_subdir = Path(args.build_subdir)

    build_dir = ensure_build_dir(repo_root, build_subdir)
    proto_path = resolve_scip_proto_path(scip_bin=args.scip_bin, build_dir=build_dir)
    generate_scip_pb2(proto_path=proto_path, out_dir=build_dir)
    logger.info("Generated scip_pb2 under %s", build_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
