#!/usr/bin/env python3
"""
Visualize SSTable token distribution relative to vnode boundaries.

For each SSTable, prints an ASCII histogram showing how many partition tokens
fall into each vnode range.

Usage examples:

  # Using scylla sstable dump-data (complete token extraction):
  python scripts/sstable_token_dist.py \\
      --scylla-path ./build/dev/scylla \\
      --scylla-yaml /path/to/scylla.yaml \\
      --sstables /path/to/*-Data.db \\
      --boundaries -8070450532247928833,-6917529027641081857,0,4611686018427387903

  # Using dump-summary for faster (sampled) extraction:
  python scripts/sstable_token_dist.py \\
      --scylla-path ./build/dev/scylla \\
      --scylla-yaml /path/to/scylla.yaml \\
      --sstables /path/to/*-Data.db \\
      --boundaries-file boundaries.txt \\
      --mode summary
"""

import os
import re
import sys
import json
import shutil
import argparse
import subprocess
from bisect import bisect_right


TOKEN_MIN = -2**63
TOKEN_MAX = 2**63 - 1


def parse_boundaries(boundaries_str: str | None, boundaries_file: str | None) -> list[int]:
    """Parse token boundaries from a comma-separated string or a file.

    Args:
        boundaries_str: Comma-separated token boundary values.
        boundaries_file: Path to a file with one boundary per line.

    Returns:
        Sorted list of token boundaries.
    """
    if boundaries_str:
        tokens = [int(t.strip()) for t in boundaries_str.split(",")]
    elif boundaries_file:
        with open(boundaries_file) as f:
            tokens = [int(line.strip()) for line in f if line.strip()]
    else:
        print("Error: provide --boundaries or --boundaries-file", file=sys.stderr)
        sys.exit(1)
    return sorted(tokens)


def extract_tokens_dump_data(scylla_path: str, scylla_yaml: str | None, sstable_files: list[str]) -> dict[str, list[int]]:
    """Extract per-partition tokens from SSTables via 'scylla sstable dump-data'.

    Args:
        scylla_path: Path to the scylla executable.
        scylla_yaml: Optional path to the scylla.yaml config file.
        sstable_files: List of SSTable Data.db file paths.

    Returns:
        Dict mapping SSTable path to a sorted list of partition tokens.
    """
    cmd = [scylla_path, "sstable", "dump-data",
           "--output-format", "json"]
    if scylla_yaml:
        cmd.extend(["--scylla-yaml-file", scylla_yaml])
    cmd.extend(["--sstables"] + sstable_files)
    try:
        result = subprocess.check_output(cmd, stderr=subprocess.PIPE)
    except subprocess.CalledProcessError as e:
        print(f"scylla sstable dump-data failed (exit {e.returncode})", file=sys.stderr)
        print(f"stderr: {e.stderr.decode('utf-8', 'ignore')}", file=sys.stderr)
        sys.exit(1)

    data = json.loads(result.decode("utf-8", "ignore"))
    tokens_by_sstable: dict[str, list[int]] = {}

    sstables = data.get("sstables", data)
    for path, partitions in sstables.items():
        toks = sorted(int(p["key"]["token"]) for p in partitions)
        tokens_by_sstable[path] = toks

    return tokens_by_sstable


# Regex to match partition_start lines in text-format dump-data output.
# Example line:
#   {partition_start: pk {key: pk{00040018f14b}, token: 966992607985823095} ...}
_PARTITION_START_RE = re.compile(r'\{partition_start:.*token:\s*(-?\d+)')


def extract_tokens_dump_text(scylla_path: str, scylla_yaml: str | None,
                             sstable_files: list[str]) -> dict[str, list[int]]:
    """Extract per-partition tokens by streaming text-format dump-data output.

    Runs 'scylla sstable dump-data' with text output (the default) and parses
    the token from each partition_start line.  This is much faster than JSON
    mode and uses constant memory regardless of SSTable size.

    Args:
        scylla_path: Path to the scylla executable.
        scylla_yaml: Optional path to the scylla.yaml config file.
        sstable_files: List of SSTable Data.db file paths.

    Returns:
        Dict mapping SSTable path to a sorted list of partition tokens.
    """
    tokens_by_sstable: dict[str, list[int]] = {}

    for sstable_path in sstable_files:
        cmd = [scylla_path, "sstable", "dump-data",
               "--output-format", "text"]
        if scylla_yaml:
            cmd.extend(["--scylla-yaml-file", scylla_yaml])
        cmd.extend(["--sstables", sstable_path])
        try:
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE, text=True)
        except OSError as e:
            print(f"Failed to run scylla sstable dump-data: {e}", file=sys.stderr)
            sys.exit(1)

        toks: list[int] = []
        for line in proc.stdout:
            m = _PARTITION_START_RE.search(line)
            if m:
                toks.append(int(m.group(1)))

        rc = proc.wait()
        if rc != 0:
            stderr_out = proc.stderr.read()
            print(f"scylla sstable dump-data failed (exit {rc}) for {sstable_path}",
                  file=sys.stderr)
            print(f"stderr: {stderr_out}", file=sys.stderr)
            sys.exit(1)

        tokens_by_sstable[sstable_path] = sorted(toks)

    return tokens_by_sstable


def extract_tokens_dump_index(scylla_path: str, scylla_yaml: str | None,
                              sstable_files: list[str]) -> dict[str, list[int]]:
    """Extract tokens from SSTables via 'scylla sstable dump-index --show-tokens'.

    Uses the --show-tokens flag so that scylla computes and includes the
    token for each partition key directly in the JSON output.  This is
    faster than dump-data because it only reads the Index component.

    Args:
        scylla_path: Path to the scylla executable.
        scylla_yaml: Optional path to the scylla.yaml config file.
        sstable_files: List of SSTable Data.db file paths.

    Returns:
        Dict mapping SSTable path to a sorted list of partition tokens.
    """
    cmd = [scylla_path, "sstable", "dump-index",
           "--show-tokens"]
    if scylla_yaml:
        cmd.extend(["--scylla-yaml-file", scylla_yaml])
    cmd.extend(["--sstables"] + sstable_files)
    try:
        result = subprocess.check_output(cmd, stderr=subprocess.PIPE)
    except subprocess.CalledProcessError as e:
        print(f"scylla sstable dump-index failed (exit {e.returncode})", file=sys.stderr)
        print(f"stderr: {e.stderr.decode('utf-8', 'ignore')}", file=sys.stderr)
        sys.exit(1)

    data = json.loads(result.decode("utf-8", "ignore"))
    tokens_by_sstable: dict[str, list[int]] = {}

    sstables = data.get("sstables", data)
    for path, entries in sstables.items():
        toks = sorted(int(entry["key"]["token"]) for entry in entries)
        tokens_by_sstable[path] = toks

    return tokens_by_sstable


def extract_tokens_dump_summary(scylla_path: str, scylla_yaml: str | None,
                                sstable_files: list[str]) -> dict[str, list[int]]:
    """Extract sampled tokens from SSTables via 'scylla sstable dump-summary'.

    Args:
        scylla_path: Path to the scylla executable.
        scylla_yaml: Optional path to the scylla.yaml config file.
        sstable_files: List of SSTable Data.db file paths.

    Returns:
        Dict mapping SSTable path to a sorted list of sampled partition tokens.
    """
    cmd = [scylla_path, "sstable", "dump-summary"]
    if scylla_yaml:
        cmd.extend(["--scylla-yaml-file", scylla_yaml])
    cmd.extend(["--sstables"] + sstable_files)
    try:
        result = subprocess.check_output(cmd, stderr=subprocess.PIPE)
    except subprocess.CalledProcessError as e:
        print(f"scylla sstable dump-summary failed (exit {e.returncode})", file=sys.stderr)
        print(f"stderr: {e.stderr.decode('utf-8', 'ignore')}", file=sys.stderr)
        sys.exit(1)

    data = json.loads(result.decode("utf-8", "ignore"))
    tokens_by_sstable: dict[str, list[int]] = {}

    sstables = data.get("sstables", data)
    for path, info in sstables.items():
        toks: list[int] = []
        for entry in info.get("entries", []):
            toks.append(int(entry["key"]["token"]))
        if "first_key" in info and "token" in info["first_key"]:
            toks.append(int(info["first_key"]["token"]))
        if "last_key" in info and "token" in info["last_key"]:
            toks.append(int(info["last_key"]["token"]))
        tokens_by_sstable[path] = sorted(set(toks))

    return tokens_by_sstable


def build_segments(boundaries: list[int]) -> list[tuple[int, int]]:
    """Build vnode segments from boundary tokens.

    Each segment is a (start, end) range.  The first segment starts at
    TOKEN_MIN.  If the last boundary is below TOKEN_MAX, an extra
    wrap-around segment is appended.

    Args:
        boundaries: Sorted list of vnode upper-bound tokens.

    Returns:
        List of (start, end) tuples covering the full token range.
    """
    segments: list[tuple[int, int]] = []
    for i, b in enumerate(boundaries):
        seg_start = TOKEN_MIN if i == 0 else boundaries[i - 1]
        segments.append((seg_start, b))
    if boundaries[-1] < TOKEN_MAX:
        segments.append((boundaries[-1], TOKEN_MAX))
    return segments


def bin_tokens(tokens: list[int], boundaries: list[int],
               n_segments: int) -> list[int]:
    """Count how many tokens fall into each vnode segment.

    Args:
        tokens: Sorted list of partition tokens.
        boundaries: Sorted list of vnode upper-bound tokens.
        n_segments: Total number of segments.

    Returns:
        List of counts, one per segment.
    """
    counts = [0] * n_segments
    for t in tokens:
        idx = bisect_right(boundaries, t)
        if idx < n_segments:
            counts[idx] += 1
        else:
            counts[-1] += 1
    return counts


def format_size(size: int) -> str:
    """Format a file size in human-readable units.

    Args:
        size: Size in bytes.

    Returns:
        Formatted string with appropriate unit.
    """
    if size < 1024:
        return f"{size} bytes"
    elif size < 1024**2:
        return f"{size / 1024:.1f} KiB"
    elif size < 1024**3:
        return f"{size / 1024**2:.1f} MiB"
    else:
        return f"{size / 1024**3:.1f} GiB"


def print_histogram(name: str, file_size: int, counts: list[int], segments: list[tuple[int, int]], max_bar_width: int):
    """Print one ASCII histogram for an SSTable.

    Args:
        name: SSTable display name.
        file_size: SSTable file size in bytes.
        counts: Token count per segment.
        segments: List of (start, end) tuples.
        max_bar_width: Maximum number of '*' characters for the largest bar.
    """
    peak = max(counts) if max(counts) > 0 else 1
    total = sum(counts)

    # Column widths
    idx_w = len(str(len(segments) - 1))
    token_w = max(len(str(t)) for seg in segments for t in seg)
    range_w = max(len("vnode range"), len(f"({0:>{token_w}}, {0:>{token_w}}]"))
    count_w = len(str(peak))

    print(f"\n{'=' * 70}")
    print(f"  SSTable: {name}    ({format_size(file_size)}, {total} partitions)")
    print(f"{'=' * 70}")
    header = f"  {'#':<{idx_w}}  {'vnode range':<{range_w}}  |{'count':>{count_w}}  | bar  (scaled to {max_bar_width})"
    print(header)
    print(f"  {'-' * (len(header) - 2)}")

    for i, ((seg_start, seg_end), count) in enumerate(zip(segments, counts)):
        bar_len = round(count / peak * max_bar_width) if peak > 0 else 0
        bar = '*' * bar_len
        range_str = f"({seg_start:>{token_w}}, {seg_end:>{token_w}}]"
        print(f"  {i:<{idx_w}}  {range_str:<{range_w}}  |{count:>{count_w}}  | {bar}")


def main():
    parser = argparse.ArgumentParser(
        description="Visualize SSTable token distribution relative to vnode boundaries (ASCII histogram).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    src = parser.add_argument_group("Token extraction (choose one source)")
    src.add_argument("--scylla-path", type=str,
                     help="Path to the scylla executable.")
    src.add_argument("--scylla-yaml", type=str,
                     help="Optional path to the scylla.yaml config file.")
    src.add_argument("--sstables", type=str, nargs="+",
                     help="SSTable Data.db file paths.")
    src.add_argument("--mode", choices=["data-text", "data-json", "index", "summary"], default="data-text",
                     help="Token extraction mode: "
                         "'data-text' (reads tokens from the Data component in text format; fast, complete, default), "
                         "'data-json' (reads tokens from the Data component in JSON format; complete but slower), "
                         "'index' (reads tokens from the Index component; fast and complete), "
                         "or 'summary' (reads sampled tokens from the Summary component; fastest but less detailed).")

    bnd = parser.add_argument_group("Token boundaries")
    bnd.add_argument("--boundaries", type=str,
                     help="Comma-separated list of vnode boundary tokens.")
    bnd.add_argument("--boundaries-file", type=str,
                     help="File with one boundary token per line.")

    out = parser.add_argument_group("Display options")
    out.add_argument("--bar-width", type=int, default=None,
                     help="Max bar width in characters. Default: auto-fit terminal.")

    args = parser.parse_args()

    boundaries = parse_boundaries(args.boundaries, args.boundaries_file)
    if not boundaries:
        print("Error: no token boundaries provided.", file=sys.stderr)
        sys.exit(1)

    # --- Extract tokens ---
    if args.scylla_path and args.sstables:
        if args.mode == "data-text":
            tokens_by_sstable = extract_tokens_dump_text(
                args.scylla_path, args.scylla_yaml, args.sstables)
        elif args.mode == "data-json":
            tokens_by_sstable = extract_tokens_dump_data(
                args.scylla_path, args.scylla_yaml, args.sstables)
        elif args.mode == "index":
            tokens_by_sstable = extract_tokens_dump_index(
                args.scylla_path, args.scylla_yaml, args.sstables)
        else:
            tokens_by_sstable = extract_tokens_dump_summary(
                args.scylla_path, args.scylla_yaml, args.sstables)
    else:
        print("Error: provide --scylla-path + --sstables.", file=sys.stderr)
        sys.exit(1)

    if not tokens_by_sstable:
        print("Error: no tokens extracted from any SSTable.", file=sys.stderr)
        sys.exit(1)

    total_tokens = sum(len(v) for v in tokens_by_sstable.values())
    print(f"Extracted {total_tokens} tokens from {len(tokens_by_sstable)} SSTable(s)")
    print(f"Using {len(boundaries)} vnode boundaries")

    # --- Build segments and determine bar width ---
    segments = build_segments(boundaries)
    term_width = shutil.get_terminal_size((120, 24)).columns
    max_bar_width = args.bar_width if args.bar_width else max(20, term_width - 80)

    # --- Print a histogram for each SSTable ---
    for path in sorted(tokens_by_sstable.keys()):
        toks = tokens_by_sstable[path]
        counts = bin_tokens(toks, boundaries, len(segments))
        name = os.path.basename(path)
        file_size = os.path.getsize(path)
        print_histogram(name, file_size, counts, segments, max_bar_width)


if __name__ == "__main__":
    main()
