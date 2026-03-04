#!/usr/bin/env python3
import csv
import re
import sys
import shutil
from pathlib import Path

INPUT = "candidate_data.csv"
OUTPUT = "candidate_data.csv"
BACKUP = "candidate_data.bak.csv"

SINCE_RE = re.compile(r"since:\d{4}-\d{2}-\d{2}")

GREEN = "\033[92m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
DIM = "\033[2m"
BOLD = "\033[1m"
RESET = "\033[0m"


def rebuild_query(row):
    nepali = row["CandidateName"].strip()
    english = row["EnglishCandidateName"].strip()
    dist = row["EnglishDistrictName"].strip().lower()
    since = SINCE_RE.search(row["PowerSearchQuery"])
    since = since.group(0) if since else "since:2025-09-05"
    return f'("{nepali}" OR "{english}") ("{dist}" OR "constituency") {since}'


def main():
    src = Path(INPUT)
    if not src.exists():
        print(f"ERROR: '{INPUT}' not found. Run from the same folder as the CSV.")
        sys.exit(1)

    shutil.copy(src, BACKUP)

    with open(src, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)

    total = len(rows)
    changed = 0
    idx = 0

    print(f"\n{BOLD}{'─' * 64}{RESET}")
    print(f"  Candidate Name Corrector  ({total} rows)")
    print(
        f"  {DIM}Enter = keep  |  type new value = correct  |  q = save & quit{RESET}"
    )
    print(f"  {DIM}b = go back one row{RESET}")
    print(f"{BOLD}{'─' * 64}{RESET}\n")

    while idx < total:
        row = rows[idx]
        eng = row["EnglishCandidateName"]
        dist = row["EnglishDistrictName"]
        const = row["ConstName"]

        print(
            f"{DIM}[{idx + 1}/{total}]{RESET}  {CYAN}{row['CandidateName']}{RESET}  {DIM}({dist} / const {const}){RESET}"
        )

        try:
            ans = input(f"  {YELLOW}{eng}{RESET} : ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nInterrupted — saving progress.")
            break

        if ans.lower() == "q":
            print("Quitting — saving progress.")
            break
        if ans.lower() == "b":
            idx = max(0, idx - 1)
            print()
            continue
        if ans:
            row["EnglishCandidateName"] = ans
            row["PowerSearchQuery"] = rebuild_query(row)
            changed += 1
            print(f"  {GREEN}✓  {ans}{RESET}")

        print()
        idx += 1

    with open(OUTPUT, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"{BOLD}{'─' * 64}{RESET}")
    print(f"  {GREEN}Saved{RESET}  {changed} row(s) changed  →  {OUTPUT}")
    print(f"  {DIM}Backup at {BACKUP}{RESET}")
    print(f"{BOLD}{'─' * 64}{RESET}\n")


if __name__ == "__main__":
    main()
