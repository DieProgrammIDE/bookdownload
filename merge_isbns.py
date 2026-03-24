#!/usr/bin/env python3
"""Merge all isbn*.txt files into a single deduplicated all_isbns.txt."""

import glob
import re

isbns = []
seen = set()

for path in sorted(glob.glob("isbn/*.txt")):
    with open(path) as f:
        for line in f:
            raw = line.strip()
            if not raw or raw.startswith("#"):
                continue
            isbn = re.sub(r"[\s-]", "", raw)
            if isbn not in seen:
                seen.add(isbn)
                isbns.append(isbn)

with open("all_isbns.txt", "w") as f:
    for isbn in isbns:
        f.write(isbn + "\n")

print(f"Wrote {len(isbns)} unique ISBNs to all_isbns.txt")
