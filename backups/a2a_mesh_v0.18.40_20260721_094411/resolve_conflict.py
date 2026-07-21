#!/usr/bin/env python3
"""Resolve git merge conflicts in cli.py — accept upstream (our) version."""
import re

with open('cli.py', 'r') as f:
    content = f.read()

count = len(re.findall(r'<<<<<<< Updated upstream', content))

# Accept upstream version for all conflicts
resolved = re.sub(
    r'<<<<<<< Updated upstream\n(.*?)\n=======\n(.*?)\n>>>>>>> Stashed changes',
    r'\1',
    content,
    flags=re.DOTALL
)

with open('cli.py', 'w') as f:
    f.write(resolved)

print(f'Resolved {count} conflicts in cli.py')