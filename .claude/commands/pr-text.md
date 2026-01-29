Generate a PR description for the current branch.

Analyze the **net changes** between the current branch and main by examining:
1. The diff summary: `git diff main...HEAD --stat`
2. The actual changes: `git diff main...HEAD` (focus on key changes, not every line)

IMPORTANT: Focus on what the branch adds/changes compared to main as a whole. Do NOT describe individual commits or intermediate work. The reviewer only sees the final diff - they don't care about bugs introduced and fixed within the same branch.

Then generate a PR title and description. Output as a markdown code block on its own line (no text before the opening ```) so the user can easily copy it:

```markdown
# PR Title
<component>: <short description>

## Summary of Changes
-
-

## Testing Verification
-
-
```

Follow the PR guidelines in CLAUDE.md for title format, body structure, and style.
