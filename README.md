## Branch Protection Policy

Direct pushes and force pushes to `main` are disabled. All changes must be merged via Pull Requests following these standards:

- **Approvals:** Requires 1 review approval. Stale approvals are dismissed upon new commits.
- **Review Validity:** Requires approval on the most recent reviewable push.
- **CI/CD Checks:** All status checks must pass, and the source branch must be up-to-date with `main`.
- **Conversations:** All discussion threads must be resolved before merging.
- **Strict Compliance:** No bypass permissions are granted (applies to administrators as well).
