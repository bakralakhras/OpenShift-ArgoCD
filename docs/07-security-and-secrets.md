# Security and Secrets

## Overview

Credentials are intentionally excluded from the repository.

The platform includes Vault and Vault Secrets Operator resources for GitOps-managed secret delivery.

---

## Secret Flow

```text
Vault KV
    |
    v
Vault Secrets Operator
    |
    v
Kubernetes Secrets
    |
    v
Application pods
```

---

## Validated State

Current validation showed:

| Resource | State |
|---|---|
| Vault pod | Running |
| Vault service | Present |
| Vault route | Present |
| VaultConnection | Healthy / Ready |
| VaultAuth | Healthy / Ready |
| VaultStaticSecret resources | Present, but several not synced/healthy |

VaultStaticSecret resources for MinIO, NiFi, PostgreSQL, and Superset still require follow-up in the current lab state.

---

## Documentation Rule

The README and docs do not publish:

- passwords
- tokens
- access keys
- admin credentials
- private route login details
- secret values

This is intentional.

---

## Engineering Interpretation

The secret architecture exists and is wired into the platform, but the current validation state should not be described as fully healthy secret sync.

Correct wording:

> Vault and Vault Secrets Operator are configured for GitOps-managed secret delivery, with runtime credentials excluded from the repository. Several VaultStaticSecret sync resources still require follow-up in the current lab environment.
