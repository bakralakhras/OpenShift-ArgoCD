#!/bin/bash
# vault-init.sh
# One-time script to seed platform secrets into Vault
# Run this ONCE from the helper node after Vault is up
# These replace all hardcoded passwords in platform manifests

set -e

VAULT_ADDR="https://vault.apps.ocp4.infrahub.com"
VAULT_TOKEN="root"

echo ">>> Logging into Vault..."
export VAULT_ADDR VAULT_TOKEN

# Enable KV v2 secrets engine at 'secret/' (already enabled in dev mode)
echo ">>> Enabling KV v2 secrets engine..."
vault secrets enable -path=secret kv-v2 2>/dev/null || echo "    secret/ already enabled, skipping"

echo ">>> Seeding MinIO credentials..."
vault kv put secret/platform/minio \
  rootUser="admin" \
  rootPassword="minio1234"

echo ">>> Seeding PostgreSQL credentials..."
vault kv put secret/platform/postgresql \
  password="hive123" \
  postgres-password="hive123"

echo ">>> Seeding NiFi credentials..."
vault kv put secret/platform/nifi \
  username="admin" \
  password="admin1234567890"

echo ">>> Seeding Grafana credentials..."
vault kv put secret/platform/grafana \
  username="admin" \
  password="admin1234567890"

echo ""
echo ">>> Verifying secrets..."
vault kv get secret/platform/minio
vault kv get secret/platform/postgresql
vault kv get secret/platform/nifi
vault kv get secret/platform/grafana

echo ""
echo ">>> Done. All platform secrets seeded into Vault."
echo "    Next: create the vault-root-token k8s secret for VSO auth"
echo "    Run: oc create secret generic vault-root-token -n vault --from-literal=token=root"
