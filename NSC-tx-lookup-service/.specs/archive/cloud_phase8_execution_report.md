# Phase 8 Cloud-Test Execution Report

> **아카이브**: 이 보고서는 Phase 8 Cloud-Test의 이력 기록이다.
> Phase 8 프로비저닝 스크립트(`scripts/cloud/phase8/`)는 인프라팀 이관 후 제거되었다.

Last updated: 2026-02-09

## 1. Scope

Environment:
- Subscription: `27db5ec6-d206-4028-b5e1-6004dca5eeef`
- Resource group: `2dt-final-team4`
- Region: `koreacentral`

Goal:
- Complete `.roadmap/implementation_roadmap.md` Phase 8 (Cloud-Test) items end-to-end.

## 2. Provisioned resources

- Log Analytics: `team4-txlookup-law-2dt026-test`
- Application Insights: `team4-txlookup-ai-2dt026-test`
- Event Hubs namespace: `team4-txlookup-2dt026-test`
- Event hubs: `ledger.entry.upserted`, `payment.order.upserted`
- Consumer group: `bo-sync-test-2dt026-v1` (both hubs)
- PostgreSQL Flexible Server: `team4-txlookup-pg-2dt026-test` (public access)
- ACR: `team4txlookup2dt026test`
- Container Apps env: `team4-tx-caenv-2dt026-t`
- Container App(API): `team4-tx-api-2dt026-t`
- Container App(Consumer): `team4-tx-con-2dt026-t`

## 2.1 Cross-repo inventory snapshot (historical)

Primary RG observed set (reference snapshot):

| Name | Type | Notes |
| --- | --- | --- |
| `team4-trade-data` | `Microsoft.EventHub/namespaces` | Existing Event Hubs namespace (legacy/shared naming) |
| `2dtfinalteam4storagetest` | `Microsoft.Storage/storageAccounts` | Existing storage account |
| `2dt-final-team4-databricks-test` | `Microsoft.Databricks/workspaces` | Databricks workspace used by Databricks repo |
| `team4-txlookup-law-2dt026-test` | `Microsoft.OperationalInsights/workspaces` | Phase 8 Log Analytics workspace |
| `team4-txlookup-ai-2dt026-test` | `Microsoft.Insights/components` | Phase 8 Application Insights |
| `team4-txlookup-2dt026-test` | `Microsoft.EventHub/namespaces` | Phase 8 isolated Event Hubs namespace |
| `team4-txlookup-pg-2dt026-test` | `Microsoft.DBforPostgreSQL/flexibleServers` | Phase 8 PostgreSQL Flexible Server |
| `team4txlookup2dt026test` | `Microsoft.ContainerRegistry/registries` | Phase 8 ACR |
| `team4-tx-caenv-2dt026-t` | `Microsoft.App/managedEnvironments` | Phase 8 Container Apps Environment |
| `team4-tx-api-2dt026-t` | `Microsoft.App/containerApps` | Phase 8 Admin API app |
| `team4-tx-con-2dt026-t` | `Microsoft.App/containerApps` | Phase 8 Consumer app |

Databricks managed RG observed set (reference snapshot):

Resource group: `databricks-rg-2dt-final-team4-databricks-test-i75zgjbs3ihjc`

| Name | Type |
| --- | --- |
| `unity-catalog-access-connector` | `Microsoft.Databricks/accessConnectors` |
| `dbstoragesgx3kg7f5lqrq` | `Microsoft.Storage/storageAccounts` |
| `dbmanagedidentity` | `Microsoft.ManagedIdentity/userAssignedIdentities` |
| `nat-gw-public-ip` | `Microsoft.Network/publicIPAddresses` |
| `workers-sg` | `Microsoft.Network/networkSecurityGroups` |
| `nat-gateway` | `Microsoft.Network/natGateways` |
| `workers-vnet` | `Microsoft.Network/virtualNetworks` |

## 2.2 Applied Phase 8 naming set

- Event Hubs namespace: `team4-txlookup-2dt026-test`
- Event hubs: `ledger.entry.upserted`, `payment.order.upserted`
- Consumer group: `bo-sync-test-2dt026-v1`
- PostgreSQL Flexible Server: `team4-txlookup-pg-2dt026-test`
- Azure Container Registry: `team4txlookup2dt026test`
- Container Apps Environment: `team4-tx-caenv-2dt026-t`
- Container App (API): `team4-tx-api-2dt026-t`
- Container App (Consumer): `team4-tx-con-2dt026-t`
- Log Analytics Workspace: `team4-txlookup-law-2dt026-test`
- Application Insights: `team4-txlookup-ai-2dt026-test`

## 3. Deployment profile

- Messaging: Event Hubs Kafka endpoint
- Secrets: SAS/env injection (phase8 test profile)
- Consumer scaling: `min=1`, `max=1`
- API ingress: public HTTPS
- DB connection: `sslmode=require`

## 4. Validation results

Cloud smoke scenarios passed:
- `happy` (publish -> consume -> upsert -> API 200)
- `duplicate` (idempotent reprocess kept stable API result)
- `out_of_order` (newer version remained authoritative)
- `error` (invalid payload path produced consumer DLQ error log)
- missing tx check (API 404)

Destroy/Recreate validation:
- Resource-group lock blocked direct delete (`ScopeLocked`).
- Fallback executed: consumer scale cycle (`min 1 -> 0 -> 1`) and pipeline health revalidated.

## 5. Artifacts

Added execution automation (제거됨 — 인프라팀 이관):
- ~~`scripts/cloud/phase8/provision_resources.sh`~~
- ~~`scripts/cloud/phase8/build_push_images.sh`~~
- ~~`scripts/cloud/phase8/run_db_migrations.sh`~~
- ~~`scripts/cloud/phase8/deploy_apps.sh`~~
- ~~`scripts/cloud/phase8/run_smoke.sh`~~
- ~~`scripts/cloud/phase8/destroy_recreate_check.sh`~~
- ~~`scripts/cloud/phase8/run_all.sh`~~
- ~~`scripts/cloud/phase8/check_admin_tx.py`~~
- `scripts/publish_synthetic_events.py` (유지 — 로컬 개발/테스트용)

Container build assets:
- `docker/api.Dockerfile`
- `docker/consumer.Dockerfile`

## 6. Notes

- Container Apps names were shortened from the original naming proposal to meet Azure length constraints.
- RG lock behavior is retained; Phase 9 secure environment should assume separate, lock-aware deployment policy.
- Persistent naming/tag/guardrail policy was moved to `.specs/infra/cloud_migration_rebuild_plan.md` section `3.3`.
