# 결정 필요 항목 목록 (Open Decisions)

작성일: 2026-02-05
업데이트: 2026-02-12

## 목적
구현 중 **명확히 결정되지 않았거나 가정으로 처리한 항목**을 기록하고,
결정이 내려지면 본 문서를 갱신한다.

## 업데이트 규칙
- 구현 중 새로운 가정/모호점이 생기면 **즉시 본 문서에 추가**한다.
- 결정이 확정되면 항목을 `결정됨`으로 변경하고 **근거 문서/룰/테이블**을 명시한다.

## 결정 필요 항목

### D-001 `user_id` ↔ `wallet_id` 매핑 기준
- 상태: **결정됨(2026-02-09)**
- 결정: 현 단계에서는 `user_id == wallet_id`로 취급한다.
- 영향: `gold.recon_daily_snapshot_flow`의 `user_id` 기준 대사 결과
- 재검토 트리거: 외부 SSOT(매핑 테이블 또는 소유 이력)가 도입되면 신규 결정으로 전환 여부를 검토한다.
- 근거: 사용자 결정(옵션 A)

### D-002 일일 스냅샷 start/end 선택 규칙
- 상태: **결정됨(2026-02-09)**
- 결정: 현 단계에서는 대상 `date_kst` 내 **최소/최대 `snapshot_ts`**를 start/end로 사용한다.
- 영향: `delta_balance_total` 계산
- 재검토 트리거: 업스트림이 “일자 cutoff 기반” 스냅샷 정책을 제공하면, cutoff 규칙으로 전환을 검토한다.
- 근거: 사용자 결정(옵션 A)

### D-003 `issued_supply` 산정 SSOT
- 상태: **결정됨(2026-02-09)**
- 결정: 현 단계에서는 원장 타입(MINT/CHARGE/BURN/WITHDRAW) 합산을 `issued_supply`로 사용한다.
- 영향: `gold.ledger_supply_balance_daily` 결과
- 재검토 트리거: OLTP/정산의 발행량 스냅샷 SSOT가 제공되면, 이를 SSOT로 전환하는 개선을 검토한다.
- 근거: 사용자 결정(옵션 A)

### D-004 게이팅 처리 정책
- 상태: **결정됨(2026-02-06)**
- 결정: Pipeline A가 stale/drop이어도 `gold.exception_ledger.severity`는 원본 등급을 유지하고, 알림 채널에서만 억제한다.
- 영향: 예외 심각도 의미를 보존하면서 운영 알림 노이즈를 제어한다.
- 근거: 운영 관측/감사 정확도 우선 정책

### D-005 Drift/공급 차이 임계치 비교 방식
- 상태: **결정됨(2026-02-06)**
- 결정: 임계치 비교는 `value > threshold`를 사용한다.
- 영향: 경계값 도달만으로 과경보가 발생하지 않도록 한다.
- 근거: 임계치 0 구간에서 노이즈 억제

### D-006 결제 실패 status 집합
- 상태: **결정됨(2026-02-06)**
- 결정: 실패 지표(`gold.ops_payment_failure_daily.failed_cnt`)는 `FAILED`, `CANCELLED`만 포함한다.
- 보완: `REFUNDED`는 실패율과 분리된 별도 지표로 관리한다(동일 Pipeline B 내 추가 산출물로 처리, 신규 파이프라인 불필요).
- 영향: 실패율 의미를 유지하면서 환불 추세를 별도로 관찰 가능
- 근거: 운영 지표 해석 분리 원칙

### D-007 페어링 품질 지표 정의
- 상태: **결정됨(2026-02-06)**
- 결정: `related_id` 그룹 크기 2 + 서로 다른 `wallet_id`인 경우만 pair 후보로 계산한다.
- 영향: 규칙 단순성과 재현성을 우선하며, 운영 대시보드 해석 일관성을 확보한다.
- 근거: 현 단계의 최소 안정 규칙 고정

### D-008 `ledger_entries` 멱등성 키 확장 여부
- 상태: **결정됨(2026-02-09)**
- 결정: 현 단계에서는 `silver.ledger_entries` 멱등성 키를 `(tx_id, wallet_id)`로 유지한다.
- 영향: `tx_id`는 행 단위 유니크(PK)라는 계약을 전제로 DQ에서 중복을 차단한다.
- 재검토 트리거: 실데이터에서 “동일 `tx_id` 다중 엔트리”가 관측되면 `entry_seq`(또는 `entry_id`) 도입으로 재결정한다.
- 근거: `.specs/data_contract.md`의 `transaction_ledger.tx_id` PK 가정 + 사용자 결정(옵션 A)

### D-009 completeness 연속 0 윈도우 상태 저장
- 상태: **결정됨(2026-02-06)**
- 결정: `gold.pipeline_state`에 `source_table`별 연속 0 윈도우 카운터를 저장/갱신한다.
- 영향: 실행 간 completeness 상태 연속성을 보장하고, 누적 경보 정확도를 높인다.
- 근거: 상태 저장 일관성 우선 정책

### D-010 `drift_pct` 분모 0 처리
- 상태: **결정됨(2026-02-06)**
- 결정: `net_flow_total == 0`이면 `drift_pct = NULL`로 유지한다.
- 영향: 분모 0 구간의 의미 왜곡을 방지하고 지표 해석을 명확히 유지한다.
- 근거: 수학적 정의 불가 구간은 NULL 표현 원칙

### D-011 `gold.fact_payment_anonymized.category` 다중 아이템 처리
- 상태: **결정됨(2026-02-06)**
- 결정: `order_ref` 기준 `order_items` 중 line_amount가 가장 큰 아이템의 `products.category`를 대표값으로 사용하고, 동률이면 가장 작은 `item_id`를 선택한다.
- 영향: 단일 카테고리 스키마를 유지해 분석/집계 단순성을 확보한다.
- 근거: 현 단계 팩트 스키마 단순화 우선

### D-012 `gold.fact_payment_anonymized` 소스 필터
- 상태: **결정됨(2026-02-06)**
- 결정: `silver.order_events` 중 `order_source = PAYMENT_ORDERS`만 `gold.fact_payment_anonymized`에 포함한다.
- 영향: 결제 이벤트 중심 팩트 정의를 유지하고 중복 위험을 줄인다.
- 근거: 결제 지표 목적의 스코프 고정

### D-013 Silver Analytics 파티셔닝
- 상태: **결정됨(2026-02-06)**
- 결정: `silver.order_items`, `silver.products`는 현재 단계에서 파티션 없이 유지한다.
- 영향: 스키마/운영 복잡도를 낮추고, 데이터 규모 증가 시 파티션 컬럼 보강 후 재평가한다.
- 근거: 초기 단계 단순화 + 단계적 최적화 원칙

### D-014 Analytics `salt` Secret Scope/Key 정의
- 상태: **결정됨(2026-02-06, dev 임시 기준)**
- 결정: `scope=ledger-analytics-dev`, `key=salt_user_key`
- 후속: 운영 재구축 시 Key Vault-backed Secret Scope로 전환
- 영향: `user_key` 익명화 구현 및 배포 환경 설정
- 근거: `.specs/cloud/phase7_cloud_setup_status.md`, `scripts/phase7/setup_minimal_cloud.sh`, `configs/dev.yaml`, `configs/common.yaml`

### D-015 로컬 더미 `salt` 값
- 상태: **결정됨(2026-02-06)**
- 결정: 로컬 더미 `salt` 상수는 `local-salt-v1`
- 영향: 로컬 테스트 재현성
- 근거: 재현 가능한 테스트 실행을 위한 고정 상수 정책

### D-016 `gold.fact_payment_anonymized` 멱등성/파티셔닝 전략
- 상태: **결정됨(2026-02-06, 개발단계 임시)**
- 결정: `date_kst` 단위 `overwrite partition` 전략을 사용하고, Pipeline C에서는 대상 파티션만 교체한다.
- 영향: 백필/재실행 시 동일 `date_kst` 범위에 대해 결과 수렴을 보장한다.
- 후속: 운영 데이터량/지연 요구가 커지면 `MERGE` 키 전략으로 재평가한다.
- 근거: `.specs/cloud/cloud_migration_rebuild_plan.md`, `.roadmap/implementation_roadmap.md` Phase 6

### D-017 개발 단계 보안 하드닝 유예 범위
- 상태: **결정됨(2026-02-06)**
- 결정: 개발/테스트 단계에서는 퍼블릭 엔드포인트 기반 최소 구성으로 진행하고, NSG/서브넷 분리/Private Endpoint/강화된 시크릿 경로는 **개발 완료 후 재구축 단계**에서 적용
- 영향: Phase 7의 보안 관련 산출물은 “최종 보안 구성”이 아닌 임시 구성이 될 수 있음
- 근거: 사용자 결정 및 `.specs/cloud/cloud_migration_rebuild_plan.md`

### D-018 서비스 프린시플 실행 주체 전환 시점
- 상태: **결정됨(2026-02-06)**
- 결정: 서비스 프린시플 기반 `run_as`/권한 전환은 보안 설정된 신규 리소스 준비 완료 후(Phase 11) 진행한다.
- 영향: Phase 8~10 구간은 사용자 principal 임시 운영을 유지하고, Secure Environment Rebuild 단계에서 전환을 수행한다.
- 근거: 사용자 결정 및 `.roadmap/implementation_roadmap.md` Phase 11 정책

### D-019 Analytics salt 해석 우선순위
- 상태: **결정됨(2026-02-06, 개발단계)**
- 결정: `ANON_USER_KEY_SALT` 환경변수 우선, 없으면 Secret Scope(`ledger-analytics-dev/salt_user_key`), 둘 다 없으면 로컬 더미 `local-salt-v1` 사용
- 영향: 로컬/Databricks 실행 모두에서 익명화 키 생성 동작을 일관화
- 근거: D-014, D-015 및 Phase 6 구현 정책

### D-020 `gold.pipeline_state` 실패 시 업데이트 규칙
- 상태: **결정됨(2026-02-06, 개발단계)**
- 결정: 성공 시 `last_success_ts`, `last_processed_end`, `last_run_id`를 모두 갱신하고, 실패 시 `last_success_ts`/`last_processed_end`는 유지하며 `last_run_id`, `updated_at`만 갱신
- 영향: 증분 재개 체크포인트는 마지막 성공 지점을 보존하고, 최근 실패 실행 ID는 추적 가능
- 근거: `.specs/project_specs.md` 9.1, Phase 8 구현 정책

### D-021 Azure 모니터링 목표/SLA
- 상태: **결정됨(2026-02-11)**
- 결정: 운영 안정성 우선으로 `P1=10분`, `P2=30분` 내 탐지를 목표로 한다.
- 영향: 초기 노이즈가 일부 증가할 수 있으나 운영 장애/데이터 품질 이상 조기 탐지가 가능하다.
- 근거: 사용자 결정(옵션 A)

### D-022 Azure 모니터링 데이터 소스 범위
- 상태: **결정됨(2026-02-11)**
- 결정:
  - 현재(v1): 운영 알림 소스는 Databricks 진단 로그 + Azure Activity Log로 고정한다.
  - 향후(v2): `gold.pipeline_state`, `silver.dq_status`, `gold.exception_ledger`를 테이블 기반 모니터링 신호로 확장한다.
- 영향:
  - 현재 운영 경보 범위를 명확히 유지하면서, 실행 실패와 데이터 품질 이상 분리 관측을 위한 확장 경로를 보존한다.
- 참조: D-037 (모니터링 SSOT 충돌 해소)
- 근거: 사용자 결정(옵션 A)

### D-023 모니터링 적용 환경 범위
- 상태: **결정됨(2026-02-11)**
- 결정: dev/prod 공통 프레임으로 운영하되, 임계치와 채널 강도는 환경별 차등 적용한다.
- 영향: dev에서 조기 검증이 가능하고 prod 도입 리스크를 낮춘다.
- 근거: 사용자 결정(옵션 A)

### D-024 초기 알림 규칙 세트
- 상태: **결정됨(2026-02-11)**
- 결정:
  - 현재(v1): 초기 알림은 실행 안정성 Core 4만 적용한다.
    1) Job 실패
    2) 최근 성공 지연(SLA breach)
    3) 재시도 소진
    4) 클러스터 시작 실패/타임아웃
  - 향후(v2): `DQ CRITICAL` 및 `SOURCE_STALE`/`EVENT_DROP_SUSPECTED` 지속 알림은 테이블 기반 신호 도입 시 확장한다.
- 영향:
  - 현재 운영 노이즈를 제한하면서 온콜 페이지 기준을 Core 4로 고정하고, 데이터 품질 경보 확장 여지를 유지한다.
- 참조: D-037 (모니터링 SSOT 충돌 해소)
- 근거: 사용자 결정(옵션 A)

### D-025 임계치 운영 방식
- 상태: **결정됨(2026-02-11)**
- 결정: 정적 임계치 + 환경별 차등(dev/prod) 전략으로 시작한다.
- 영향: 규칙 해석/감사가 단순해지고 초기 튜닝 리스크가 낮아진다.
- 재검토 트리거: 트래픽 변동으로 과경보가 지속되면 동적 임계치 도입을 검토한다.
- 근거: 사용자 결정(옵션 A)

### D-026 알림 억제 구현 위치
- 상태: **결정됨(2026-02-11)**
- 결정: 억제 정책(`SOURCE_STALE` 등)은 Azure Monitor 규칙에서 중앙 구현한다.
- 영향: 팀별 워크플로우가 늘어도 억제 정책의 일관성을 유지할 수 있다.
- 근거: 사용자 결정(옵션 A)

### D-027 채널/온콜 라우팅 구조
- 상태: **결정됨(2026-02-11)**
- 결정: Severity + Owner 기반 Action Group 분리 구조를 사용한다.
- 영향: 책임 경계를 명확히 하고 알림 소음을 줄인다.
- 근거: 사용자 결정(옵션 A)

### D-028 대시보드 표준 구조
- 상태: **결정됨(2026-02-11)**
- 결정: 통합 대시보드 1개(옵션 B)로 시작하되, 초기 범위는 Pipeline A/B/C만 포함한다.
- 영향: 빠른 가시화가 가능하며, 이후 팀원 워크플로우를 같은 대시보드에 확장한다.
- 근거: 사용자 결정(옵션 B + 초기 A/B/C 한정)

### D-029 상태 SSOT 전략
- 상태: **결정됨(2026-02-11)**
- 결정: 목적별 이중 SSOT를 사용한다.
  - 실행 상태: Databricks run/system tables
  - 데이터 상태: `gold.pipeline_state`, `silver.dq_status`, `gold.exception_ledger`
- 영향: 실행 실패와 데이터 품질 실패를 혼동 없이 분리 진단할 수 있다.
- 근거: 사용자 결정(옵션 A)

### D-030 모니터링 권한 모델(RBAC)
- 상태: **결정됨(2026-02-11)**
- 결정: 초기 단계에서 팀 공통 Contributor 모델을 사용한다.
- 영향: 설정/변경 속도는 빠르나, 권한 통제 강도는 낮아진다.
- 재검토 트리거: 운영팀/워크플로우 수 증가 시 역할 분리형 RBAC로 전환 검토.
- 근거: 사용자 결정(옵션 B)

### D-031 로그 보존/비용 정책
- 상태: **결정됨(2026-02-11)**
- 결정: 계층형 보존 + 고용량 로그 필터링 전략을 적용한다.
- 영향: 비용 예측성과 감사 대응력을 동시에 확보한다.
- 근거: 사용자 결정(옵션 A)

### D-032 모니터링 검증 방식
- 상태: **결정됨(2026-02-11)**
- 결정: 정기 검증(월 1회 game-day: 실패/지연/DQ 경보 시나리오)을 운영한다.
- 영향: 경보/런북 드리프트를 조기에 발견할 수 있다.
- 근거: 사용자 결정(옵션 A)

### D-033 `silver.bad_records` 영속화 방식
- 상태: **결정됨(2026-02-11)**
- 결정:
  1) `silver.bad_records` 단일 통합 테이블을 운영 산출물로 도입한다.
  2) 적재 정책은 append-only로 고정하고 파티션 키는 `detected_date_kst`를 사용한다.
  3) E2E setup 경로에서 **격리 저장 후 fail-fast** 순서를 고정한다.
  4) fail-fast 적용 대상은 `silver.wallet_snapshot`, `silver.ledger_entries`로 유지한다.
  5) `record_json`은 원본 전체를 저장하고, 보존 정책은 180일 + 월 1회 정리로 운영한다.
- 영향:
  - 계약 위반 row의 재처리/감사 추적성을 확보한다.
  - quarantine 원칙과 구현 상태를 일치시킨다.
- 근거:
  - 문서: `.specs/project_specs.md`, `.specs/data_contract.md`, `.specs/ops/operations_runbook.md`
  - 코드: `src/common/contracts.py`, `src/common/table_metadata.py`, `src/transforms/silver_controls.py`, `src/transforms/analytics.py`, `scripts/e2e/setup_e2e_env.py`
- 구현(2026-02-12, 운영 정리 자동화):
  1) 운영 정리 스크립트 `scripts/ops/cleanup_bad_records.py`를 추가했다.
  2) Databricks Workflow `bad_records_retention_cleanup`(월 1회, KST 00:50)를 `databricks.yml`에 추가했다.
  3) runbook 정리 절차를 scheduled job 기본 + 수동 SQL fallback으로 정렬했다.

### D-034 운영 부트스트랩(UC schema/table) 정책
- 상태: **결정됨**
- 결정:
  1) 운영에서는 `scripts/e2e/setup_e2e_env.py`를 사용하지 않는다.
  2) 운영 실행 전 `catalog.bronze/silver/gold` 스키마와 계약 테이블 존재를 사전 보장한다.
  3) 부트스트랩 범위는 `스키마+테이블 생성`으로 고정한다(데이터 적재/물질화 제외).
- 구현:
  1) 공유 모듈: `src/io/catalog_bootstrap.py`
  2) 운영 스크립트: `scripts/bootstrap_catalog.py` (`--catalog` 필수, `--dry-run` 지원)
  3) Databricks job: `bootstrap_catalog` (`databricks.yml`)
  4) cutover 절차에 bootstrap 단계 반영: `.specs/ops/cutover_preflight_exit_template.md`
- 영향:
  - 운영 시작 전 테이블 부재로 인한 첫 실행 실패 가능성을 낮춘다.
  - E2E 보조 스크립트 의존 없이 운영 경로를 분리할 기준을 확보한다.
- 근거:
  - 코드: `databricks.yml`, `src/io/catalog_bootstrap.py`, `scripts/bootstrap_catalog.py`, `src/common/contracts.py`
  - 문서: `.specs/ops/cutover_preflight_exit_template.md`

### D-035 Silver 운영 물질화 경로
- 상태: **결정됨(2026-02-11)**
- 배경:
  - Pipeline B/C는 `silver.*` 입력을 읽지만, 운영 `run_pipeline_*` 경로에는 Bronze->Silver 물질화 단계가 없다.
  - 현재 Silver 물질화는 E2E setup 경로(`scripts/e2e/setup_e2e_env.py`)에 사실상 집중되어 있다.
- 영향:
  - 운영에서 Silver 갱신이 보장되지 않으면 B/C 산출물 stale 또는 미생성 리스크가 발생한다.
- 결정:
  1) 전용 Silver job(`run_pipeline_silver`, workflow: `pipeline_silver_materialization`)을 도입한다.
  2) Pipeline B/C는 `pipeline_silver` 체크포인트(`gold.pipeline_state.last_processed_end`) 기준 fail-closed dependency를 적용한다.
  3) 윈도우 파라미터가 비어 있으면 `전일(KST) 1일 backfill`을 기본값으로 자동 해석한다.
  4) 운영 스케줄은 Silver 선행 버퍼를 고정한다.
     - Silver: `00:00 KST`
     - B: `00:20 KST`
     - C: `00:35 KST`
  5) `e2e_full_pipeline` task graph는 `sync_dim_rule_scd2 -> pipeline_a -> pipeline_silver -> pipeline_b/pipeline_c`로 고정한다.
  6) Silver Bronze-window 필터는 `timestamp_fields` 중 존재하는 컬럼 전체를 OR로 평가한다(첫 컬럼 단독 선택 금지).
  7) Pipeline B `--task all` 경로의 readiness 검증은 failure-state 기록 경계(try/except) 내부에서 수행해 실패 시 `gold.pipeline_state` 업데이트를 보장한다.
- D-040 연계:
  - Pipeline B `gold.exception_ledger` 멱등성 키
    `(date_kst, domain, exception_type, run_id, metric, message)`는 변경하지 않는다.
  - readiness 실패는 신규 예외 적재 확장 없이 즉시 실패로 처리한다.
- 근거:
  - 코드: `scripts/run_pipeline_silver.py`, `scripts/run_pipeline_b.py`, `scripts/run_pipeline_c.py`, `src/io/upstream_readiness.py`, `src/common/window_defaults.py`, `databricks.yml`
  - 문서: `.specs/project_specs.md`, `.specs/ops/operations_runbook.md`, `.specs/ops/cutover_preflight_exit_template.md`

### D-036 룰 SSOT 전환(`mock seed` -> `gold.dim_rule_scd2`)
- 상태: **결정됨(2026-02-11)**
- 배경:
  - 런타임 룰 로딩이 `mock_data/fixtures/dim_rule_scd2.json` 기반으로 고정되어 있다.
  - 운영 문서는 `gold.dim_rule_scd2` 점검/사용을 전제로 서술되어 있다.
- 영향:
  - 룰 변경이 코드 배포/파일 교체에 묶여 운영 민첩성과 감사 일관성이 낮아진다.
- 결정:
  1) 운영 룰 SSOT는 `gold.dim_rule_scd2`로 고정한다.
  2) 룰 로딩 정책은 `run_pipeline_a/b/silver --rule-load-mode`로 제어한다.
     - `prod`: `strict`(table only, fail-closed)
     - `dev/test`: `fallback`(table 우선, 실패 시 seed fallback)
  3) 룰 거버넌스(승인/버전/유효기간) SSOT는 `.specs/ops/operations_runbook.md`로 고정한다.
  4) 룰 반영 경로는 전용 sync job(`scripts/sync_dim_rule_scd2.py`, `databricks.yml:sync_dim_rule_scd2`)으로 운영한다.
- 구현:
  1) 계약/메타데이터: `gold.dim_rule_scd2` 추가
  2) 룰 로더: `load_rule_table`, `load_runtime_rules` 추가
  3) 런타임: `scripts/run_pipeline_a.py`, `scripts/run_pipeline_b.py`, `scripts/run_pipeline_silver.py`에 rule load 파라미터 추가
  4) E2E setup: `gold.dim_rule_scd2` seed sync 후 table 경유 로딩
- 근거:
  - 코드: `src/io/rule_loader.py`, `scripts/run_pipeline_a.py`, `scripts/run_pipeline_b.py`, `scripts/run_pipeline_silver.py`, `scripts/sync_dim_rule_scd2.py`, `databricks.yml`
  - 문서: `.specs/project_specs.md`, `.specs/data_contract.md`, `.specs/ops/operations_runbook.md`
- 구현 보강(2026-02-12, prod fail-closed 강제):
  1) `src/common/rule_mode_guard.py`를 추가했다.
  2) `run_pipeline_a.py`, `run_pipeline_b.py`, `run_pipeline_silver.py`에서 prod 문맥 + `rule_load_mode != strict`를 즉시 차단한다.
  3) prod 판별 우선순위를 `PIPELINE_ENV=prod -> catalog=prod_catalog -> configs/prod.yaml catalog 일치`로 고정했다.

### D-037 모니터링 SSOT 충돌 해소
- 상태: **결정됨(2026-02-11)**
- 배경:
  - 결정 로그(D-022, D-024)는 테이블 기반 모니터링 신호 포함으로 기록되어 있다.
  - 운영/모니터링 문서는 현재 v1 범위에서 테이블 기반 알림을 Out-of-Scope로 명시한다.
- 영향:
  - 운영팀이 실제 경보 범위를 오해할 수 있고, 온콜 대응 기준이 문서마다 달라질 수 있다.
- 결정:
  1) Current = v1(Log Analytics only)로 확정한다.
  2) Future = v2(테이블 기반 알림 확장)로 분리한다.
  3) 모니터링 최상위 SSOT는 `.specs/ops/azure_monitoring_integration_plan.md`로 고정한다.
- 후속 정렬:
  - D-022, D-024는 `현재(v1)/향후(v2)` 구조로 재정의해 이력을 보존한다.
  - `.specs/ops/operations_runbook.md`, `.specs/project_specs.md`, `.specs/ops/cutover_preflight_exit_template.md`는 v1 기준 문구로 정렬한다.
- 근거:
  - 문서: `.specs/decision_open_items.md`, `.specs/ops/azure_monitoring_integration_plan.md`, `.specs/ops/operations_runbook.md`

### D-038 대용량 처리 전략(`collect()` 제거)
- 상태: **결정됨(2026-02-12)**
- 배경:
  - A/B/C 런타임 경로에 driver `collect()` 사용이 다수 존재한다.
  - 데이터 규모 증가 시 driver 메모리 병목/실패 가능성이 높다.
- 영향:
  - 실운영 확장 시 배치 실패율 상승 및 실행 시간 변동성 확대 리스크가 있다.
- 결정:
  1) `legacy + spark` 병행 전환 후 최종 PR에서 legacy 경로를 제거한다.
  2) PR0~PR4 동안 `--engine-mode legacy|spark`로 배포 안전장치를 유지하고, 정리 PR에서 spark-only로 단순화한다.
  3) `collect()` 정책은 `unbounded 금지 / bounded 허용`으로 고정하고 허용 경로는 `safe_collect(max_rows=...)`로 제한한다.
  4) 전환 우선순위는 `PR1(Silver) -> PR2(B) -> PR3(C) -> PR4(A) -> PR5(정리)`로 고정한다.
  5) 완료 기준은 기능 parity, 멱등성, L3 검증, 문서/증적 업데이트까지 포함한다.
- 구현(2026-02-12, PR5):
  1) `scripts/run_pipeline_a.py`, `scripts/run_pipeline_b.py`, `scripts/run_pipeline_c.py`, `scripts/run_pipeline_silver.py`를 spark-only로 정리했다.
  2) `databricks.yml`에서 `engine_mode` 변수/잡 파라미터/CLI 전달 인자를 제거했다.
  3) parity 통합 테스트를 spark 기대값 검증 테스트로 교체했다.
- 근거:
  - 코드: `scripts/run_pipeline_a.py`, `scripts/run_pipeline_b.py`, `scripts/run_pipeline_c.py`
  - 문서: `.specs/d038_collect_removal_execution_plan.md`, `.specs/ops/performance_partitioning_checklist.md`

### D-039 설정 SSOT 연결 + 하드코딩 제거
- 상태: **결정됨(2026-02-12)**
- 배경:
  - `configs/common.yaml`, `configs/dev.yaml`, `configs/prod.yaml`가 존재하지만 런타임 스크립트가 직접 소비하지 않았다.
  - 런타임 스크립트에 catalog(`2dt_final_team4_databricks_test`), secret_scope, secret_key가 하드코딩되어 있었다.
- 영향:
  - 환경 전환(dev/prod) 시 설정 드리프트와 배포 실수 가능성이 증가한다.
- 결정:
  1) `configs/*.yaml`을 런타임 SSOT로 승격한다. `src/common/config_loader.py`가 이를 로딩한다.
  2) `databricks.yml`은 job parameter 전달 역할을 유지한다(CLI args > env vars > configs/{env}.yaml > configs/common.yaml).
  3) 하드코딩 제거 대상: `--catalog` 기본값(A/B/C/Silver 4개 스크립트), `--secret-scope`/`--secret-key`(Pipeline C), `DEFAULT_SECRET_SCOPE`/`DEFAULT_SECRET_KEY`(`secret_loader.py`).
  4) `_default_repo_root()` 중복 함수(4개 스크립트 동일)를 `config_loader.find_repo_root()`로 통합한다.
- 구현:
  - 신규: `src/common/config_loader.py` (load_config, get_config_value, find_repo_root)
  - 수정: `scripts/run_pipeline_a.py`, `scripts/run_pipeline_b.py`, `scripts/run_pipeline_c.py`, `scripts/run_pipeline_silver.py`, `src/io/secret_loader.py`
  - 보강: `configs/common.yaml`에 `databricks.catalog: null` 추가
  - 의존성: `requirements-dev.txt`에 `pyyaml` 추가
  - 테스트: `tests/unit/test_config_loader.py` (13 cases)
- 근거:
  - 코드: `src/common/config_loader.py`, `configs/common.yaml`, `configs/dev.yaml`, `configs/prod.yaml`
  - 문서: `.specs/cloud/cloud_migration_rebuild_plan.md`
- 구현 보강(2026-02-12, env override 레이어 연결):
  1) `PIPELINE_CFG__<NESTED__KEY>` 형식 env override를 `config_loader`에 추가했다.
  2) 적용 우선순위를 `CLI args > env vars > configs/{env}.yaml > configs/common.yaml`로 코드에 반영했다.
  3) unknown key override는 fail-fast(`KeyError`), 값 파싱은 `true/false/null/숫자` 강제 변환으로 고정했다.
  4) `src/io/secret_loader.py`의 secret scope/key 기본값을 설정 기반으로만 해석하도록 정리했다.

### D-040 `gold.exception_ledger` 멱등성 충돌(재실행 시 MERGE 실패)
- 상태: **결정됨(2026-02-11)**
- 배경:
  - D-036 L3 재검증 중 Pipeline B 재실행에서 `pipeline_b_recon`이 실패했다.
  - 오류: `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE` (SQLSTATE 21506)
  - 기존 `gold.exception_ledger` MERGE 키 `(date_kst, domain, exception_type, run_id)`는 동일 run 내 다건 예외를 구분하지 못했다.
- 영향:
  - 같은 입력/같은 `run_id` 재실행 시 Pipeline B 멱등성 검증이 실패한다.
  - 당시 L3 acceptance를 통과하지 못해 D-036 운영 전환 검증이 블로킹되었다.
- 결정:
  1) 예외 적재 단위는 행 단위를 유지한다(집계 전환하지 않음).
  2) `gold.exception_ledger` MERGE 키를 `(date_kst, domain, exception_type, run_id, metric, message)`로 확장한다.
  3) 동일 `run_id` 재실행을 허용하고, 동일 입력 기준 결과 수렴을 L3 idempotency 기준으로 고정한다.
- 변경 유형:
  - Major(키 변경): `.specs/schema/schema_migration_policy.md`의 변경 분류 기준을 따른다.
- 재검토 트리거:
  - `message` 직렬화 규칙이 바뀌어 key 안정성이 깨지거나, 동일 6-key 중복 source가 반복 관측되면 별도 stable key 컬럼 도입을 재검토한다.
- 근거:
  - 로그: `.agents/logs/verification/2026-02-11_d036_l3_timeout_and_failure.md`
  - 로그: `.agents/logs/verification/L3_d036_resume_20260211T104159Z.log`
  - 코드: `src/common/table_metadata.py`, `scripts/run_pipeline_b.py`, `src/transforms/ledger_controls.py`, `src/transforms/dq_guardrail.py`
  - 테스트: `tests/unit/test_table_metadata.py`, `tests/integration/test_backfill_idempotency.py`

### D-041 Pipeline C Spark category tie-break/조인 정책
- 상태: **결정됨(2026-02-12)**
- 결정:
  1) `gold.fact_payment_anonymized.category` 파생 조인키는 `order_ref`만 사용한다. (`order_id` fallback 금지)
  2) 대표 카테고리 선정 우선순위는 D-011을 유지한다. (`line_amount DESC`, 동률 시 `item_id` 최소)
  3) dirty data(`item_id` 동시 NULL 동률)에서는 재실행 결정성을 위해 `product_id`, `category` 오름차순을 추가 tie-break로 사용한다.
- 영향:
  - 계약 준수 데이터에서는 legacy parity를 유지한다.
  - 계약 위반/dirty 데이터에서도 Spark 경로 결과가 멱등적으로 고정된다.
- 근거:
  - 코드: `src/jobs/pipeline_c_spark.py`, `tests/integration/test_pipeline_c_spark.py`
  - 문서: D-011, D-038

### D-042 Pipeline A 빈 윈도우 파라미터 자동 해석
- 상태: **결정됨(2026-02-12)**
- 배경:
  - `pipeline_a_guardrail`는 10분 스케줄이지만 기본 파라미터는 `run_mode=incremental` + `start_ts/end_ts` 공백으로 전달될 수 있다.
  - `JobParams` 규칙상 incremental은 `start_ts/end_ts`가 필수라 런타임 시작 시점 실패가 발생할 수 있다.
- 결정:
  1) Pipeline A에 빈 윈도우 자동 해석을 도입한다.
  2) 적용 조건: `run_mode`가 공백/`incremental`이고 `start_ts/end_ts/date_kst_start/date_kst_end`가 모두 공백인 경우.
  3) 해석 규칙:
     - `end_ts = now_utc`
     - `start_ts = gold.pipeline_state(pipeline_a).last_processed_end` (존재 시)
     - 상태가 없거나 역전(`start_ts >= end_ts`)이면 `start_ts = end_ts - 10분`
  4) `run_mode`는 `incremental`로 고정한다.
- 영향:
  - Pipeline A 스케줄 실행과 파라미터 검증 규칙의 충돌을 제거한다.
  - 체크포인트가 존재하면 연속성 기반 증분 처리, 없으면 최근 10분 기본 윈도우로 안전 시작한다.
- 근거:
  - 코드: `scripts/run_pipeline_a.py`, `src/common/window_defaults.py`
  - 테스트: `tests/unit/test_window_defaults.py`
