# Local scripts

## Backfill (JSONL)

Use JSON lines dumps and optional time window filtering.

```
python -m src.consumer.main backfill \
  --ledger-file /path/to/ledger.jsonl \
  --payment-order-file /path/to/payment_orders.jsonl \
  --since 2026-02-01T00:00:00Z \
  --until 2026-02-05T00:00:00Z
```

## Publish sample events

```
python scripts/publish_sample_events.py
```

## Publish synthetic scenario events

```
python scripts/publish_synthetic_events.py --scenario happy --run-id smoke-001
python scripts/publish_synthetic_events.py --scenario duplicate --run-id smoke-dup-001
python scripts/publish_synthetic_events.py --scenario out_of_order --run-id smoke-ooo-001
python scripts/publish_synthetic_events.py --scenario error --run-id smoke-err-001
```

## Replay DLQ file

```
python scripts/replay_dlq.py
```

## Admin API smoke test

```
python scripts/smoke_admin_tx.py
```

## Cloud Phase 8 automation

```
scripts/cloud/phase8/run_all.sh
```

Details: `scripts/cloud/phase8/README.md`
