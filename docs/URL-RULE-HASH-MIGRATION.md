# Repair existing URL OR-list rules

After deploying PR #8, run this **once** to fix URL hashes in rules saved before the fix. The command updates only affected rules; IDs, filters, tags, and timestamps are preserved. It does not replay Kafka messages that were already missed.

**Before production:** test on staging, choose a low-traffic period, and avoid adding/editing/deleting rules while running the migration. Kafka processing can continue. Keep the backup **outside Kubernetes**: it contains rule queries and metadata.

## Run in Kubernetes

Set your namespace and UI pod name (`kubectl get pods` shows available pods):

```sh
NS=manticore-streams
kubectl -n "$NS" get pods
POD='<ui-pod-name>'
PLAN=/tmp/url-rule-repair.jsonl
```

**1. Preview changes (no writes):**

```sh
kubectl -n "$NS" exec "$POD" -c manticoresearch-ui -- \
  php artisan url-rules:repair --all --plan="$PLAN"
```

For a large installation, prefer `--stream=123` instead of `--all` and use a separate plan for each stream. Plan files must not already exist.

**2. Save the plan/backup to your computer before applying:**

```sh
umask 077
kubectl -n "$NS" exec "$POD" -c manticoresearch-ui -- \
  cat "$PLAN" > url-rule-repair-backup.jsonl

# Rules requiring manual review (these are NOT fixed automatically):
jq -r 'select(.type == "review") | "stream=\(.stream) id=\(.id) \(.reason)"' \
  url-rule-repair-backup.jsonl
```

Keep this file securely; it is also used for rollback. Review any reported exceptions.

**3. Apply to 10 rules first, check matching, then finish:**

```sh
kubectl -n "$NS" exec "$POD" -c manticoresearch-ui -- \
  php artisan url-rules:repair --plan="$PLAN" --apply --max-updates=10 --delay-ms=25

# After checking the canary results:
kubectl -n "$NS" exec "$POD" -c manticoresearch-ui -- \
  php artisan url-rules:repair --plan="$PLAN" --apply --delay-ms=25
```

The same plan can be rerun safely after interruption; already repaired rules are skipped. Stop and investigate any reported conflict. Verify that rule counts are unchanged and test representative rules with `CALL PQ` after applying.

## Roll back if necessary

```sh
kubectl -n "$NS" exec "$POD" -c manticoresearch-ui -- \
  php artisan url-rules:repair --plan="$PLAN" --rollback --delay-ms=25
```

Rollback restores the **old, potentially broken hashes** and will not overwrite rules modified afterward. If the pod was replaced, first copy the saved JSONL file back to the new pod at the same `PLAN` path:

```sh
kubectl -n "$NS" exec -i "$POD" -c manticoresearch-ui -- \
  sh -c 'umask 077; cat > /tmp/url-rule-repair.jsonl' < url-rule-repair-backup.jsonl
```

The migration only fixes future matching. Handle `review` entries separately; they are not migrated automatically.
