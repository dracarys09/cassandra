# CASSANDRA-19130: Transactional TRUNCATE via TCM

## Context

**Problem**: The current `TRUNCATE` implementation (`StorageProxy.truncateBlocking()`) requires ALL token-owning nodes to be up. If any node is unreachable, truncate fails with `UnavailableException`. This is overly restrictive for an admin operation.

**Root cause**: Truncate works by sending `TRUNCATE_REQ` to every node and waiting for all responses. There's no way for a node that misses the message to catch up later.

**Solution**: Implement TRUNCATE as a TCM (Transactional Cluster Metadata) transformation that atomically drops and recreates the table with a new `TableId` in a single epoch. TCM's log replication ensures all nodes eventually see the change, even if they're temporarily down.

**Why DROP+CREATE works**: When a table is dropped and recreated, it gets an entirely new `TableId` (epoch-based). Old SSTables reference the old `TableId` and become orphaned. The epoch-based coordination ensures all coordinators/replicas have a consistent view of which `TableId` the table refers to before serving any reads or writes.

**Key JIRA consensus** (Sam Tunnicliffe, Abe Ratnofsky, Stefan Miklosovic):
- TRUNCATE should be a **single `SchemaTransformation`** executed atomically in one epoch (Abe, Jul 2025)
- **Prohibit truncations in mixed-version clusters** — consistent with how schema/topology changes are already handled (Sam, Jul 2024)
- Virtual tables excluded from transactional truncation (they're local-only)
- The `execute()` method must be **side-effect free** — local cleanup happens via post-commit listeners (Sam, Mar 2024)
- Suppress driver `DROPPED`/`CREATED` event notifications so clients don't see DROP+CREATE for a TRUNCATE (Abe, Jul 2025)
- NOTE: `SystemKeyspace.getTruncatedAt` is NOT used on the read path (Abe's finding, confirmed by Sam) — the truncation record only matters for commit log replay

---

## Phase 1: Infrastructure

### 1.1 Add serialization version
- **File**: `src/java/org/apache/cassandra/tcm/serialization/Version.java`
- Add new version constant (e.g., `V9`) for the transactional truncate feature
- This gates the transformation so mixed-version clusters cannot commit it

### 1.2 Add Transformation.Kind
- **File**: `src/java/org/apache/cassandra/tcm/Transformation.java`
- Add to `Kind` enum:
  - `TRUNCATE_TABLE` — single-step atomic truncation (non-Accord tables)
  - `PREPARE_TRUNCATE_ACCORD_TABLE` — first step for Accord tables
  - `FINISH_TRUNCATE_ACCORD_TABLE` — second step for Accord tables

---

## Phase 2: Core Transformation — `TruncateTable`

### 2.1 New file: `src/java/org/apache/cassandra/tcm/transformations/TruncateTable.java`

**Implements**: `Transformation` (modeled on `AlterSchema` pattern)

**Fields**: `String keyspace`, `String table`, `TableId currentTableId` (for conflict detection)

**`execute(ClusterMetadata prev)` logic**:
1. Validate table exists, `currentTableId` matches (reject if concurrently modified), not a view, not virtual, not an Accord table (those use multi-step path), not `pendingDrop`/`pendingTruncate`
2. Get the `KeyspaceMetadata`
3. Generate new `TableId` — use `TableId.get(prev)` (epoch-based, guaranteed unique)
4. Build new `TableMetadata`: same name, columns, params, indexes, triggers — but new `TableId` and new epoch
5. For each materialized view on the table: regenerate its `TableId` and update `baseTableId` to point to the new base table
6. Update the `DistributedSchema`: remove old table/views, add new table/views
7. Return `Transformation.success(transformer, AffectedRanges.EMPTY)`

**Key**: This is a pure function — no side effects. The actual SSTables cleanup happens via the existing `SchemaListener` infrastructure which sees the old table as "dropped" and the new table as "created" in the keyspace diff.

### 2.2 Serializer
- Inner `Serializer` class implementing `AsymmetricMetadataSerializer<Transformation, TruncateTable>`
- Serialize/deserialize keyspace, table, currentTableId

---

## Phase 3: Accord Table Handling (Multi-Step)

For tables with `requiresAccordSupport()`, in-flight Accord transactions must complete before truncation. This follows the `DropAccordTable` pattern exactly.

### 3.1 `src/java/org/apache/cassandra/tcm/transformations/PrepareTruncateAccordTable.java` (NEW)
- Sets `pendingTruncate = true` on table params (blocks new Accord transactions)
- Creates a `TruncateAccordTable` sequence in `inProgressSequences`

### 3.2 `src/java/org/apache/cassandra/tcm/transformations/FinishTruncateAccordTable.java` (NEW)
- Performs the same ID-swap logic as `TruncateTable`
- Removes the sequence from `inProgressSequences`

### 3.3 `src/java/org/apache/cassandra/tcm/sequences/TruncateAccordTable.java` (NEW)
- Extends `MultiStepOperation<Epoch>` (modeled on `DropAccordTable`)
- `executeNext()`: waits for Accord to finish in-flight transactions, then commits `FinishTruncateAccordTable`

### 3.4 `src/java/org/apache/cassandra/schema/TableParams.java`
- Add `boolean pendingTruncate` field (analogous to existing `pendingDrop`)
- Update builder, serialization (gated on new version)

### 3.5 `src/java/org/apache/cassandra/tcm/MultiStepOperation.java`
- Add `TRUNCATE_ACCORD_TABLE` to `Kind` enum

---

## Phase 4: Statement Integration

### 4.1 `src/java/org/apache/cassandra/cql3/statements/TruncateStatement.java`

Replace `StorageProxy.truncateBlocking(keyspace(), name())` with:
```
if (metaData.isVirtual()) {
    executeForVirtualTable(metaData.id);  // unchanged
} else if (metaData.requiresAccordSupport()) {
    commit PrepareTruncateAccordTable → await sequence completion
} else {
    commit TruncateTable(keyspace, table, metaData.id)
}
```

Also update `executeLocally()` to use the same transformation path (or keep as-is for internal/tool use).

### 4.2 Suppress driver notifications
- In the schema event notification path, detect that this is a TRUNCATE (not a real DROP+CREATE) and suppress `SchemaChange` events
- One approach: add a flag or metadata to the transformation that the event notification code can check
- Investigate: `src/java/org/apache/cassandra/schema/DistributedSchema.java` — look at how `initializeKeyspaceInstances()` handles the diff and where driver events are fired

---

## Phase 5: Local Cleanup & Snapshot Handling

### 5.1 How local cleanup works (existing infrastructure)

When `SchemaListener.notifyPreCommit()` processes the schema diff:
- `Tables.diff()` matches by `TableId` — old table appears as "dropped", new as "created"
- `dropTable()` → `Keyspace.dropCf(oldTableId)` → `cfs.invalidate()` → drops SSTables
- `createTable()` → `Keyspace.initCf(newMetadata)` → creates fresh empty CFS

**No custom listener needed for the core cleanup** — it piggybacks on existing schema change infrastructure.

### 5.2 Snapshot handling
- Current truncate creates a snapshot before deleting data (if `auto_snapshot` is enabled)
- With DROP+CREATE, we need to ensure the snapshot is taken during the "drop" phase
- Investigate: does `Keyspace.dropCf()` / `cfs.invalidate()` already handle auto_snapshot? If not, add snapshot logic in the drop path when the cause is a TRUNCATE transformation

### 5.3 Commit log replay safety
- Old `TableId` mutations in commit log: `CommitLogReplayer` uses `Schema.instance.getColumnFamilyStoreInstance(tableId)` which returns `null` for the old ID → safely skipped
- New `TableId` mutations: replayed into the fresh CFS
- No special handling needed

---

## Phase 6: Concurrent Write Visibility & Epoch Ordering

### The concern (Sam's "gap")
A client issues TRUNCATE → coordinator A commits at epoch E → success returned. Client immediately sends a WRITE to coordinator B (still at epoch E-1). B routes it with old schema → write lands on old CFS → B catches up to epoch E → old CFS dropped → write is LOST.

### Why this is acceptable / how TCM handles it

This is **identical behavior to any schema change** in TCM (DROP TABLE, ALTER TABLE, etc.). The epoch provides a total order:
- Writes coordinated at epoch < E are **before** the truncation → correctly lost
- Writes coordinated at epoch >= E are **after** the truncation → correctly survive

### Mitigation mechanisms (already built into TCM)

1. **Schema agreement on coordinators**: When a coordinator processes epoch E, it knows the new `TableId`. All subsequent writes target the new table.
2. **`waitForEpoch` on replicas**: Write messages carry the coordinator's epoch. Replicas check they're at least at that epoch before applying. If a replica is ahead of the coordinator (has seen TRUNCATE), it would reject writes for the old TableId (table not found).
3. **Driver schema agreement**: The TRUNCATE response can trigger schema-agreement waiting on the driver side (same as CREATE/DROP). The driver waits until all known nodes have the same schema version before proceeding.
4. **Suppressing driver events**: We suppress the DROP+CREATE notification but still return a `SCHEMA_CHANGE` result so drivers know to wait for agreement.

### What we DON'T need to solve
- Current TRUNCATE already has undefined semantics for concurrent writes ("depends on when the flush was executed" — Abe)
- The TCM approach is actually BETTER than the status quo because the epoch gives a clear ordering boundary
- Users running concurrent mutations and truncations get best-effort semantics (same as today)

### Test for this scenario
Add `testConcurrentWriteDuringTruncatePropagation` to the dtest suite:
- 3 nodes, inject message filter to delay TCM log delivery to node 2
- TRUNCATE on node 1 (commits at epoch E)
- Immediately write via node 2 coordinator (still at epoch E-1)
- Verify: the write is lost (it was "before" the truncation in epoch ordering)
- Then: write via node 2 AFTER it catches up to epoch E
- Verify: this write survives

---

## Phase 7: Other Edge Cases to Handle

1. **Prepared statements**: Resolve tables by name at execution time, not by cached `TableId` — should work without changes. Verify.
2. **In-flight repairs/streaming**: Old `TableId` streams should be rejected after truncation epoch. The epoch coordination ensures this. Verify that active repairs are aborted.
3. **Secondary indexes**: Dropped with the old CFS via `indexManager.dropAllIndexes()`, recreated empty with the new CFS. Verify.
4. **SSTable directory structure**: Old dir `<tablename>-<oldTableId>` cleaned up by `invalidate()`. New dir created by `initCf()`. Verify.
5. **Concurrent truncations of the same table**: `currentTableId` conflict detection prevents races — second truncation would see a different `TableId` and fail with `Rejected`.

---

## Phase 8: Testing Strategy

### 7.1 Correctness Invariants

| ID | Invariant |
|----|-----------|
| INV-1 | After truncation epoch E is enacted on a node, no read returns data written before E |
| INV-2 | After truncation, table schema (columns, types, indexes, params) is identical to before |
| INV-3 | New `TableId` is globally unique and never collides with any existing table |
| INV-4 | Views on the truncated table are also emptied and their schema preserved |
| INV-5 | A node that was down during truncation applies it correctly when it catches up |
| INV-6 | Commit log replay after truncation never resurfaces old data |
| INV-7 | Writes that complete after the truncation epoch target the new table and survive |

### 7.2 Liveness Properties

| ID | Property |
|----|----------|
| LIV-1 | TRUNCATE completes even with minority of nodes down (key improvement) |
| LIV-2 | For Accord tables, TRUNCATE eventually completes after in-flight transactions finish |
| LIV-3 | No deadlock between concurrent truncations of different tables |

### 7.3 Unit Tests

**File**: `test/unit/org/apache/cassandra/tcm/transformations/TruncateTableTest.java`

| Test | Validates |
|------|-----------|
| `testNewTableIdGenerated` | Resulting metadata has different `TableId`, same name/schema (INV-2, INV-3) |
| `testSchemaPreservedExactly` | All columns, types, indexes, triggers, params identical (INV-2) |
| `testViewsUpdatedCorrectly` | Views get new IDs, point to new base table (INV-4) |
| `testRejectsNonExistentTable` | Returns `Rejected` for missing table |
| `testRejectsView` | Returns `Rejected` for materialized view |
| `testRejectsAccordTable` | Returns `Rejected` — Accord tables use multi-step path |
| `testRejectsOnTableIdMismatch` | Conflict detection works (concurrent modification) |
| `testSerialization` | Serialize + deserialize roundtrip |
| `testMixedVersionRejection` | `eligibleToCommit()` returns false when version < V9 |

**File**: `test/unit/org/apache/cassandra/tcm/sequences/TruncateAccordTableTest.java`
- Modeled on `DropAccordTableTest`
- E2E prepare → finish sequence, `pendingTruncate` flag behavior

### 7.4 In-JVM Distributed Tests (dtests)

**File**: `test/distributed/org/apache/cassandra/distributed/test/TransactionalTruncateTest.java`

#### Critical scenarios (must-have)

| Test | Setup | Validates |
|------|-------|-----------|
| `testMixedVersionClusterRejectsTruncate` | Cluster with mixed serialization versions (some nodes < V9) | TRUNCATE is **rejected** — `eligibleToCommit()` returns false. Verify error message is clear. This ensures safety during rolling upgrades. |
| `testNoDataResurrectionAfterRestart` | Insert data, TRUNCATE, hard-kill node (no flush), restart | No ghost data from commit log replay (INV-6). Old `TableId` mutations in commitlog are skipped because no CFS exists for that ID. |
| `testNoDataResurrectionAfterNodeCatchup` | 3 nodes, stop node 3, insert data on nodes 1+2, TRUNCATE on node 1, restart node 3 | Node 3 catches up to truncation epoch, old data is gone, table is empty (INV-5) |
| `testTruncateWithNodeDown` | 3 nodes, stop 1 node, TRUNCATE | **Succeeds** — this is the key improvement over current behavior (LIV-1) |
| `testGuardrailBlocksTruncate` | Set `drop_truncate_table_enabled: false` guardrail | TRUNCATE is rejected with appropriate error |
| `testSnapshotCreatedOnTruncate` | `auto_snapshot=true`, insert data, TRUNCATE | Snapshot directory exists with pre-truncate data |
| `testSnapshotNotCreatedWhenDisabled` | `auto_snapshot=false`, insert data, TRUNCATE | No snapshot created |
| `testTruncateNonExistentTableThrows` | TRUNCATE a table that doesn't exist | Returns `InvalidRequestException` — matches current behavior |

#### Additional coverage

| Test | Setup | Validates |
|------|-------|-----------|
| `testBasicTruncate` | 3 nodes up, insert data, TRUNCATE | SELECT returns 0 rows on all nodes (INV-1) |
| `testDataAfterTruncateSurvives` | Insert, TRUNCATE, insert more, read | Post-truncate data present (INV-7) |
| `testSchemaPreservedAfterTruncate` | Complex schema (indexes, UDTs), TRUNCATE | Schema identical, indexes work (INV-2) |
| `testTruncateWithViews` | Table with MV, TRUNCATE | Both base and MV empty (INV-4) |
| `testMultipleTruncatesInSequence` | Truncate 5x with writes between | Each clears prior data, final writes survive |
| `testTruncateDoesNotAffectOtherTables` | Two tables, truncate one | Other table's data intact |
| `testConcurrentWritesDuringTruncate` | Writers + TRUNCATE | Writes before epoch gone, writes after present (INV-1, INV-7) |
| `testTruncateAccordTable` | Accord table, concurrent txns + TRUNCATE | In-flight txns complete first (LIV-2) |
| `testPreparedStatementAfterTruncate` | Prepare INSERT, truncate, execute prepared | Works correctly with new table |

### 7.5 Simulation Tests

**File**: `test/simulator/test/org/apache/cassandra/simulator/test/TruncateSimulationTest.java`

| Property | Description |
|----------|-------------|
| **Linearizability** | Under adversarial message ordering: writes ack'd before truncation epoch are gone; writes ack'd after survive |
| **No ghost resurrection** | With delayed message delivery, old SSTables cannot serve data after truncation epoch |
| **Epoch ordering** | Any read/write that observes the truncation epoch interacts with the new (empty) table |
| **Concurrent truncate + writes** | No inconsistency under any interleaving of truncation commit and mutations |

---

## Phase 9: Files Summary

### New files
- `src/java/org/apache/cassandra/tcm/transformations/TruncateTable.java`
- `src/java/org/apache/cassandra/tcm/transformations/PrepareTruncateAccordTable.java`
- `src/java/org/apache/cassandra/tcm/transformations/FinishTruncateAccordTable.java`
- `src/java/org/apache/cassandra/tcm/sequences/TruncateAccordTable.java`
- `test/unit/org/apache/cassandra/tcm/transformations/TruncateTableTest.java`
- `test/unit/org/apache/cassandra/tcm/sequences/TruncateAccordTableTest.java`
- `test/distributed/org/apache/cassandra/distributed/test/TransactionalTruncateTest.java`
- `test/simulator/test/org/apache/cassandra/simulator/test/TruncateSimulationTest.java`

### Modified files
- `src/java/org/apache/cassandra/tcm/Transformation.java` — add Kind entries
- `src/java/org/apache/cassandra/tcm/serialization/Version.java` — add version
- `src/java/org/apache/cassandra/tcm/MultiStepOperation.java` — add sequence Kind
- `src/java/org/apache/cassandra/schema/TableParams.java` — add `pendingTruncate`
- `src/java/org/apache/cassandra/cql3/statements/TruncateStatement.java` — use TCM commit
- `src/java/org/apache/cassandra/schema/DistributedSchema.java` — snapshot handling, suppress events

### Reference files (patterns to follow)
- `src/java/org/apache/cassandra/tcm/transformations/AlterSchema.java` — schema transformation pattern
- `src/java/org/apache/cassandra/tcm/transformations/PrepareDropAccordTable.java` — Accord multi-step pattern
- `src/java/org/apache/cassandra/tcm/sequences/DropAccordTable.java` — Accord sequence pattern

---

## Implementation Order

```
Phase 1 (Version, Kind, TableParams)  ← no dependencies
    ↓
Phase 2 (TruncateTable transformation) ← core logic
    ↓
Phase 3 (Accord multi-step)            ← depends on Phase 2
    ↓
Phase 4 (TruncateStatement changes)    ← depends on Phases 2 & 3
    ↓
Phase 5 (Snapshot, cleanup)            ← depends on Phase 2
    ↓
Phase 7 (Tests)                        ← depends on all above
```

## Verification

1. Run unit tests: `ant test -Dtest.name=TruncateTableTest`
2. Run dtests: `ant test-jvm-dtest-some -Dtest.name=TransactionalTruncateTest`
3. Run simulator: `ant test-simulator-some -Dtest.name=TruncateSimulationTest`
4. Manual verification: start 3-node cluster via dtest harness, stop a node, issue TRUNCATE, verify it succeeds
