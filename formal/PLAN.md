# Plan: formally modeling `dgen_registry_member`'s replication

Status: **plan only** — nothing below is implemented yet. This document is a
complete implementation guide: it contains the full TLA+ specification, all
model-checker configs, the runner script, the CI workflow, and a step-by-step
verification procedure. An implementer should be able to follow it top to
bottom without re-deriving any design decision.

> **Caveat for the implementer.** The spec in §5 was written without a local
> TLC run (the authoring sandbox could not download `tla2tools.jar` — see
> §10 step 1). It is designed to be correct, but expect to fix a handful of
> TLA+ syntax/type errors on the first TLC runs. §11 lists the likely
> pitfalls. Do not merge until the procedure in §10 passes end to end.

---

## 1. Recommendation: TLA+, checked with TLC

The bespoke replication in `src/dgen_registry_member.erl` (design:
`docs/design/dgen_registry_design.md`, §4.5/§5) is a message-passing protocol
whose correctness rests on interleaving-sensitive arguments: FIFO ordering
between broadcasts and replies, gap detection keeping every replica a prefix
of the leader's stream, version-guarded acks, epoch fencing, and a
freshest-wins handoff gather. That is exactly the class of system **TLA+** was
built for, and the model checker **TLC** explores every interleaving of a
bounded instance exhaustively.

Why TLA+ over the alternatives considered:

- **TLA+ / TLC** — the industry default for leader-based replication
  protocols (Raft, Paxos, MongoDB replication, AWS services, and FoundationDB
  itself have public TLA+ heritage). Mature, headless (`tla2tools.jar` on any
  JRE), so it drops into CI trivially. Rich literature to crib idioms from.
- **Quint (+ Apalache)** — nicer syntax, but a younger toolchain and a heavier
  CI footprint (SMT solver); TLA+'s ecosystem advantage wins for a protocol
  this subtle.
- **P** — good for event-driven state machines, but its checker is
  randomized/exploratory rather than exhaustive on a bounded model, and the
  toolchain (dotnet) is heavier.
- **Alloy** — structural/relational modeling; awkward for temporal,
  message-ordering properties, which are the heart of this protocol.
- **SPIN/Promela** — viable, but channels with dynamic leadership and
  map-typed state are clumsier than TLA+ functions.

## 2. What is modeled (scope)

One module, `formal/DgenRegistryReplication.tla`, at the abstraction level of
the `dgen_registry_member` moduledoc:

1. **Fenced commit** — the leader is sole writer; a commit succeeds only if
   the durable leader key + epoch still match (models the FDB conflict-range
   fence, §5.1). One op per batch: batching is a latency optimization, not a
   safety mechanism — each batch has a single commit version either way.
2. **Version-stamped broadcasts** — each carries `{Epoch, PrevVersion,
   Version}`; followers apply only **contiguous** broadcasts. The model's
   receive actions mirror `apply_bcast/6` case for case.
3. **Resync** — a gapped follower requests a snapshot from its stream's
   sender; the (believed) leader answers with the same snapshot shape the
   handoff fan-out uses.
4. **Forwarded registrations, version-guarded acks** — the leader's
   `{register_reply, yes, Version}` follows the broadcast on the same FIFO
   channel; the follower acks its caller only once `applied_version ≥
   Version`, else defers (`deferred_yes` / `flush_deferred`). Deferred acks
   are rejected on a leadership change.
5. **Direct registrations, replicate-before-ack** — `replicate_sync` /
   `replicate_ack` with `register_replicas = 1`; degrade-open timeout behind
   the `DegradeOpen` constant.
6. **Leadership handoff** — election bumps the durable epoch (abstracting the
   elector's DB-serialised queue); the new leader adopts the **freshest**
   live replica (max `applied_version`) and fans out snapshots. Snapshots
   apply only when `Epoch ≥ current`.
7. **Failures** — member crash (replica lost, no restart; bounded by
   `MaxCrashes`) and message loss. Loss is drop-head per channel, which
   together with FIFO delivery yields exactly Erlang's signal-ordering
   guarantee: delivered messages keep their pairwise order, any subset may be
   lost. Messages a member sent before crashing remain deliverable (also
   Erlang semantics).

Deliberately **out of scope** for v1 (state this in the module header):

- unregister / retract / `DOWN` cleanup and the §5.6 conflict trail +
  kill budget (heuristic repair keyed to wall-clock TTLs — a poor fit for
  model checking; a candidate v2 module),
- metadata / queries / presence (they ride the same pipeline and add no new
  replication mechanism),
- membership joins and **partial gathers**: the main model assumes the
  handoff gather reaches every live member, matching the single-fault
  guarantee's premise; a partial gather is the documented `degraded` mode,
- Erlang-distribution-only partitions that remove live nodes from the member
  set (a ≥2-fault scenario per the design doc).

## 3. Checked properties → design-doc guarantees

| Invariant | Meaning | Design doc |
|---|---|---|
| `PrefixConsistency` | every live member's replica equals the committed history at its applied version — "every replica is a prefix of the leader's stream" | §4.5, §5.7 |
| `UniqueBinding` | at most one pid is ever acked `yes` per name (no unregister in v1, so this is Guarantee 1 restricted to modeled ops) | Guarantee 1 |
| `DurableAcked` | every acked registration has ≥ `1 + MaxCrashes − crashes` **version-visible** holders (`appliedVer ≥ commit version` — a row held below that is invisible to the freshest-wins gather, which is precisely the window `deferred_yes` closes) | Guarantee 4, §5.5 |
| `LeaderEpochUnique` | two members may both believe they lead only under different epochs | §4.2, §5.1 |

## 4. Files to add

```
formal/
  DgenRegistryReplication.tla             the spec (§5)
  DgenRegistryReplication.cfg             main model — must pass (§6.1)
  DgenRegistryReplicationNoGuard.cfg      mutation — must fail (§6.2)
  DgenRegistryReplicationDegradeOpen.cfg  mutation — must fail (§6.3)
  check.sh                                runner, local + CI (§7)
  README.md                               spec ↔ code mapping (§9)
.github/workflows/formal.yml              CI workflow (§8)
```

Also append to `.gitignore` (repo root):

```
formal/tla2tools.jar
formal/states/
```

(`states/` is TLC's scratch directory, created next to the spec.)

## 5. The specification — `formal/DgenRegistryReplication.tla`

Create the file with exactly this content (fix-ups from TLC runs permitted;
keep the comments — they carry the code mapping):

```tla
------------------------ MODULE DgenRegistryReplication ------------------------
(***************************************************************************)
(* Model of the bespoke replication protocol in dgen_registry_member.erl. *)
(*                                                                         *)
(* Scope: register-only workload; fenced single-leader commits; version-   *)
(* stamped broadcasts with gap detection + resync; version-guarded         *)
(* forwarded acks (deferred_yes); replicate-before-ack for direct          *)
(* registrations; freshest-wins handoff gather; member crashes; message    *)
(* loss with Erlang signal-ordering semantics (per-pair FIFO of delivered  *)
(* messages, arbitrary loss).                                              *)
(*                                                                         *)
(* Out of scope (v1): unregister/DOWN/§5.6 conflict repair, metadata,      *)
(* presence, membership joins, partial gathers, DB-level partitions.       *)
(* The handoff gather is assumed to reach every live member — the          *)
(* single-fault guarantee's premise (design doc §5.4/§5.7).                *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Members,            \* e.g. {m1, m2, m3}
    Names,              \* e.g. {n1}
    Pids,               \* e.g. {p1, p2}
    MaxVersion,         \* bound on commit versions, e.g. 3
    MaxEpoch,           \* bound on elections, e.g. 2
    MaxCrashes,         \* the single-fault bound: 1
    MaxChanLen,         \* state constraint on channel length, e.g. 2
    VersionGuardedAck,  \* TRUE = current code; FALSE = pre-guard bug (mutation)
    DegradeOpen,        \* FALSE = strict_replication; TRUE = degrade-open (mutation)
    NoPid, NoMember     \* model values

ASSUME NoPid \notin Pids
ASSUME NoMember \notin Members
ASSUME MaxCrashes < Cardinality(Members)

VARIABLES
    \* ---- durable state (the database; §4.4: leader key + version key) ----
    dbLeader,    \* Members \cup {NoMember}: the committed leader
    dbEpoch,     \* Nat: the committed epoch (the fencing token)
    dbVersion,   \* Nat: the version key — last committed version
    \* ---- ghost (specification bookkeeping, not protocol state) ----
    histMap,     \* [0..MaxVersion -> [Names -> Pids \cup {NoPid}]]:
                 \* full name map after each committed version
    acked,       \* set of <<name, pid, ver>> whose caller was answered `yes`
    crashes,     \* Nat: crashes so far
    \* ---- per-member state ----
    alive,       \* [Members -> BOOLEAN]
    rep,         \* [Members -> [Names -> Pids \cup {NoPid}]]: the ETS replica
    appliedVer,  \* [Members -> Nat]: applied_version
    epoch,       \* [Members -> Nat]: last epoch learned (set by snapshots/assume)
    leaderView,  \* [Members -> Members \cup {NoMember}]: believed leader;
                 \* leaderView[m] = m  <=>  m believes it is the leader
    deferred,    \* [Members -> SUBSET (Names \X Pids \X Nat)]: deferred_yes
    pendingAcks, \* [Members -> SUBSET (Names \X Pids \X Nat)]: leader-side
                 \* direct registrations awaiting a replicate_ack
    resyncReq,   \* [Members -> Members \cup {NoMember}]: outstanding resync
                 \* target (the gapped stream's sender), NoMember = none
    \* ---- the network ----
    chan         \* [Members -> [Members -> Seq(Msg)]]: per-ordered-pair FIFO

vars == <<dbLeader, dbEpoch, dbVersion, histMap, acked, crashes,
          alive, rep, appliedVer, epoch, leaderView,
          deferred, pendingAcks, resyncReq, chan>>

(***************************************************************************)
(* Basic definitions                                                       *)
(***************************************************************************)

EmptyMap == [n \in Names |-> NoPid]
Live     == {m \in Members : alive[m]}
PidOpt   == Pids \cup {NoPid}
Versions == 0..MaxVersion
Regs     == Names \X Pids \X Versions

MaxOf(S) == CHOOSE x \in S : \A y \in S : x >= y

\* Message shapes.  bcast/reply mirror the stamped {name_registered}
\* broadcast and {register_reply, Ref, yes, Version} cast; sync/ack mirror
\* {replicate_sync}/{replicate_ack}; snap mirrors {apply_names_snapshot}.
Msg ==
       [type : {"bcast"}, name : Names, pid : Pids,
        epoch : 0..MaxEpoch, prev : Versions, ver : Versions]
  \cup [type : {"reply"}, name : Names, pid : Pids, ver : Versions]
  \cup [type : {"sync"},  ver : Versions]
  \cup [type : {"ack"},   ver : Versions]
  \cup [type : {"snap"},  rep : [Names -> PidOpt], epoch : 0..MaxEpoch,
        ver : Versions, ldr : Members]

HeadIs(s, r, t) == /\ chan[s][r] # <<>>
                   /\ Head(chan[s][r]).type = t

Consume(s, r) == [chan EXCEPT ![s][r] = Tail(chan[s][r])]

(***************************************************************************)
(* Init                                                                    *)
(***************************************************************************)

Init ==
    /\ dbLeader = NoMember /\ dbEpoch = 0 /\ dbVersion = 0
    /\ histMap = [v \in Versions |-> EmptyMap]
    /\ acked = {} /\ crashes = 0
    /\ alive = [m \in Members |-> TRUE]
    /\ rep = [m \in Members |-> EmptyMap]
    /\ appliedVer = [m \in Members |-> 0]
    /\ epoch = [m \in Members |-> 0]
    /\ leaderView = [m \in Members |-> NoMember]
    /\ deferred = [m \in Members |-> {}]
    /\ pendingAcks = [m \in Members |-> {}]
    /\ resyncReq = [m \in Members |-> NoMember]
    /\ chan = [s \in Members |-> [r \in Members |-> <<>>]]

(***************************************************************************)
(* Elections and handoff                                                   *)
(***************************************************************************)

\* The elector commits a new leader + epoch to the database (§4.2).
\* Abstracted to one atomic durable write; the new leader learns of it in
\* AssumeGather.  Allowing any live member keeps deposed-while-alive
\* scenarios (fencing tests) reachable.
Elect(m) ==
    /\ alive[m]
    /\ dbEpoch < MaxEpoch
    /\ dbLeader' = m
    /\ dbEpoch' = dbEpoch + 1
    /\ UNCHANGED <<dbVersion, histMap, acked, crashes, alive, rep, appliedVer,
                   epoch, leaderView, deferred, pendingAcks, resyncReq, chan>>

\* The genuine-leadership-change path: elector_assume_and_distribute ->
\* spawn_assume_gather -> gather_maps -> the {assume_gathered} continuation.
\* Modeled atomically; the gather reaches every live member (see module
\* header).  Freshest-wins: adopt the max-applied_version replica (version
\* tie => identical content, which PrefixConsistency asserts).  Deferred
\* acks are rejected and pending direct acks resolved-as-rejected, matching
\* do_leader_changed/reject_forwards; then snapshots fan out to every other
\* live member from this process (FIFO with later broadcasts).
AssumeGather(m) ==
    /\ alive[m]
    /\ dbLeader = m
    /\ epoch[m] < dbEpoch
    /\ LET maxV == MaxOf({appliedVer[x] : x \in Live})
       IN \E src \in Live :
            /\ appliedVer[src] = maxV
            /\ rep' = [rep EXCEPT ![m] = rep[src]]
            /\ appliedVer' = [appliedVer EXCEPT ![m] = maxV]
            /\ LET snap == [type |-> "snap", rep |-> rep[src],
                            epoch |-> dbEpoch, ver |-> maxV, ldr |-> m]
               IN chan' = [chan EXCEPT ![m] =
                    [r \in Members |->
                       IF r = m \/ ~alive[r] THEN chan[m][r]
                       ELSE Append(chan[m][r], snap)]]
    /\ epoch' = [epoch EXCEPT ![m] = dbEpoch]
    /\ leaderView' = [leaderView EXCEPT ![m] = m]
    /\ deferred' = [deferred EXCEPT ![m] = {}]
    /\ pendingAcks' = [pendingAcks EXCEPT ![m] = {}]
    /\ resyncReq' = [resyncReq EXCEPT ![m] = NoMember]
    /\ UNCHANGED <<dbLeader, dbEpoch, dbVersion, histMap, acked, crashes,
                   alive>>

(***************************************************************************)
(* Registration commits (leader = sole writer)                             *)
(***************************************************************************)

\* Commit preconditions: l believes it leads AND the durable fence passes
\* (dgen_registry_names:start_commit's conflict on the leader key, §5.1).
\* rep[l][n] \in {NoPid, p} is the `yes` verdict (plan_op/3: same-pid
\* re-registration is an idempotent yes); a different pid is answered `no`
\* with no state change, so the `no` case needs no action.
CanCommit(l, n, p) ==
    /\ alive[l]
    /\ leaderView[l] = l
    /\ dbLeader = l /\ dbEpoch = epoch[l]
    /\ dbVersion < MaxVersion
    /\ rep[l][n] \in {NoPid, p}

\* A follower-forwarded registration ({register_req} -> group commit ->
\* broadcast + {register_reply, Ref, yes, Version}).  The enqueue/commit
\* split in the code collapses into one atomic action: an op enqueued under
\* a belief that fails the fence at commit time is simply a rejected op with
\* no state change, which this model represents by the action not firing.
\* CRUCIAL FIFO DETAIL: the reply is appended AFTER the broadcast on the
\* same l->f channel, exactly like the code (broadcast precedes reply).
RegisterForward(f, l, n, p) ==
    /\ f # l
    /\ alive[f]
    /\ leaderView[f] = l          \* the follower forwards to its believed leader
    /\ CanCommit(l, n, p)
    /\ LET v      == dbVersion + 1
           newRep == [rep[l] EXCEPT ![n] = p]
           b == [type |-> "bcast", name |-> n, pid |-> p,
                 epoch |-> epoch[l], prev |-> appliedVer[l], ver |-> v]
           r == [type |-> "reply", name |-> n, pid |-> p, ver |-> v]
       IN /\ dbVersion' = v
          /\ histMap' = [histMap EXCEPT ![v] = newRep]
          /\ rep' = [rep EXCEPT ![l] = newRep]
          /\ appliedVer' = [appliedVer EXCEPT ![l] = v]
          /\ chan' = [chan EXCEPT ![l] =
               [g \in Members |->
                  IF g = l \/ ~alive[g] THEN chan[l][g]
                  ELSE IF g = f THEN chan[l][g] \o <<b, r>>
                  ELSE Append(chan[l][g], b)]]
    /\ UNCHANGED <<dbLeader, dbEpoch, acked, crashes, alive, epoch,
                   leaderView, deferred, pendingAcks, resyncReq>>

\* A direct (leader-local) registration: replicate-before-ack (§5.5).  The
\* binding commits and broadcasts, a {replicate_sync} chases the broadcast
\* on every channel, and the caller's `yes` is parked in pendingAcks until
\* a follower confirms (RecvAck) or the degrade-open timeout fires.
RegisterDirect(l, n, p) ==
    /\ CanCommit(l, n, p)
    /\ LET v      == dbVersion + 1
           newRep == [rep[l] EXCEPT ![n] = p]
           b == [type |-> "bcast", name |-> n, pid |-> p,
                 epoch |-> epoch[l], prev |-> appliedVer[l], ver |-> v]
           s == [type |-> "sync", ver |-> v]
       IN /\ dbVersion' = v
          /\ histMap' = [histMap EXCEPT ![v] = newRep]
          /\ rep' = [rep EXCEPT ![l] = newRep]
          /\ appliedVer' = [appliedVer EXCEPT ![l] = v]
          /\ pendingAcks' = [pendingAcks EXCEPT ![l] = @ \cup {<<n, p, v>>}]
          /\ chan' = [chan EXCEPT ![l] =
               [g \in Members |->
                  IF g = l \/ ~alive[g] THEN chan[l][g]
                  ELSE chan[l][g] \o <<b, s>>]]
    /\ UNCHANGED <<dbLeader, dbEpoch, acked, crashes, alive, epoch,
                   leaderView, deferred, resyncReq>>

(***************************************************************************)
(* Broadcast delivery — mirrors apply_bcast/6 case for case                *)
(***************************************************************************)

\* Contiguous (prev = applied, or another message of the batch we are at):
\* apply the row, advance applied_version (bump_applied), flush any deferred
\* acks the advance releases (flush_deferred), cancel an outstanding resync.
RecvBcastApply(s, f) ==
    /\ alive[f]
    /\ HeadIs(s, f, "bcast")
    /\ LET m == Head(chan[s][f]) IN
       /\ m.epoch >= epoch[f]
       /\ (m.prev = appliedVer[f] \/ m.ver = appliedVer[f])
       /\ LET newV    == IF m.ver > appliedVer[f] THEN m.ver ELSE appliedVer[f]
              flushed == {d \in deferred[f] : d[3] <= newV}
          IN /\ rep' = [rep EXCEPT ![f][m.name] = m.pid]
             /\ appliedVer' = [appliedVer EXCEPT ![f] = newV]
             /\ deferred' = [deferred EXCEPT ![f] = @ \ flushed]
             /\ acked' = acked \cup flushed
             /\ resyncReq' = [resyncReq EXCEPT ![f] = NoMember]
             /\ chan' = Consume(s, f)
    /\ UNCHANGED <<dbLeader, dbEpoch, dbVersion, histMap, crashes, alive,
                   epoch, leaderView, pendingAcks>>

\* Stale epoch (a deposed leader's broadcast), or a duplicate at-or-behind
\* our baseline: drop.
RecvBcastDrop(s, f) ==
    /\ alive[f]
    /\ HeadIs(s, f, "bcast")
    /\ LET m == Head(chan[s][f]) IN
       \/ m.epoch < epoch[f]
       \/ /\ m.epoch >= epoch[f]
          /\ m.prev # appliedVer[f] /\ m.ver # appliedVer[f]
          /\ m.ver <= appliedVer[f]
    /\ chan' = Consume(s, f)
    /\ UNCHANGED <<dbLeader, dbEpoch, dbVersion, histMap, acked, crashes,
                   alive, rep, appliedVer, epoch, leaderView, deferred,
                   pendingAcks, resyncReq>>

\* A gap: we missed a batch.  Refuse the broadcast (do NOT apply past the
\* hole) and flag a resync request to the stream's sender.
RecvBcastGap(s, f) ==
    /\ alive[f]
    /\ HeadIs(s, f, "bcast")
    /\ LET m == Head(chan[s][f]) IN
       /\ m.epoch >= epoch[f]
       /\ m.prev # appliedVer[f] /\ m.ver # appliedVer[f]
       /\ m.ver > appliedVer[f]
    /\ chan' = Consume(s, f)
    /\ resyncReq' = [resyncReq EXCEPT ![f] =
                       IF @ = NoMember THEN s ELSE @]
    /\ UNCHANGED <<dbLeader, dbEpoch, dbVersion, histMap, acked, crashes,
                   alive, rep, appliedVer, epoch, leaderView, deferred,
                   pendingAcks>>

(***************************************************************************)
(* Forwarded-registration reply — handle_register_reply/4                  *)
(***************************************************************************)

\* leaderView[f] # s models the pending_forwards entry having been rejected
\* on a leadership change (an unknown-Ref reply is ignored).  Otherwise:
\* applied >= ver  -> ack now (the broadcast/snapshot already delivered the
\*                    row; the follower is a version-visible second holder);
\* gapped + guard  -> defer (deferred_yes), released by flush_deferred;
\* gapped, no guard-> the pre-guard bug: optimistic row write + immediate
\*                    ack.  The row exists but at a version the freshest-wins
\*                    gather ignores — the silent-loss window.
RecvReply(s, f) ==
    /\ alive[f]
    /\ HeadIs(s, f, "reply")
    /\ LET m == Head(chan[s][f])
           t == <<m.name, m.pid, m.ver>>
       IN /\ chan' = Consume(s, f)
          /\ IF leaderView[f] # s
             THEN UNCHANGED <<rep, deferred, acked>>
             ELSE IF appliedVer[f] >= m.ver
             THEN /\ acked' = acked \cup {t}
                  /\ UNCHANGED <<rep, deferred>>
             ELSE IF VersionGuardedAck
             THEN /\ deferred' = [deferred EXCEPT ![f] = @ \cup {t}]
                  /\ UNCHANGED <<rep, acked>>
             ELSE /\ rep' = [rep EXCEPT ![f][m.name] = m.pid]
                  /\ acked' = acked \cup {t}
                  /\ UNCHANGED deferred
    /\ UNCHANGED <<dbLeader, dbEpoch, dbVersion, histMap, crashes, alive,
                   appliedVer, epoch, leaderView, pendingAcks, resyncReq>>

(***************************************************************************)
(* Replicate-before-ack — {replicate_sync}/{replicate_ack}                 *)
(***************************************************************************)

\* The follower confirms only if it has applied up to the batch's version
\* (the version-guarded replicate_sync handler); otherwise it stays silent
\* and the leader falls back to other followers or its timeout policy.
RecvSync(s, f) ==
    /\ alive[f]
    /\ HeadIs(s, f, "sync")
    /\ LET m == Head(chan[s][f]) IN
       chan' = IF appliedVer[f] >= m.ver /\ alive[s]
               THEN [chan EXCEPT
                       ![s][f] = Tail(chan[s][f]),
                       ![f][s] = Append(chan[f][s],
                                        [type |-> "ack", ver |-> m.ver])]
               ELSE Consume(s, f)
    /\ UNCHANGED <<dbLeader, dbEpoch, dbVersion, histMap, acked, crashes,
                   alive, rep, appliedVer, epoch, leaderView, deferred,
                   pendingAcks, resyncReq>>

\* With register_replicas = 1 the first distinct follower ack resolves the
\* batch (the distinct-follower counting in the code only matters for
\* register_replicas > 1 — note it in README as a v2 extension).
RecvAck(s, l) ==
    /\ alive[l]
    /\ HeadIs(s, l, "ack")
    /\ LET m   == Head(chan[s][l])
           pas == {pa \in pendingAcks[l] : pa[3] = m.ver}
       IN /\ chan' = Consume(s, l)
          /\ IF leaderView[l] = l /\ pas # {}
             THEN /\ acked' = acked \cup pas
                  /\ pendingAcks' = [pendingAcks EXCEPT ![l] = @ \ pas]
             ELSE UNCHANGED <<acked, pendingAcks>>
    /\ UNCHANGED <<dbLeader, dbEpoch, dbVersion, histMap, crashes, alive,
                   rep, appliedVer, epoch, leaderView, deferred, resyncReq>>

\* The {replicate_timeout} handler with strict_replication = false: ack
\* `yes` leader-only.  Enabled only in the DegradeOpen mutation model; the
\* strict (fail-closed) policy simply never acks an unconfirmed direct
\* registration (the retract path is out of scope with no unregister).
DegradeTimeout(l) ==
    /\ DegradeOpen
    /\ alive[l]
    /\ leaderView[l] = l
    /\ \E pa \in pendingAcks[l] :
         /\ acked' = acked \cup {pa}
         /\ pendingAcks' = [pendingAcks EXCEPT ![l] = @ \ {pa}]
    /\ UNCHANGED <<dbLeader, dbEpoch, dbVersion, histMap, crashes, alive,
                   rep, appliedVer, epoch, leaderView, deferred, resyncReq,
                   chan>>

(***************************************************************************)
(* Snapshots — {apply_names_snapshot} and the resync serve                 *)
(***************************************************************************)

\* Epoch-guarded (Epoch >= CurrentEpoch) wholesale re-baseline.  On a
\* leadership change the deferred acks are rejected (do_leader_changed);
\* on a same-leader snapshot (a resync) the version advance releases them
\* (flush_deferred).  A member that believed it led relinquishes.
RecvSnap(s, f) ==
    /\ alive[f]
    /\ HeadIs(s, f, "snap")
    /\ LET m == Head(chan[s][f]) IN
       /\ chan' = Consume(s, f)
       /\ IF m.epoch >= epoch[f]
          THEN LET changed == leaderView[f] # m.ldr
                   flushed == IF changed THEN {}
                              ELSE {d \in deferred[f] : d[3] <= m.ver}
               IN /\ rep' = [rep EXCEPT ![f] = m.rep]
                  /\ appliedVer' = [appliedVer EXCEPT ![f] = m.ver]
                  /\ epoch' = [epoch EXCEPT ![f] = m.epoch]
                  /\ leaderView' = [leaderView EXCEPT ![f] = m.ldr]
                  /\ resyncReq' = [resyncReq EXCEPT ![f] = NoMember]
                  /\ deferred' = [deferred EXCEPT ![f] =
                                    IF changed THEN {} ELSE @ \ flushed]
                  /\ acked' = acked \cup flushed
                  /\ pendingAcks' = [pendingAcks EXCEPT ![f] =
                                       IF m.ldr # f THEN {} ELSE @]
          ELSE UNCHANGED <<rep, appliedVer, epoch, leaderView, resyncReq,
                           deferred, acked, pendingAcks>>
    /\ UNCHANGED <<dbLeader, dbEpoch, dbVersion, histMap, crashes, alive>>

\* The {resync_req} handler: a member that believes it leads answers with
\* its full replica.  DELIBERATELY not durably fenced — the code isn't
\* either; per-pair FIFO plus the requester's own state make a stale serve
\* harmless, which this model checks rather than assumes.  The request flag
\* clears on serve (the code clears a timer on snapshot arrival and
\* re-requests on expiry; a lost snapshot here is re-requested by the next
\* gapped broadcast).
ServeResync(l, f) ==
    /\ alive[l] /\ alive[f]
    /\ resyncReq[f] = l
    /\ leaderView[l] = l
    /\ chan' = [chan EXCEPT ![l][f] = Append(chan[l][f],
         [type |-> "snap", rep |-> rep[l], epoch |-> epoch[l],
          ver |-> appliedVer[l], ldr |-> l])]
    /\ resyncReq' = [resyncReq EXCEPT ![f] = NoMember]
    /\ UNCHANGED <<dbLeader, dbEpoch, dbVersion, histMap, acked, crashes,
                   alive, rep, appliedVer, epoch, leaderView, deferred,
                   pendingAcks>>

(***************************************************************************)
(* Failures                                                                *)
(***************************************************************************)

\* A member crash: the ETS replica dies with the process; no restart in v1
\* (a restarted member is `fresh` and holds nothing — no new behavior for
\* the checked invariants).  Incoming in-flight messages die with the
\* receiver, but messages the crashed member already sent may still be
\* delivered — Erlang signal semantics.
Crash(m) ==
    /\ alive[m]
    /\ crashes < MaxCrashes
    /\ alive' = [alive EXCEPT ![m] = FALSE]
    /\ crashes' = crashes + 1
    /\ rep' = [rep EXCEPT ![m] = EmptyMap]
    /\ appliedVer' = [appliedVer EXCEPT ![m] = 0]
    /\ epoch' = [epoch EXCEPT ![m] = 0]
    /\ leaderView' = [leaderView EXCEPT ![m] = NoMember]
    /\ deferred' = [deferred EXCEPT ![m] = {}]
    /\ pendingAcks' = [pendingAcks EXCEPT ![m] = {}]
    /\ resyncReq' = [resyncReq EXCEPT ![m] = NoMember]
    /\ chan' = [s \in Members |-> [r \in Members |->
                  IF r = m THEN <<>> ELSE chan[s][r]]]
    /\ UNCHANGED <<dbLeader, dbEpoch, dbVersion, histMap, acked>>

\* Message loss: drop the head of any channel.  Drop-head + deliver-head
\* yields every subsequence, i.e. Erlang's guarantee exactly: delivered
\* messages keep their pairwise order, any subset may be lost (a dropped
\* connection's in-flight signals).
DropMsg(s, r) ==
    /\ chan[s][r] # <<>>
    /\ chan' = [chan EXCEPT ![s][r] = Tail(chan[s][r])]
    /\ UNCHANGED <<dbLeader, dbEpoch, dbVersion, histMap, acked, crashes,
                   alive, rep, appliedVer, epoch, leaderView, deferred,
                   pendingAcks, resyncReq>>

(***************************************************************************)
(* Next / Spec                                                             *)
(***************************************************************************)

Next ==
    \/ \E m \in Members :
         Elect(m) \/ AssumeGather(m) \/ Crash(m) \/ DegradeTimeout(m)
    \/ \E l \in Members, n \in Names, p \in Pids :
         \/ RegisterDirect(l, n, p)
         \/ \E f \in Members : RegisterForward(f, l, n, p)
    \/ \E s \in Members, r \in Members :
         \/ RecvBcastApply(s, r) \/ RecvBcastDrop(s, r) \/ RecvBcastGap(s, r)
         \/ RecvReply(s, r) \/ RecvSync(s, r) \/ RecvAck(s, r)
         \/ RecvSnap(s, r) \/ ServeResync(s, r) \/ DropMsg(s, r)

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* Invariants                                                              *)
(***************************************************************************)

TypeOK ==
    /\ dbLeader \in Members \cup {NoMember}
    /\ dbEpoch \in 0..MaxEpoch
    /\ dbVersion \in Versions
    /\ histMap \in [Versions -> [Names -> PidOpt]]
    /\ alive \in [Members -> BOOLEAN]
    /\ rep \in [Members -> [Names -> PidOpt]]
    /\ appliedVer \in [Members -> Versions]
    /\ epoch \in [Members -> 0..MaxEpoch]
    /\ leaderView \in [Members -> Members \cup {NoMember}]
    /\ deferred \in [Members -> SUBSET Regs]
    /\ pendingAcks \in [Members -> SUBSET Regs]
    /\ resyncReq \in [Members -> Members \cup {NoMember}]
    /\ acked \subseteq Regs
    /\ crashes \in 0..MaxCrashes
    /\ \A s \in Members : \A r \in Members :
         \A i \in 1..Len(chan[s][r]) : chan[s][r][i] \in Msg

\* "Every replica is a prefix of the leader's stream" (§4.5): a live
\* member's replica is exactly the committed history at its applied version.
\* Implies version-tie => identical content, which freshest-wins tie-breaking
\* relies on (gather_maps' self-first CHOOSE).
PrefixConsistency ==
    \A m \in Live : rep[m] = histMap[appliedVer[m]]

\* Guarantee 1, restricted to the modeled (register-only) workload: two
\* different pids are never both acked `yes` for one name.
UniqueBinding ==
    \A n \in Names :
      Cardinality({p \in Pids : \E v \in Versions : <<n, p, v>> \in acked})
        <= 1

\* Guarantee 4 (two-holder durability), in inductive form.  A holder must be
\* VERSION-VISIBLE (appliedVer >= the binding's commit version): a row held
\* below that version is invisible to the freshest-wins gather — exactly the
\* silent-loss window deferred_yes closes.  With MaxCrashes = 1: two holders
\* while no crash has happened, one surviving holder after the crash.
Holders(a) ==
    {m \in Live : appliedVer[m] >= a[3] /\ rep[m][a[1]] = a[2]}

DurableAcked ==
    \A a \in acked : Cardinality(Holders(a)) >= 1 + MaxCrashes - crashes

\* Fencing sanity: two members may both believe they lead only under
\* different epochs (§5.1's monotonic fencing token).
LeaderEpochUnique ==
    \A m1 \in Live : \A m2 \in Live :
      (m1 # m2 /\ leaderView[m1] = m1 /\ leaderView[m2] = m2)
        => epoch[m1] # epoch[m2]

(***************************************************************************)
(* Model-checking bounds                                                   *)
(***************************************************************************)

ChanBound == \A s \in Members : \A r \in Members :
               Len(chan[s][r]) <= MaxChanLen

Symm == Permutations(Pids)

=============================================================================
```

### Design notes the implementer must not "simplify away"

- **Reply after broadcast on the same channel** (`RegisterForward`'s
  `<<b, r>>`): the version guard is only meaningful because FIFO normally
  delivers the broadcast first; loss (`DropMsg`) is what creates the gapped
  case.
- **`Holders` requires `appliedVer[m] >= a[3]`**: dropping that conjunct
  makes `DurableAcked` blind to the very bug the NoGuard mutation must catch.
- **`ServeResync` is not durably fenced** — matching the code is the point;
  the model demonstrates it is nevertheless safe.
- **`Crash` keeps the crashed member's outgoing channels** — a reply sent
  just before the leader died must remain deliverable, or the model misses
  the interesting post-crash ack scenarios.
- **The atomic `AssumeGather` reads `Live`**, not the member set at election
  time — the v1 assumption that the gather reaches every live member.

## 6. Model configs

### 6.1 `formal/DgenRegistryReplication.cfg` — main model, must pass

```
SPECIFICATION Spec
CONSTANTS
  m1 = m1
  m2 = m2
  m3 = m3
  Members = {m1, m2, m3}
  n1 = n1
  Names = {n1}
  p1 = p1
  p2 = p2
  Pids = {p1, p2}
  NoPid = NoPid
  NoMember = NoMember
  MaxVersion = 3
  MaxEpoch = 2
  MaxCrashes = 1
  MaxChanLen = 2
  VersionGuardedAck = TRUE
  DegradeOpen = FALSE
INVARIANTS
  TypeOK
  PrefixConsistency
  UniqueBinding
  DurableAcked
  LeaderEpochUnique
CONSTRAINT ChanBound
SYMMETRY Symm
```

One name and two pids suffice: `UniqueBinding` needs two *pids*, not two
names, and names are independent with no unregister. If the run finishes in
well under a minute, raise `MaxVersion` to 4 and `MaxChanLen` to 3 and keep
the largest bounds that stay under ~10 minutes in CI.

### 6.2 `formal/DgenRegistryReplicationNoGuard.cfg` — mutation, must fail

Identical to 6.1 except:

```
  VersionGuardedAck = FALSE
```

and the invariant list reduced to the ones the mutation must break:

```
INVARIANTS
  UniqueBinding
  DurableAcked
```

Expected counterexample (TLC will likely find the short form): leader `l`
commits `<<n1, p1, v>>` forwarded by `f`; the broadcast to `f` is dropped
(`DropMsg`); the reply is delivered and — unguarded — `f` acks with
`appliedVer[f] < v`. `DurableAcked` fails immediately: `l` is the only
version-visible holder (`f`'s optimistic row sits below `v`, invisible to a
gather). The deeper trace behind it: `Crash(l)` then leaves zero holders,
a new leader reconstructs without the binding, and `p2` can be acked for
`n1` — violating `UniqueBinding`. This is the §5.5 silent-loss window that
`deferred_yes` exists to close.

### 6.3 `formal/DgenRegistryReplicationDegradeOpen.cfg` — mutation, must fail

Identical to 6.1 except:

```
  DegradeOpen = TRUE
```

```
INVARIANTS
  DurableAcked
```

Expected counterexample: `RegisterDirect` commits, `DegradeTimeout` acks
leader-only before any follower confirms → one version-visible holder with
no crash spent. This reproduces the documented degrade-open caveat of the
default `strict_replication = false` policy (design doc Guarantee 4's
carve-out) — the point of checking it is proving the model, not the code,
is sensitive to the policy.

## 7. Runner — `formal/check.sh`

```sh
#!/usr/bin/env bash
# Run TLC on one model config.
#   usage: check.sh <ConfigBasenameWithout.cfg> [pass|fail]
# `fail` asserts TLC finds an invariant violation (mutation configs).
# Requires tla2tools.jar next to this script, or $TLA2TOOLS_JAR.
set -euo pipefail
cd "$(dirname "$0")"

CFG="${1:?usage: check.sh <config-basename> [pass|fail]}"
EXPECT="${2:-pass}"
JAR="${TLA2TOOLS_JAR:-tla2tools.jar}"
OUT="$(mktemp)"
trap 'rm -f "$OUT"' EXIT

set +e
# -deadlock DISABLES deadlock checking: bounded models legitimately run out
# of enabled actions when MaxVersion/MaxEpoch/MaxCrashes are exhausted.
java -XX:+UseParallelGC -cp "$JAR" tlc2.TLC \
  -workers auto -deadlock -cleanup \
  -config "${CFG}.cfg" DgenRegistryReplication.tla 2>&1 | tee "$OUT"
STATUS=${PIPESTATUS[0]}
set -e

if [ "$EXPECT" = "pass" ]; then
  exit "$STATUS"
fi

# expected-fail: demand a genuine invariant violation, so a parse error or
# JVM crash cannot masquerade as the expected counterexample.
if [ "$STATUS" -ne 0 ] && grep -Eq "Invariant .* is violated" "$OUT"; then
  echo "OK: expected invariant violation was found"
  exit 0
fi
echo "ERROR: expected an invariant violation, but TLC exited $STATUS without one"
exit 1
```

`chmod +x formal/check.sh`. TLC exits `12` on a safety violation; the script
accepts any non-zero **plus** the violation line.

## 8. CI — `.github/workflows/formal.yml`

```yaml
name: Formal

on:
  push:
    branches: ["main"]
  pull_request:

# No `paths` filter on purpose: the spec asserts properties of
# src/dgen_registry_member.erl, so it should run on code changes too, as a
# prompt to keep spec and code in step.

env:
  TLA_VERSION: v1.8.0
  # sha256 of tla2tools.jar for TLA_VERSION — compute once (§10 step 1) and
  # pin here before merging:
  TLA_SHA256: "FILL_ME_IN"

jobs:
  tlc:
    runs-on: ubuntu-latest
    name: TLC ${{ matrix.cfg }} (expect ${{ matrix.expect }})
    strategy:
      fail-fast: false
      matrix:
        include:
          - cfg: DgenRegistryReplication
            expect: pass
          - cfg: DgenRegistryReplicationNoGuard
            expect: fail
          - cfg: DgenRegistryReplicationDegradeOpen
            expect: fail
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-java@v4
        with:
          distribution: temurin
          java-version: "21"
      - name: Cache tla2tools.jar
        id: cache-tla
        uses: actions/cache@v3
        with:
          path: formal/tla2tools.jar
          key: tla2tools-${{ env.TLA_VERSION }}
      - name: Download tla2tools.jar
        if: steps.cache-tla.outputs.cache-hit != 'true'
        run: |
          curl -fsSL -o formal/tla2tools.jar \
            "https://github.com/tlaplus/tlaplus/releases/download/${TLA_VERSION}/tla2tools.jar"
      - name: Verify checksum
        run: echo "${TLA_SHA256}  formal/tla2tools.jar" | sha256sum -c -
      - name: Model-check
        run: formal/check.sh ${{ matrix.cfg }} ${{ matrix.expect }}
```

Conventions match `ci.yml` (`actions/checkout@v3`, `actions/cache@v3`,
push-to-main + pull_request triggers).

## 9. `formal/README.md` — content to write

Short document with three parts:

1. **How to run locally**:

   ```sh
   curl -fsSL -o formal/tla2tools.jar \
     https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar
   formal/check.sh DgenRegistryReplication            # must pass
   formal/check.sh DgenRegistryReplicationNoGuard fail
   formal/check.sh DgenRegistryReplicationDegradeOpen fail
   ```

2. **Spec ↔ code map** (table; keep in sync when either side changes):

   | Spec | Code |
   |---|---|
   | `Elect` | `dgen_registry_elector` committing a membership/leadership change (abstracted to one durable write) |
   | `AssumeGather` | `{elector_assume_and_distribute}` genuine-change clause → `spawn_assume_gather` → `gather_maps/3` → `{assume_gathered}` continuation (modeled atomically; gather assumed to reach every live member) |
   | `CanCommit`'s `dbLeader/dbEpoch` conjuncts | the fenced version-key bump in `dgen_registry_names:start_commit/4` (§5.1) |
   | `RegisterForward` | follower `route_register` forward → leader `{register_req}` → group commit → broadcast + `{register_reply, Ref, yes, Version}` |
   | `RegisterDirect` + `RecvSync`/`RecvAck` | leader-local `route_register` → `pending_acks` / `{replicate_sync}` / `{replicate_ack}` |
   | `DegradeTimeout` | `handle_info({replicate_timeout, _})` with `strict_replication = false` |
   | `RecvBcastApply/Drop/Gap` | `apply_bcast/6` (the four-way case split) |
   | `RecvReply` | `handle_register_reply/4` incl. `deferred_yes`; flushes are `bump_applied/2` → `flush_deferred/1` |
   | `RecvSnap` | `handle_cast({apply_names_snapshot, ...})` |
   | `ServeResync` / `resyncReq` | `{resync_req}` handler / `request_resync/2` |
   | `Crash` | member/node death; the ETS replica dies with the process |
   | `DropMsg` | casts lost on an Erlang-distribution disconnect (signal-ordering semantics) |

3. **Scope and assumptions** — copy the out-of-scope list from §2 and the
   invariant table from §3 of this plan; state the expected-fail mutation
   configs and what each demonstrates.

## 10. Implementation procedure (do these in order)

1. **Get the checker.** `curl -fsSL -o formal/tla2tools.jar
   https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar`,
   then `sha256sum formal/tla2tools.jar` and paste the digest into
   `TLA_SHA256` in the workflow. Requires Java 11+ (`java -version`).
   *Note:* the sandbox this plan was written in could not reach GitHub
   release assets (egress proxy 403; the jar is not on Maven Central). If
   yours can't either, land the workflow with the checksum step temporarily
   removed, iterate via CI, then pin the checksum.
2. **Create the files** from §5–§9 verbatim, plus the `.gitignore` lines
   from §4. `chmod +x formal/check.sh`.
3. **Parse-check**: `java -cp formal/tla2tools.jar tla2sany.SANY
   formal/DgenRegistryReplication.tla`. Fix syntax errors (§11) until clean.
4. **Run the main model**: `formal/check.sh DgenRegistryReplication`.
   - TLC prints progress like `... 123456 states generated, 23456 distinct
     states found`. Success ends with `Model checking completed. No error
     has been found.` and exit 0.
   - If TLC reports an invariant violated, read the error trace state by
     state and decide: modeling bug (fix the spec — most likely) or a real
     protocol finding (write it up; do not silently weaken the invariant).
   - If the state space explodes (>~10 min), lower `MaxVersion` to 2 or
     `MaxChanLen` to 1 first; only then consider dropping `TypeOK` from the
     INVARIANTS list (it is the most expensive and least interesting).
5. **Run the mutations**:
   `formal/check.sh DgenRegistryReplicationNoGuard fail` and
   `formal/check.sh DgenRegistryReplicationDegradeOpen fail` — both must
   print `OK: expected invariant violation was found`. Read each
   counterexample trace and confirm it matches the prose in §6.2/§6.3; if
   TLC instead finds some unrelated artifact violation, fix the spec.
6. **Write `formal/README.md`** (§9), including the two traces' summaries.
7. **CI**: add the workflow, push, and confirm all three matrix legs are
   green (the expected-fail legs are green *because* TLC failed).
8. Keep total CI wall time for the three legs under ~10 minutes by tuning
   the §6.1 bounds.

## 11. Known TLA+/TLC pitfalls (read before debugging)

- `@` in `EXCEPT` refers to the old value: `[f EXCEPT ![x] = @ \cup S]`.
- A multi-key update is one `EXCEPT` with commas:
  `[chan EXCEPT ![s][f] = ..., ![f][s] = ...]` — valid only when the two
  paths differ (here `s # f` always holds where it's used).
- `Permutations` needs `EXTENDS TLC`; model values (`m1`, `p1`, ...) must be
  declared in the `.cfg` exactly as written in §6.1.
- Disjunctions inside a conjunction list need parentheses:
  `/\ (A \/ B)` — a bare `\/` continues the outer bullet list instead.
- Every action must define a prime for *every* variable (via assignment or
  `UNCHANGED`); TLC's "successor state is not completely specified" error
  means one is missing — check the `UNCHANGED` tuples first after any edit.
- Tuple indexing is 1-based: `d[3]` is the version of `<<name, pid, ver>>`.
- `CHOOSE` over an empty set is an error: `MaxOf({appliedVer[x] : x \in
  Live})` requires `Live # {}`, which `MaxCrashes < Cardinality(Members)`
  guarantees.
- If TLC warns about the `SYMMETRY` set, run once without it — symmetry can
  make error traces harder to read but never masks a violation.
- The `-deadlock` flag *disables* deadlock checking (needed: bounded models
  legitimately exhaust their actions).

## 12. Acceptance criteria

- `SANY` parses the spec cleanly.
- Main config passes exhaustively with the §6.1 bounds (or documented larger
  ones) — no invariant violations, no TLC errors.
- Both mutation configs fail with a genuine `Invariant ... is violated`
  whose trace matches §6.2/§6.3.
- `formal.yml` is green on all three matrix legs, `TLA_SHA256` pinned.
- `formal/README.md` documents run instructions, the code map, scope, and
  the two expected counterexamples.

## 13. Possible v2 extensions (not now)

- Unregister + the §5.6 release trail: adds `name_unregistered` broadcasts,
  pid-guarded retracts, and gives `UniqueBinding` name-reuse cases.
- `register_replicas > 1` (distinct-follower ack counting becomes load-bearing).
- Partial gathers (`GATHER_TIMEOUT` skips) + `reject_when_degraded`: check
  the prevention mode actually prevents re-issue after an incomplete gather.
- A `NoEpochGuard` mutation (drop `m.epoch < epoch[f]` in `RecvBcastDrop` /
  allow stale-epoch applies) to demonstrate the broadcast epoch stamp is
  load-bearing after a handoff.
- Liveness (every forwarded register eventually answered) under fairness —
  TLC checks temporal properties too, at higher cost.
- A model of `dgen_registry_elector`'s DB-serialised membership queue if its
  exactly-once consumption claims ever need the same treatment.
