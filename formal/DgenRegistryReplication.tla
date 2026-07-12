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
    SafeAssume,         \* TRUE = fixed code (version-key-fenced handoff + monotonic
                        \* snapshot); FALSE = the pre-fix handoff-gather race (mutation)
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
\* THE FIX (SafeAssume): the assuming leader must not reconstruct from a gather
\* that is behind the durable version key.  `maxV` is the freshest applied_version
\* any live member reports; `dbVersion` is the committed frontier (the version
\* key, §4.4).  A live member can never be ahead of the frontier, so
\* `maxV >= dbVersion` means "the freshest live member has applied every committed
\* batch" — only then does its replica equal the true current map, so freshest-wins
\* is sound.  Without this guard, a gather that races an in-flight (committed but
\* not-yet-applied) broadcast reconstructs a *stale* map and then overwrites the
\* very followers that are about to hold the missing binding — the handoff-gather
\* race (see formal/README.md).  In the real code this is the new leader comparing
\* its gathered MaxVersion against the durable version key and retrying/waiting the
\* gather until caught up (bounded, then assume-degraded — the availability
\* tradeoff, out of this safety model's scope, analogous to DegradeOpen).
AssumeGather(m) ==
    /\ alive[m]
    /\ dbLeader = m
    /\ epoch[m] < dbEpoch
    /\ LET maxV == MaxOf({appliedVer[x] : x \in Live})
       IN /\ (SafeAssume => maxV >= dbVersion)
          /\ \E src \in Live :
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
\*
\* THE FIX (SafeAssume), second half: the re-baseline must be version-monotonic
\* — a snapshot may not move a follower BACKWARD in applied_version.  The pre-fix
\* code guarded only on epoch, so a stale snapshot (an old assume/resync snapshot
\* delivered late, after the follower already applied a newer broadcast) would
\* wholesale-overwrite the follower back to older state, silently dropping a row
\* it — or a peer — had already acked.  With SafeAssume the apply requires
\* `m.ver >= appliedVer[f]`.  This is sound with the AssumeGather guard above:
\* a legitimate new leader is caught up (its snapshot's version is >= the durable
\* frontier >= any follower's applied_version), and a legitimate resync only ever
\* moves a gapped follower FORWARD, so the guard rejects only genuinely stale
\* snapshots.  In the real code: add a version check beside the `Epoch >= CurrentEpoch`
\* check in handle_cast({apply_names_snapshot, ...}).
RecvSnap(s, f) ==
    /\ alive[f]
    /\ HeadIs(s, f, "snap")
    /\ LET m == Head(chan[s][f]) IN
       /\ chan' = Consume(s, f)
       /\ IF m.epoch >= epoch[f] /\ (SafeAssume => m.ver >= appliedVer[f])
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
