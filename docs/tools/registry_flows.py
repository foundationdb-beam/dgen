"""Generates the swimlane diagrams in docs/design/dgen_registry_message_flows.md.

Run it from anywhere with `python3 docs/tools/registry_flows.py`. It writes one SVG
per flow to docs/design/assets/registry_flows/ and regenerates the Markdown page.
The flows below are written by hand from the code, so update them when a message
changes.

*This script is LLM-generated. See the AI disclosure in README.md.*
"""
import html
import math
import os
import textwrap

HERE = os.path.dirname(os.path.abspath(__file__))
MEMBER = "src/dgen_registry/dgen_registry_member.erl"
ELECTOR = "src/dgen_registry/dgen_registry_elector.erl"
ACTIONS = "src/dgen_registry/dgen_registry_elector_actions.erl"
REPLICA = "src/dgen_registry/dgen_registry_replica.erl"
INTENTS = "src/dgen_registry/dgen_registry_intents.erl"
NAMES = "src/dgen_registry/dgen_registry_names.erl"
API = "src/dgen_registry.erl"

GUTTER = 40
HEAD_H = 46
TOP = 72
MONO_W = 6.55   # px per char at 11px mono
SANS_W = 6.0    # px per char at 11.5px sans, conservative
LINE = 15

LANES = {
    "C": ("Caller", "any process"),
    "F": ("Follower", "member"),
    "F2": ("Other followers", "members"),
    "L": ("Leader", "member"),
    "W": ("Commit worker", "dgen_transaction"),
    "DB": ("Backend", "FDB keys"),
    "E": ("Elector", "dgen_server"),
    "H": ("Helper", "spawned process"),
    "P": ("Registered pid", "monitored"),
    "J": ("Joining member", "new node"),
    "L0": ("Old leader", "member"),
    "N": ("New leader", "member"),
    "T": ("Names table", "local ETS"),
}


def esc(s):
    return html.escape(s, quote=True)


class Diagram:
    def __init__(self, lanes, spacing):
        self.lanes = lanes
        self.S = spacing
        self.W = GUTTER + len(lanes) * spacing + 8
        self.parts = []
        self.y = TOP
        self.step = 0

    def x(self, key):
        return GUTTER + self.S / 2 + self.lanes.index(key) * self.S

    def clamp(self, cx, w):
        return max(w / 2 + 4, min(self.W - w / 2 - 4, cx))

    def number(self, y):
        self.step += 1
        self.parts.append(
            f'<circle class="num-dot" cx="18" cy="{y - 4:.1f}" r="9"/>'
            f'<text class="num" x="18" y="{y - 0.5:.1f}" text-anchor="middle">{self.step}</text>'
        )

    def head(self, x2, y, cls, left):
        d = -1 if left else 1
        pts = f"{x2:.1f},{y:.1f} {x2 - d * 8:.1f},{y - 4.5:.1f} {x2 - d * 8:.1f},{y + 4.5:.1f}"
        self.parts.append(f'<polygon class="head {cls}" points="{pts}"/>')

    def msg(self, frm, to, label, kind="cast", opts=()):
        targets = to if isinstance(to, list) else [to]
        y = self.y + 22
        x1 = self.x(frm)
        cls = f"m-{kind}" + (" accent" if "accent" in opts else "")
        xs = [self.x(t) for t in targets]
        far = max(xs, key=lambda v: abs(v - x1))
        near = min(xs, key=lambda v: abs(v - x1))
        lost = "lost" in opts
        if lost:
            x_end = x1 + (far - x1) * 0.62
            segs = [(x1, x_end)]
        else:
            # One segment per side, so a broadcast can fan out both ways.
            segs = [(x1, min(xs) + 8)] if min(xs) < x1 else []
            if max(xs) > x1:
                segs.append((x1, max(xs) - 8))
        for a, b in segs:
            self.parts.append(
                f'<line class="ln {cls}" x1="{a:.1f}" y1="{y}" x2="{b:.1f}" y2="{y}"/>'
            )
        if kind == "call":
            self.parts.append(f'<circle class="dot {cls}" cx="{x1:.1f}" cy="{y}" r="3.2"/>')
        if lost:
            cx = x_end
            self.parts.append(
                f'<path class="lost" d="M{cx - 5:.1f},{y - 5} L{cx + 5:.1f},{y + 5} '
                f'M{cx + 5:.1f},{y - 5} L{cx - 5:.1f},{y + 5}"/>'
            )
        else:
            for xt in xs:
                self.head(xt, y, cls, xt < x1)
        w = len(label) * MONO_W
        span_mid = (x1 + (near if not lost else x_end)) / 2
        cx = self.clamp(span_mid, w)
        self.parts.append(
            f'<text class="lbl {cls}" x="{cx:.1f}" y="{y - 7}" text-anchor="middle">{esc(label)}</text>'
        )
        self.number(y)
        self.y += 34

    def act(self, lane, text, cls=""):
        maxw = self.S * 1.75
        chars = max(14, int((maxw - 18) / SANS_W))
        lines = textwrap.wrap(text, chars)
        w = min(maxw, max(len(l) for l in lines) * SANS_W + 20)
        h = len(lines) * LINE + 10
        cx = self.clamp(self.x(lane), w)
        y0 = self.y + 6
        self.parts.append(
            f'<rect class="act {cls}" x="{cx - w / 2:.1f}" y="{y0}" width="{w:.1f}" height="{h}" rx="5"/>'
        )
        for i, l in enumerate(lines):
            self.parts.append(
                f'<text class="act-t {cls}" x="{cx:.1f}" y="{y0 + 16 + i * LINE}" text-anchor="middle">{esc(l)}</text>'
            )
        self.y = y0 + h + 4

    def note(self, a, b, text):
        xa, xb = sorted([self.x(a), self.x(b)])
        x0 = max(GUTTER - 4, xa - self.S / 2 + 10)
        x1 = min(self.W - 4, xb + self.S / 2 - 10)
        w = x1 - x0
        chars = max(20, int((w - 24) / SANS_W))
        lines = textwrap.wrap(text, chars)
        h = len(lines) * LINE + 12
        y0 = self.y + 8
        self.parts.append(
            f'<rect class="note" x="{x0:.1f}" y="{y0}" width="{w:.1f}" height="{h}" rx="4"/>'
        )
        for i, l in enumerate(lines):
            self.parts.append(
                f'<text class="note-t" x="{x0 + 12:.1f}" y="{y0 + 17 + i * LINE}">{esc(l)}</text>'
            )
        self.y = y0 + h + 6

    def phase(self, text):
        y = self.y + 20
        self.parts.append(
            f'<line class="phase-ln" x1="{GUTTER - 30}" y1="{y}" x2="{self.W - 4}" y2="{y}"/>'
        )
        w = len(text) * 6.9 + 16
        self.parts.append(
            f'<rect class="phase-bg" x="{GUTTER - 30}" y="{y - 9}" width="{w:.1f}" height="18"/>'
            f'<text class="phase-t" x="{GUTTER - 24}" y="{y + 4}">{esc(text.upper())}</text>'
        )
        self.y = y + 14

    def render(self, aria):
        H = self.y + 18
        out = [
            f'<svg viewBox="0 0 {self.W:.0f} {H:.0f}" width="{self.W:.0f}" role="img" '
            f'aria-label="{esc(aria)}" xmlns="http://www.w3.org/2000/svg">'
        ]
        for k in self.lanes:
            x = self.x(k)
            name, sub = LANES[k]
            out.append(
                f'<line class="life" x1="{x:.1f}" y1="{HEAD_H + 6}" x2="{x:.1f}" y2="{H - 6:.0f}"/>'
            )
            out.append(
                f'<rect class="lane-head" x="{x - self.S / 2 + 8:.1f}" y="4" width="{self.S - 16}" '
                f'height="{HEAD_H}" rx="6"/>'
                f'<text class="lane-n" x="{x:.1f}" y="24" text-anchor="middle">{esc(name)}</text>'
                f'<text class="lane-s" x="{x:.1f}" y="40" text-anchor="middle">{esc(sub)}</text>'
            )
        out.extend(self.parts)
        out.append("</svg>")
        return "\n".join(out)


def build(spec):
    d = Diagram(spec["lanes"], spec.get("S", 172))
    for s in spec["steps"]:
        k = s[0]
        if k == "m":
            d.msg(s[1], s[2], s[3], s[4] if len(s) > 4 else "cast", s[5] if len(s) > 5 else ())
        elif k == "a":
            d.act(s[1], s[2], s[3] if len(s) > 3 else "")
        elif k == "n":
            d.note(s[1], s[2], s[3])
        elif k == "p":
            d.phase(s[1])
    return d.render(spec["claim"])


FLOWS = [
    {
        "id": "forwarded",
        "title": "Register through a follower",
        "claim": "A follower forwards the registration to the leader, which commits it, broadcasts the batch, then replies. FIFO puts the batch ahead of the reply, so the follower already holds the binding when it answers yes.",
        "lanes": ["C", "F", "L", "W", "DB", "F2"],
        "steps": [
            ("m", "C", "F", "{register, Name, Pid, Meta}", "call"),
            ("a", "F", "Stores an intent under Ref. It can't decide alone."),
            ("m", "F", "L", "{register_req, Ref, F, Name, Pid, Meta}"),
            ("a", "L", "Enqueues {add, …, {forward, F, Ref}}. Ops that arrive during a commit join the next batch."),
            ("m", "L", "W", "start_commit(epoch E)"),
            ("m", "W", "DB", "fence on leader key {L, E}, bump version", "db"),
            ("m", "DB", "W", "commit version V", "reply"),
            ("m", "W", "L", "{dgen_transaction, Ref, {committed, V}}"),
            ("a", "L", "Applies the batch to its replica. Monitors Pid with tag {down, Name}."),
            ("m", "L", ["F", "F2"], "{names_batch, Ops, E, PrevV, V, L}", "cast", ("accent",)),
            ("a", "F", "PrevV matches its version, so the whole batch applies. Replica is at (E, V)."),
            ("m", "L", "F", "{register_reply, Ref, yes, V, E}", "cast", ("accent",)),
            ("a", "F", "Replica holds V in epoch E, so the yes is released."),
            ("m", "F", "C", "yes", "reply"),
            ("n", "F", "L", "If the batch was refused as a gap, F holds the yes as {awaiting_version, V, E, L} and asks L for a resync. A yes from another epoch never counts."),
        ],
        "notes": [
            "The follower's loop never blocks on the leader. Every hop between members is a cast tagged with <code>Ref</code>.",
            "The leader answers forwarded registrations as soon as they commit. The forwarding follower is the second holder, so no replica ack is needed.",
            "The worker only bumps the version key. Name state lives in members' memory. The leader key read is what fences a deposed leader.",
        ],
        "code": [
            ("register call", MEMBER, 233), ("register_req", MEMBER, 571), ("start_commit/1", MEMBER, 1440),
            ("apply_committed_plan/3", MEMBER, 1480), ("names_batch handler", MEMBER, 302),
            ("register_reply handler", MEMBER, 270),
        ],
    },
    {
        "id": "direct",
        "title": "Register on the leader's node",
        "claim": "A registration made on the leader's own node commits, then waits for register_replicas followers to confirm they hold the batch before the caller gets yes.",
        "lanes": ["C", "L", "W", "F"],
        "S": 196,
        "steps": [
            ("m", "C", "L", "{register, Name, Pid, Meta}", "call"),
            ("a", "L", "Enqueues {add, …, {local, From}}"),
            ("m", "L", "W", "commit batch, fenced on {L, E}"),
            ("m", "W", "L", "{dgen_transaction, Ref, {committed, V}}"),
            ("m", "L", "F", "{names_batch, Ops, E, PrevV, V, L}"),
            ("m", "L", "F", "{replicate_sync, BatchRef, L, V}", "cast", ("accent",)),
            ("a", "L", "Starts the replicate_timeout timer"),
            ("a", "F", "Acks only if its applied version is at least V"),
            ("m", "F", "L", "{replicate_ack, BatchRef, F}", "cast", ("accent",)),
            ("a", "L", "Enough distinct followers acked. Cancels the timer."),
            ("m", "L", "C", "yes", "reply"),
            ("p", "If too few acks arrive in time"),
            ("a", "L", "{replicate_timeout, BatchRef}", "warn"),
            ("m", "L", "C", "yes (degrade-open), or no plus a retract under strict_replication", "reply"),
        ],
        "notes": [
            "<code>replicate_sync</code> is sent after the batch on the same channel, so FIFO delivers it only once the follower has seen the batch.",
            "A follower that refused the batch as a gap stays silent. It never claims to hold bindings it doesn't have.",
            "With no followers at all, the leader answers <code>yes</code> right away.",
        ],
        "code": [
            ("await_replica_acks/3", MEMBER, 1637), ("replicate_sync handler", MEMBER, 280),
            ("replicate_ack handler", MEMBER, 289), ("replicate_timeout handler", MEMBER, 373),
        ],
    },
    {
        "id": "unregister",
        "title": "Unregister and process exit",
        "claim": "An unregister deletes the local row at once and is committed by the leader. When a registered process exits, the leader's tagged monitor turns the exit into the same unregister batch.",
        "lanes": ["C", "F", "L", "W", "F2", "P"],
        "S": 166,
        "steps": [
            ("p", "Explicit unregister through a follower"),
            ("m", "C", "F", "{unregister, Name}", "call"),
            ("a", "F", "Deletes its own row now and notes the released pid"),
            ("m", "F", "L", "{unregister_req, Ref, F, Name}"),
            ("a", "L", "Deletes its row. Enqueues {remove, Name, Pid}."),
            ("m", "L", "W", "commit batch"),
            ("m", "W", "L", "{committed, V}"),
            ("m", "L", ["F", "F2"], "{names_batch, [{name_unregistered, Name, Pid}], …}"),
            ("m", "L", "F", "{unregister_reply, Ref}", "cast", ("accent",)),
            ("m", "F", "C", "ok", "reply"),
            ("n", "C", "L", "With no reachable leader, F answers ok right away and keeps the unregister as an intent. It sends {retract_req, Name, Pid} once a leader is reachable."),
            ("p", "The registered process exits"),
            ("m", "P", "L", "{{down, Name}, MRef, process, Pid, Reason}", "signal", ("accent",)),
            ("a", "L", "Enqueues {down, Name, MRef}. Skipped if Name was rebound under a new monitor."),
            ("m", "L", "W", "commit batch"),
            ("m", "W", "L", "{committed, V}"),
            ("m", "L", ["F", "F2"], "{names_batch, [{name_unregistered, Name, Pid}], …}"),
        ],
        "notes": [
            "Only the leader monitors registered pids. The monitor's custom tag carries the name, so the DOWN says which name to clear.",
            "A retract only clears the name if the same pid still holds it, so it never removes a later registration.",
        ],
        "code": [
            ("route_unregister/3", MEMBER, 1315), ("unregister_req", MEMBER, 581),
            ("{down, Name} handler", MEMBER, 403), ("monitor_name/2", MEMBER, 2217),
        ],
    },
    {
        "id": "reads",
        "title": "Snapshot and consistent reads",
        "claim": "A snapshot read is a lock-free ETS lookup with no messages. A consistent read goes to the leader, and a helper checks the durable leader key before it replies to the caller directly.",
        "lanes": ["C", "T", "F", "L", "H", "DB"],
        "S": 162,
        "steps": [
            ("p", "whereis_name/1"),
            ("m", "C", "T", "ets:lookup(Table, Name)", "call"),
            ("m", "T", "C", "Pid or undefined", "reply"),
            ("p", "whereis_name_consistent/1 on a follower's node"),
            ("m", "C", "F", "{whereis, Name}", "call"),
            ("m", "F", "L", "{whereis_req, Name, From}"),
            ("a", "L", "Reads the value from its replica now"),
            ("m", "L", "H", "spawn verify_leader(L, E)"),
            ("m", "H", "DB", "read leader key", "db"),
            ("m", "DB", "H", "{L, E}", "reply"),
            ("m", "H", "C", "gen_server:reply(From, Pid)", "reply", ("accent",)),
            ("n", "C", "F", "If the leader is unreachable, F answers undefined itself. If the key shows another leader, the helper answers undefined."),
        ],
        "notes": [
            "The snapshot table is <code>protected</code>. The member is its only writer, and callers read it without locking.",
            "<code>get_metadata_consistent/1</code> and <code>query_consistent/2</code> take the same path with their own request tags.",
            "Neither the follower nor the leader waits for the backend. The reply goes straight to the caller's <code>From</code>.",
        ],
        "code": [
            ("whereis_name/1", API, 342), ("forward_read/5", MEMBER, 848),
            ("whereis_req", MEMBER, 602), ("reply_fenced/4", MEMBER, 2037),
        ],
    },
    {
        "id": "join",
        "title": "A member joins",
        "claim": "A joining member announces itself through the elector's durable queue. The elector keeps the sticky leader, and the leader onboards the joiner with one snapshot while existing followers only start monitoring it.",
        "lanes": ["J", "E", "DB", "L", "F"],
        "S": 186,
        "steps": [
            ("m", "J", "E", "{join, J, Token, Fresh = true}", "db"),
            ("a", "E", "In one transaction, adds J and keeps leader L. The epoch stays E."),
            ("m", "E", "DB", "commit state and leader key {L, E}", "db"),
            ("a", "E", "Runs the post-commit action"),
            ("m", "E", "L", "{elector_assume_and_distribute, J, …, E}", "call"),
            ("a", "L", "Same epoch and synced, so it takes the fast path. Monitors J."),
            ("m", "L", "E", "ok", "reply"),
            ("m", "L", "J", "{apply_names_snapshot, Records, L, AllIds, Tokens, E, V}", "cast", ("accent",)),
            ("a", "J", "Adopts the replica at (E, V) and becomes a follower"),
            ("m", "L", "F", "{peer_joined, J, Tokens}"),
            ("a", "F", "Monitors J. Its replica is untouched."),
            ("p", "J synced before, as after a partition"),
            ("a", "L", "A helper fetches J's replica and probes conflicting pids, then sends {joiner_gathered, …}"),
            ("a", "L", "Repairs conflicts (§5.6), then sends the snapshot as above"),
        ],
        "notes": [
            "Every node runs an elector process over the same durable state. Whichever one consumes the <code>join</code> runs the action.",
            "Because the snapshot comes from the leader process, FIFO puts it ahead of every later <code>names_batch</code> the joiner sees.",
            "The first member to join finds no leader, so the elector elects it with a new epoch and it takes the full handoff path.",
        ],
        "code": [
            ("join transaction", ELECTOR, 228), ("push_leader_to_members/1", ACTIONS, 26),
            ("leader assume call", MEMBER, 540), ("assume_leadership_fast_path/6", MEMBER, 869),
            ("cast_data_to_new_member/4", MEMBER, 1016),
        ],
    },
    {
        "id": "failover",
        "title": "Leader failure and handoff",
        "claim": "When the leader dies, survivors report it to the elector, which elects a new leader in a new epoch. The new leader gathers every replica, waits until it has caught up to the durable version, and sends each member a snapshot in the new epoch.",
        "lanes": ["L0", "N", "H", "F", "E", "DB"],
        "S": 166,
        "steps": [
            ("a", "L0", "Process or node dies", "warn"),
            ("m", "L0", ["N", "F"], "'DOWN' on the peer monitor", "signal"),
            ("a", "N", "Sets leader_down. A helper reads L0's current token."),
            ("m", "N", "E", "{member_down, L0, Token}", "db"),
            ("a", "E", "Removes L0 and elects N. Epoch becomes E+1."),
            ("m", "E", "DB", "commit leader key {N, E+1}", "db", ("accent",)),
            ("m", "E", "N", "{elector_assume_and_distribute, undefined, …, E+1}", "call"),
            ("a", "N", "Moves to assuming. Captures its own records and version."),
            ("m", "N", "E", "ok", "reply"),
            ("m", "N", "H", "spawn the gather"),
            ("m", "H", "F", "get_names_snapshot", "call"),
            ("m", "F", "H", "{Records, V, Released, LeaderGone}", "reply"),
            ("m", "H", "DB", "read committed frontier", "db"),
            ("a", "H", "Gathers again every 25 ms until MaxVersion reaches the frontier, every peer reports LeaderGone, or 2 s pass"),
            ("m", "H", "E", "get_subscriptions", "call"),
            ("m", "H", "N", "{assume_gathered, …, Freshest, {MaxVersion, Frontier}, …}"),
            ("a", "N", "Repairs conflicts. Adopts the replica at (E+1, MaxVersion). Becomes leader and starts the monitor sweep."),
            ("m", "N", "F", "{apply_names_snapshot, Records, N, …, E+1, MaxVersion}", "cast", ("accent",)),
            ("a", "F", "Follows N. Releases or re-drives its held replies."),
        ],
        "notes": [
            "F sends its own <code>member_down</code> too. The elector drops the second one because L0 is already gone.",
            "The new leader key fences L0. If L0 is alive but cut off, its next commit aborts.",
            "Ending at the deadline without reaching the frontier marks the leader <code>degraded</code>. A committed binding may be missing, and §5.6 repairs it.",
            "A forwarded registration that reaches N while it is assuming is postponed until the gather finishes.",
        ],
        "code": [
            ("peer DOWN handler", MEMBER, 409), ("member_down transaction", ELECTOR, 237),
            ("elect_leader/5", ELECTOR, 323), ("prepare_to_assume_leadership/6", MEMBER, 887),
            ("gather_until_caught_up/7", MEMBER, 1103), ("assume_gathered", MEMBER, 720),
        ],
    },
    {
        "id": "resync",
        "title": "Gap detection and resync",
        "claim": "Each batch names the version it follows. A follower that sees a gap applies nothing and asks the leader for a snapshot. The heartbeat exposes a lost tail, and a batch from a later epoch always counts as a gap.",
        "lanes": ["L", "F", "N"],
        "S": 250,
        "steps": [
            ("m", "L", "F", "{names_batch, Ops1, E, 10, 20, L}"),
            ("m", "L", "F", "{names_batch, Ops2, E, 20, 30, L}", "cast", ("lost",)),
            ("m", "L", "F", "{names_batch, Ops3, E, 30, 40, L}"),
            ("a", "F", "PrevV 30 doesn't match applied 20. A gap, so it applies nothing.", "warn"),
            ("m", "F", "L", "{resync_req, F}", "cast", ("accent",)),
            ("a", "F", "Won't ask again for 2 s"),
            ("m", "L", "F", "{apply_names_snapshot, Records, L, [], #{}, E, 40}", "cast", ("accent",)),
            ("a", "F", "Re-baselined at (E, 40)"),
            ("p", "A lost tail with no later writes"),
            ("m", "L", "F", "heartbeat {names_batch, [], E, 50, 50, L}"),
            ("a", "F", "The heartbeat is ahead of its version, so it resyncs as above"),
            ("p", "A batch from a later epoch"),
            ("m", "N", "F", "{names_batch, Ops, E+1, PrevV, V, N}"),
            ("a", "F", "A gap even if PrevV matches. Only a snapshot moves a replica into a new epoch.", "warn"),
        ],
        "notes": [
            "Applying batches whole and in order keeps every replica a prefix of the leader's stream. Two replicas at the same version hold the same content.",
            "A batch from an older epoch is dropped as stale, and one at or behind the applied version is a duplicate.",
            "The leader sends the heartbeat every 5 s.",
        ],
        "code": [
            ("offer_batch/5", REPLICA, 123), ("request_resync/2", MEMBER, 1996),
            ("resync_req", MEMBER, 606), ("broadcast_heartbeat/1", MEMBER, 1974),
        ],
    },
    {
        "id": "held-yes",
        "title": "A held yes across a leader change",
        "claim": "A follower holding a yes from epoch E drops it when a snapshot moves it into a later epoch, and sends the registration to the new leader again instead of answering from versions the new leader produced.",
        "lanes": ["C", "F", "L0", "N"],
        "S": 216,
        "steps": [
            ("m", "C", "F", "{register, Name, Pid, Meta}", "call"),
            ("m", "F", "L0", "{register_req, Ref, F, Name, …}"),
            ("a", "L0", "Commits at V in epoch E"),
            ("m", "L0", "F", "{names_batch, …, E, PrevV, V, L0}", "cast", ("lost",)),
            ("m", "L0", "F", "{register_reply, Ref, yes, V, E}"),
            ("a", "F", "Replica isn't at V in E. Holds {awaiting_version, V, E, L0} and asks for a resync."),
            ("a", "L0", "Dies before answering", "warn"),
            ("n", "L0", "N", "N takes over degraded. Its rebuild lacks the commit at V."),
            ("m", "N", "F", "{apply_names_snapshot, …, E+1, MaxVersion}"),
            ("a", "F", "Its epoch moved past E, so the held yes no longer counts. The intent goes back to the leader."),
            ("m", "F", "N", "{register_req, Ref2, F, Name, …}", "cast", ("accent",)),
            ("a", "N", "Commits at V2 in epoch E+1, or refuses"),
            ("m", "N", "F", "{names_batch, …, E+1, PrevV, V2, N}"),
            ("m", "N", "F", "{register_reply, Ref2, Result, V2, E+1}"),
            ("m", "F", "C", "Result", "reply"),
            ("n", "C", "N", "Before this was fixed, F applied N's later-epoch batches and released the old yes once their versions passed V. The caller heard yes for a binding no live member held."),
        ],
        "notes": [
            "A yes from an epoch F has already left is re-dispatched along the current route, not held.",
            "A yes from an epoch F hasn't reached yet is held, and F asks for a resync to get there.",
            "The TLA+ config <code>NoEpochFence</code> turns this off and finds the false ack.",
        ],
        "code": [
            ("on_reply/5", INTENTS, 121), ("on_version/2", INTENTS, 170),
            ("offer_batch/5", REPLICA, 123), ("apply_names_snapshot", MEMBER, 318),
        ],
    },
    {
        "id": "fenced",
        "title": "A deposed leader is fenced",
        "claim": "A leader cut off from the cluster keeps trying to commit. The backend rejects its commit because the leader key names a newer leader, and the old leader hands its work back and rejoins.",
        "lanes": ["C", "L0", "W", "DB", "E"],
        "S": 190,
        "steps": [
            ("n", "L0", "E", "L0 is partitioned. The elector has already moved leadership to N at epoch E+1."),
            ("m", "C", "L0", "{register, Name, Pid, Meta}", "call"),
            ("m", "L0", "W", "commit batch, fenced on {L0, E}"),
            ("m", "W", "DB", "check leader key", "db"),
            ("m", "DB", "W", "{N, E+1}: conflict", "reply", ("accent",)),
            ("m", "W", "L0", "{dgen_transaction, Ref, {aborted, fenced}}", "cast", ("accent",)),
            ("a", "L0", "Retries the ops after 1 s. Once deposed, a local register becomes an intent."),
            ("m", "L0", "E", "{join, L0, Token, false}", "db"),
            ("a", "L0", "Rejoins at most once per epoch"),
            ("n", "L0", "E", "N's snapshot later moves L0 to follower, and its intents go to N."),
        ],
        "notes": [
            "The commit reads the leader key, or adds a read conflict on it. A newer leader key aborts the commit.",
            "A forwarded registration in the failed batch is dropped. Its follower re-drives it to the new leader.",
        ],
        "code": [
            ("handle_commit_result/3", MEMBER, 2053), ("maybe_rejoin/1", MEMBER, 2077),
            ("verify_leader/4", NAMES, 172),
        ],
    },
]


def legend_svg():
    items = [
        ("cast", "cast (async)", "m-cast", False),
        ("call", "call (waits for a reply)", "m-call", True),
        ("reply", "reply", "m-reply", False),
        ("db", "durable: backend or elector queue", "m-db", False),
        ("signal", "monitor signal", "m-signal", False),
    ]
    parts = []
    y = 14
    x = 4
    for _k, text, cls, dot in items:
        parts.append(f'<line class="ln {cls}" x1="{x}" y1="{y}" x2="{x + 40}" y2="{y}"/>')
        parts.append(f'<polygon class="head {cls}" points="{x + 48},{y} {x + 40},{y - 4.5} {x + 40},{y + 4.5}"/>')
        if dot:
            parts.append(f'<circle class="dot {cls}" cx="{x}" cy="{y}" r="3.2"/>')
        parts.append(f'<text class="leg-t" x="{x + 56}" y="{y + 4}">{esc(text)}</text>')
        y += 24
    parts.append(f'<line class="ln m-cast accent" x1="{x}" y1="{y}" x2="{x + 40}" y2="{y}"/>')
    parts.append(f'<polygon class="head m-cast accent" points="{x + 48},{y} {x + 40},{y - 4.5} {x + 40},{y + 4.5}"/>')
    parts.append(f'<text class="leg-t" x="{x + 56}" y="{y + 4}">the hop the flow is about</text>')
    y += 24
    parts.append(f'<line class="ln m-cast" x1="{x}" y1="{y}" x2="{x + 26}" y2="{y}"/>')
    parts.append(f'<path class="lost" d="M{x + 21},{y - 5} L{x + 31},{y + 5} M{x + 31},{y - 5} L{x + 21},{y + 5}"/>')
    parts.append(f'<text class="leg-t" x="{x + 56}" y="{y + 4}">message lost or delayed</text>')
    h = y + 14
    return (f'<svg class="legend" viewBox="0 0 280 {h}" width="280" role="img" '
            f'aria-label="Legend for arrow styles" xmlns="http://www.w3.org/2000/svg">' + "".join(parts) + "</svg>")


# The SVGs are shown through <img>, so each carries its own styles. System font
# stacks only, because an image can't load web fonts. The dark palette follows the
# reader's OS setting, and the opaque background keeps it legible either way.
SVG_CSS = """
:root{--surface:#ffffff;--fg:#1a2226;--muted:#5a676d;--rule:#c9d2d5;--lane:#e6ecee;
--accent:#0a6a86;--durable:#8c5a06;--warn:#b0281f;--note:#f8f4e6;--note-rule:#c9b98a}
@media (prefers-color-scheme: dark){:root{--surface:#172024;--fg:#e1e8ea;--muted:#97a5ab;
--rule:#33424a;--lane:#1e2a30;--accent:#56c2df;--durable:#e2ab52;--warn:#ff8074;
--note:#23241d;--note-rule:#6d6440}}
.bg{fill:var(--surface)}
.leg-t{font-family:system-ui,-apple-system,"Segoe UI",Roboto,sans-serif;font-size:12px;fill:var(--fg)}
.life{stroke:var(--rule);stroke-width:1;stroke-dasharray:3 4}
.lane-head{fill:var(--lane);stroke:var(--rule)}
.lane-n{font-family:system-ui,-apple-system,"Segoe UI",Roboto,sans-serif;font-weight:600;font-size:13.5px;fill:var(--fg)}
.lane-s,.lbl,.phase-t,.num{font-family:ui-monospace,"SF Mono",Menlo,Consolas,"DejaVu Sans Mono",monospace}
.lane-s{font-size:10.5px;fill:var(--muted)}
.ln{stroke-width:1.4;fill:none}
.lbl{font-size:11px;paint-order:stroke;stroke-width:4px;stroke-linejoin:round}
.m-cast,.m-call{stroke:var(--fg);fill:var(--fg)}
.m-reply{stroke:var(--muted);fill:var(--muted)}
.ln.m-reply{stroke-dasharray:5 4}
.m-db{stroke:var(--durable);fill:var(--durable)}
.ln.m-db{stroke-dasharray:1.5 3;stroke-width:1.8;stroke-linecap:round}
.m-signal{stroke:var(--warn);fill:var(--warn)}
.accent{stroke:var(--accent);fill:var(--accent)}
.ln.accent{stroke-width:2.2}
.lbl{stroke:var(--surface)}
.lbl.accent{font-weight:600}
.head,.dot{stroke:none}
.lost{stroke:var(--warn);stroke-width:2;fill:none;stroke-linecap:round}
.act{fill:var(--surface);stroke:var(--rule)}
.act.warn{stroke:var(--warn)}
.act-t,.note-t{font-family:system-ui,-apple-system,"Segoe UI",Roboto,sans-serif;font-size:11.5px;fill:var(--fg)}
.act-t.warn{fill:var(--warn)}
.note{fill:var(--note);stroke:var(--note-rule);stroke-dasharray:4 3}
.note-t{font-style:italic}
.phase-ln{stroke:var(--rule);stroke-width:1}
.phase-bg{fill:var(--surface)}
.phase-t{font-size:10.5px;letter-spacing:.08em;fill:var(--muted)}
.num-dot{fill:none;stroke:var(--rule)}
.num{font-size:10px;fill:var(--muted)}
"""

ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
ASSETS = os.path.join(ROOT, "docs", "design", "assets", "registry_flows")
DOC = os.path.join(ROOT, "docs", "design", "dgen_registry_message_flows.md")


def standalone(svg):
    """Adds the styles and a background to an inline SVG so it works as a file."""
    head, rest = svg.split(">", 1)
    return f'{head}><style>{SVG_CSS}</style><rect class="bg" width="100%" height="100%"/>{rest}\n'


def code_refs(refs):
    by_mod = {}
    for lbl, path, _line in refs:
        mod = os.path.splitext(os.path.basename(path))[0]
        by_mod.setdefault(mod, []).append(lbl)
    return "; ".join(f"`{m}`: " + ", ".join(ls) for m, ls in by_mod.items())


def md_inline(text):
    """Turns the notes' <code> tags into Markdown backticks."""
    return text.replace("<code>", "`").replace("</code>", "`")


def markdown():
    out = [
        "<!-- Generated by docs/tools/registry_flows.py. Edit the script, then run it. -->",
        "",
        "# dgen_registry Message Flows",
        "",
        "Each diagram follows one operation through the processes that take part. Time runs",
        "downward. Message names are the tuples in the code, and `E` and `V` are the epoch",
        "and the commit version. A member's loop never blocks on another member. Calls",
        "between members run in spawned helpers or come from the elector.",
        "",
        "Section numbers like §5.6 refer to the [dgen_registry Design](dgen_registry_design.md).",
        "",
        "![Arrow styles: cast, call, reply, durable, monitor signal, the highlighted hop, and a lost message](assets/registry_flows/legend.svg)",
        "",
        "**This document is LLM generated.**",
        "",
    ]
    for f in FLOWS:
        out += [
            f"## {f['title']}",
            "",
            f["claim"],
            "",
            f"![Swimlane diagram: {f['title'].replace(chr(39), chr(8217))}](assets/registry_flows/{f['id']}.svg)",
            "",
        ]
        out += [f"- {md_inline(n)}" for n in f["notes"]]
        out += ["", f"In the code: {code_refs(f['code'])}.", ""]
    return "\n".join(out)


if __name__ == "__main__":
    os.makedirs(ASSETS, exist_ok=True)
    for f in FLOWS:
        with open(os.path.join(ASSETS, f["id"] + ".svg"), "w") as fh:
            fh.write(standalone(build(f)))
    with open(os.path.join(ASSETS, "legend.svg"), "w") as fh:
        fh.write(standalone(legend_svg()))
    with open(DOC, "w") as fh:
        fh.write(markdown())
    print("wrote", DOC)
