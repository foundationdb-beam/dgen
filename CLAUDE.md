# Writing comments and docs

These rules apply to code comments, `-moduledoc`/`-doc` strings, Elixir
`@moduledoc`/`@doc`, and the Markdown design docs.

- Write the way you'd explain the code to a colleague out loud.
- Use full sentences with a subject that does something: "We drop the reply",
  not "Reply dropped".
- Don't use field or type names as English words ("an intent is a kind in a
  phase"). When you name a field, variable or atom, put it in backticks and use
  it as a name.
- No stacked possessives ("its forward's reference"). No parentheticals that
  carry the main point. Don't use semicolons or colons to glue sentences
  together; write two sentences.
- One idea per sentence. Say why when the why isn't obvious.
- Function comments start with a third-person verb: "Sends...", "Returns...".
- Avoid shorthand jargon ("CP:", "adjudicated", "owed", "surrendered") unless
  it is a term this codebase defines. Use defined terms (for example "fenced",
  §5.1 of the registry design doc) consistently.
- Don't be wordy. Don't comment what the code already says, and don't turn a
  short, accurate comment into a paragraph.
- Keep `§N.N` references to `docs/design/dgen_registry_design.md` accurate, and
  don't renumber its sections.
- Keep the "LLM-generated" disclaimer on files that carry it.

Example. Before:

```erlang
%% Drop timed-out register/set_meta intents so a long leaderless window cannot grow
%% the map. Unregisters are kept: their effect is owed whether or not anyone waits.
```

After:

```erlang
%% Drops timed-out registers and set_metas so the map can't grow during a long
%% stretch without a leader. Unregisters are kept, because the name must still be
%% removed even when nobody is waiting.
```
