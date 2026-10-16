# Configuration

For basic configuration instructions, see [this documentation](https://developers.openai.com/codex/config-basic).

For advanced configuration instructions, see [this documentation](https://developers.openai.com/codex/config-advanced).

For a full configuration reference, see [this documentation](https://developers.openai.com/codex/config-reference).

## Lifecycle hooks

Admins can set top-level `allow_managed_hooks_only = true` in
`requirements.toml` to ignore user, project, and session hook configs while
still allowing managed hooks from requirements and managed config layers. This
setting is only supported in `requirements.toml`; putting it in `config.toml`
does not enable managed-hooks-only mode.

## Unload completed multi-agent V2 children

An optional delay releases idle child runtimes after completion, error, or
interruption. The root runtime remains loaded. Omit this key to disable automatic
unload. A value of `0` requests unload as soon as the terminal result is delivered
and saved history is ready for reload.

```toml
[features.multi_agent_v2]
enabled = true
completed_agent_unload_delay_secs = 60
```

Enable this setting only in a profile whose MCP servers and tools can restart
without losing required state. Unload stops the child session and its MCP clients.
Saved conversation history, queue-only mail, and environment selections survive.
Codex does not restore transient REPL variables, subscriptions, or server-side tasks.

An active turn, durable sleep, trigger mail, background terminal, active code-mode
cell, approval, or active descendant blocks unload. Codex retries a blocked unload
once after five seconds, then keeps the runtime loaded. Resume or a new turn cancels
the pending deadline. A later terminal turn can set a new deadline.

Follow-up input waits for an unload in progress, then reloads the child from saved
history. Codex does not replay input that the old session already accepted.
This setting does not change concurrency limits or capacity-driven eviction.
