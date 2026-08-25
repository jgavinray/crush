package mcp

import (
	"context"
	"sync"

	"github.com/charmbracelet/crush/internal/pubsub"
)

// Bus channel routing (SPEC §16 Q9-a/Q9-b, §17 P3).
//
// When the mailbox-bus MCP server pushes a notifications/claude/channel
// event, the bus knows only the recipient agent_id — not the Crush
// session that owns it. The binding is learned where the identity is
// available: the tool executor. Every session registers itself on the bus
// with its agent_id as its first action (SPEC §11.1), and that register
// call runs inside the session's agent loop, where the session ID is in
// the context (SPEC §16 Q9-a: "the identity is available on the server
// side"). BindAgentSession + AgentSession + SubscribeChannelEvents
// together let the app layer route a push to the owning session and start
// a fresh agent turn from it (SPEC §16 Q9-b: the message self-drives the
// receiving agent; no host-side loop wrapper required).

var agentBindings sync.Map // agent_id (string) -> session ID (string)

// BindAgentSession records that sessionID registered agentID on the bus.
// Re-registering (reconnect, new session for the same agent) overwrites
// the binding with the newest session — the bus re-pushed mail belongs to
// whoever registered last, and only that session's agent loop can answer
// it.
func BindAgentSession(agentID, sessionID string) {
	if agentID == "" || sessionID == "" {
		return
	}
	agentBindings.Store(agentID, sessionID)
}

// AgentSession returns the session ID that registered agentID on the bus.
func AgentSession(agentID string) (string, bool) {
	v, ok := agentBindings.Load(agentID)
	if !ok {
		return "", false
	}
	sid, ok := v.(string)
	return sid, ok
}

// ForgetAgentSession drops a learned binding (used by tests).
func ForgetAgentSession(agentID string) {
	if agentID != "" {
		agentBindings.Delete(agentID)
	}
}

// SubscribeChannelEvents returns a stream of channel-message events only.
// The process-wide SubscribeEvents fan-out deliberately excludes
// EventChannelMessage: channel pushes carry no workspace or session
// identity and the MCP broker is process-global, so fanning them out would
// let every workspace receive every other workspace's mail — a
// cross-workspace injection path. P3 routing consumes this dedicated
// stream instead and resolves the owning session via the agent bindings.
func SubscribeChannelEvents(ctx context.Context) <-chan pubsub.Event[Event] {
	raw := broker.Subscribe(ctx)
	out := make(chan pubsub.Event[Event], 64)
	go func() {
		defer close(out)
		for ev := range raw {
			if ev.Payload.Type != EventChannelMessage {
				continue
			}
			select {
			case out <- ev:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}
