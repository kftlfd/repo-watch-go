package events

// ReplayPolicy describes whether a dead-lettered event can be replayed
// automatically by operational tooling such as cmd/dlqctl.
//
// Replay safety is not the same as schema validity. An event can be valid and
// still be unsafe to replay because consumers rely on ordering, versioning, or
// idempotency that a replay can violate.
type ReplayPolicy struct {
	Unsafe bool
	Reason string
}

// replayPolicies lists event types that must not be replayed automatically.
// Every other event type is considered safe to replay.
var replayPolicies = map[string]ReplayPolicy{
	// repository.removed has no ordering/version metadata. Replaying an old
	// removal after a later re-subscribe can deactivate the repository
	// incorrectly. Revisit after the repository removal ordering/tombstone
	// redesign.
	EventRepositoryRemoved: {
		Unsafe: true,
		Reason: "repository removal has no ordering/version metadata; replaying it after a later re-subscribe can deactivate the repository incorrectly",
	},

	// The mailer has no idempotency store. Replaying email.queued after a
	// partially successful delivery can send duplicate emails.
	EventEmailQueued: {
		Unsafe: true,
		Reason: "the mailer has no idempotency store; replaying can send duplicate emails",
	},
}

// ReplayPolicyFor returns the replay classification for eventType. Event types
// without an entry are safe to replay.
func ReplayPolicyFor(eventType string) ReplayPolicy {
	return replayPolicies[eventType]
}
