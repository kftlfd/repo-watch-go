# Subscription database

## Tables and relationships

```text
users ────────< user_tokens
  │
  └──────────< sessions ────────< refresh_tokens

repositories ─< subscriptions ─< subscription_tokens

outbox            (standalone)
processed_events  (standalone)
```

## Lock-order rules

PostgreSQL holds row locks until transaction commit. Locks acquired by `UPDATE`, `DELETE`, and `INSERT ... ON CONFLICT DO UPDATE` matter just as much as explicit `SELECT ... FOR UPDATE` locks. A plain `SELECT` does not lock a row.

Use these lock-acquisition orders consistently:

```text
users         -> user_tokens
sessions      -> refresh_tokens
subscriptions -> subscription_tokens -> repositories
```

The last subscription transition step updates the repository aggregate counter. Although a repository is the relational parent of its subscriptions, it must not be locked before an existing subscription that is being activated or deactivated.

When adding a transaction with more than one row lock, document its order here and add a real-PostgreSQL concurrency regression for operations that can acquire the same rows.

### Token-consuming operations

A token lookup must not lock the token before its parent. Instead:

1. Find the token by hash **without locking it** to discover its owner.
2. Lock the parent row.
3. Find the token again with `FOR UPDATE`.
4. Revalidate the locked token against the discovered parent and all original validity conditions. The first result is only a candidate and can become stale while waiting for the parent lock.

Use these query sequences:

| Operation | Required sequence |
| --- | --- |
| Verify email | `FindUserToken` → `GetUserByEmailForUpdate` → `FindUserTokenForUpdate`; verify the user email still matches before consuming the token. |
| Refresh or revoke by refresh token | `GetRefreshTokenByHash` → `GetSessionByIDForUpdate` → `GetRefreshTokenByHashForUpdate`; verify the session ID still matches. Both operations use `lockSessionAndRefreshToken`. |
| Confirm subscription | `FindSubscriptionTokenWDate` → `GetSubscriptionByIDForUpdate` → `FindSubscriptionTokenWDateForUpdate`; verify the subscription ID and reuse the same creation-time cutoff. |
| Unsubscribe by token | `FindSubscriptionToken` → `GetSubscriptionByIDForUpdate` → `FindSubscriptionTokenForUpdate`; verify the subscription ID still matches. |

Do not replace these sequences with an initial `Find*ForUpdate` call to save a query. That recreates token-before-parent locking.

Signup and resend verification already lock or insert the user before `UpsertUserToken`. Session creation likewise inserts the session before its refresh token. Keep those parent-before-dependent orders.

### Subscription and repository transitions

Activation and deactivation use this order:

1. Lock or conditionally update the `subscriptions` row. `ActivateSubscription` and `DeactivateSubscription` acquire the row lock even when the preceding read was unlocked.
2. Delete, insert, or update its `subscription_tokens` rows.
3. Update the repository counter with `IncrementRepositoryActiveCount` or `DecrementRepositoryActiveCount`.
4. Append transactional-outbox rows.

`CreateRepository` intentionally uses `ON CONFLICT DO NOTHING`. Do not change it to a conflicting-row `DO UPDATE`: doing so would lock an existing repository before the subscription transition, reversing the required order. Inserting a genuinely new repository first is safe because competing transactions cannot lock that uncommitted row independently; a conflicting insert waits for its creator.

Repository-removal processing in `internal/subscription/inbox` must preserve the same order:

1. `ClaimProcessedEvent`, before taking any domain-row locks.
2. `DeactivateSubscriptionsForRepoAndRemoveSubscriptionTokens`.
3. `MarkRepositoryRemoved`.

In particular, do not move `MarkRepositoryRemoved` ahead of subscription deactivation. The reverse order can deadlock with either token-based or authenticated unsubscription.
