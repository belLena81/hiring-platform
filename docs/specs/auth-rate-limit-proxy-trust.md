# Auth Rate Limit Proxy Trust

Status: implemented pending final local verification and independent review.

## Scope and Decisions

- User outcome: authentication rate limits distinguish clients behind a known reverse proxy without trusting client-supplied address headers from direct callers.
- Scope: HTTP transport address resolution, packaged configuration, rate-limit route wiring, focused tests, and operator documentation.
- Non-goals: a distributed/shared limiter, proxy provisioning, GraphQL schema changes, persistence, migrations, or client-IP diagnostics.
- Contract: `http.trusted-proxy-cidrs` defaults to `[]`. Only when `request.remoteAddr` belongs to this allowlist does the transport inspect forwarding headers. RFC 7239 `Forwarded` takes precedence; typed `X-Forwarded-For` is the fallback for proxies that do not emit `Forwarded`. Both chains are scanned from right to left, skipping malformed, obfuscated, or configured-proxy addresses. Missing, entirely unusable, or all-proxy chains fall back to the TCP peer; a missing peer is represented as `None`. `/0` CIDRs are rejected.
- Contract: login, signup, and admin bootstrap use process-local token buckets keyed by typed remote address and operation. Each bucket has `attempts` capacity and refills by `attempts` every `window-seconds`; `max-buckets` bounds memory and causes replacement of an existing entry rather than fail-closed rejection. Exhausted buckets return HTTP 429 with `Retry-After`.

## Acceptance Criteria

| ID | Given / When / Then | Evidence |
|---|---|---|
| ARPT-AC01 | Given no configured trusted proxy or an untrusted TCP peer, when `Forwarded` or `X-Forwarded-For` is supplied, then the limiter key remains the TCP peer or `None` | `ClientAddressResolverSpec`; route regression |
| ARPT-AC02 | Given a trusted proxy and a valid concrete forwarding chain, when login or signup is called, then distinct clients receive distinct operation-specific token buckets | resolver and `HiringApiRoutesSpec` regressions |
| ARPT-AC03 | Given a trusted TCP peer and forwarding metadata containing malformed, unknown, obfuscated, or incomplete hops, when at least one parseable non-proxy hop exists, then resolution uses the nearest such hop; otherwise it falls back to the TCP peer | `ClientAddressResolverSpec` |
| ARPT-AC04 | Given packaged/default or overridden configuration, when CIDRs are decoded, then `[]`, IPv4, IPv6, and single-host ranges are accepted while malformed and `/0` ranges fail safely | `AppConfigSpec` |
| ARPT-AC05 | Given a full limiter cache or concurrent requests for one key, when permits are requested, then new keys are admitted and per-key token consumption remains bounded without a shared whole-map CAS loop | `AuthRateLimiterSpec` |
| ARPT-AC06 | Given both trusted forwarding headers, when they identify different clients, then the usable `Forwarded` address determines the limiter key | `ClientAddressResolverSpec` |

## Operational Assumptions

- Each configured immediate proxy strips or replaces inbound forwarding headers and appends its observed source peer.
- Only configured proxy networks can directly reach the application; direct application access is restricted at the network layer.
- Token buckets remain process-local. Cross-replica limits require a separately scoped shared-store design.
