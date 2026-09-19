# Auth Rate Limit Proxy Trust

Status: implemented pending final local verification and independent review.

## Scope and Decisions

- User outcome: login and signup rate limits distinguish clients behind a known reverse proxy without trusting client-supplied address headers from direct callers.
- Scope: HTTP transport address resolution, packaged configuration, rate-limit route wiring, focused tests, and operator documentation.
- Non-goals: a distributed/shared limiter, proxy provisioning, `X-Forwarded-For` support, GraphQL schema changes, persistence, migrations, or client-IP diagnostics.
- Contract: `http.trusted-proxy-cidrs` defaults to `[]`. Only when `request.remoteAddr` belongs to this allowlist does the transport inspect RFC 7239 `Forwarded`. It requires concrete IP `for=` values throughout the typed chain, scans from right to left, skips configured proxy CIDRs, and uses the first non-proxy address. Missing, malformed, unknown, obfuscated, incomplete, or all-proxy chains fall back to the TCP peer; a missing peer is `unknown`. `/0` CIDRs are rejected.

## Acceptance Criteria

| ID | Given / When / Then | Evidence |
|---|---|---|
| ARPT-AC01 | Given no configured trusted proxy or an untrusted TCP peer, when `Forwarded` is supplied, then the limiter key remains the TCP peer or `unknown` | `ClientAddressResolverSpec`; route regression |
| ARPT-AC02 | Given a trusted proxy and a valid concrete `Forwarded` chain, when login or signup is called, then distinct clients receive distinct operation-specific fixed-window buckets | resolver and `HiringApiRoutesSpec` regressions |
| ARPT-AC03 | Given malformed, unknown, obfuscated, incomplete, or multi-line-invalid forwarding metadata, when the TCP peer is trusted, then resolution fails closed to that peer | `ClientAddressResolverSpec` |
| ARPT-AC04 | Given packaged/default or overridden configuration, when CIDRs are decoded, then `[]`, IPv4, IPv6, and single-host ranges are accepted while malformed and `/0` ranges fail safely | `AppConfigSpec` |

## Operational Assumptions

- Each configured immediate proxy strips or replaces inbound `Forwarded` and appends its observed source peer.
- Only configured proxy networks can directly reach the application; direct application access is restricted at the network layer.
- Fixed-window buckets remain process-local. Cross-replica limits require a separately scoped shared-store design.
