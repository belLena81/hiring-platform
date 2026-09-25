# MongoDB design

## Candidate matching search

Candidate profiles persist optional `currentResidence`, `availabilityStatus`, and `recruiterSearchOptIn` fields. Location also stores `countryCanonical` and optional `cityCanonical` sidecars for case-insensitive equality filters. Skills retain their original spelling for profile display and have a `skillsCanonical` sidecar for all-required skill filters. None of these fields is part of the embedding text.

Candidate search uses a new Atlas vector index (`candidates_embedding_vector_match_v1`) separate from the legacy candidate recommendation index. Its filter paths cover role, active-account status, embedding model, canonical skills, consent, canonical country/city, and availability. Candidate lexical search uses the fixed `profile.skills` and `profile.experienceSummary` fields through `candidates_text_search`. Index setup waits for queryability and verifies the returned definition; a same-name incompatible definition fails startup without dropping or replacing existing indexes.

Candidate matches return only ID, name, skills, experience summary, embedding metadata, and score from persistence. Residence/status/consent are available through the authenticated candidate's own profile only. Recruiter search telemetry stores the job identifier but omits private filter values.

Private residence/availability filters use an OR predicate: an opted-in candidate must satisfy every supplied private filter; a profile with opt-in false or absent bypasses only these private predicates. All other retrieval predicates, including Candidate role, Active account, model, and required skills, apply to every branch before its candidate limit.

## Candidate profile migration 002

Before deploying this schema, stop every older application binary. Older profile updates replace the full profile and can erase fields introduced here. The startup migration `002_candidate_search_profile_verification` scans users in bounded `_id` batches, checkpoints after each verified batch, allows absent private fields, rejects malformed present residence/status/consent values, and derives canonical skills from the existing skill list. It marks completion only after the scan. A failed validation leaves the migration incomplete; repair the malformed record and restart. Re-running a completed migration skips the scan. It never infers or backfills location, availability, or consent.

Profile writes validate the same residence limits and enum values. Legacy absent consent decodes as false. The migration is forward-only; application rollback is not a data rollback. Keep new indexes until a separately reviewed retirement plan allows removal of the old index.
