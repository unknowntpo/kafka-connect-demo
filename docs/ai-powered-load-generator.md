# AI-Powered Load Generator Design

## Goal

The generator should create realistic event streams for demo dashboards without hard-coding one fixed traffic curve.

The practical design is two-stage:

```text
LLM / AI planner
    |
    | generates a load profile JSON
    v
Deterministic Java event generator
    |
    | executes the profile into Kafka
    v
Kafka Connect -> Elasticsearch -> Kibana
    |
    | score-load-profile.sh
    v
Feedback loop and profile tuning
```

The LLM should not generate every event one by one. That would be slow, expensive, hard to reproduce, and hard to test. Instead, AI generates the scenario characteristics: phases, event weights, inventory, failure modes, user scale, and time-shape. The Java generator turns those characteristics into repeatable events.

## Example Scenario: Flash-Sale Coupon

User-facing story:

```text
A limited discount coupon drops. Many users refresh the page, enter a waiting room,
some claim coupons successfully, then coupon inventory reaches zero and failures spike.
```

The profile lives at:

```text
profiles/flash-sale-coupon.json
```

Important profile fields:

- `total_events`: target event count.
- `duration_seconds`: historical time window to generate.
- `inventory`: limited coupon quantity.
- `time_skew_power`: controls how strongly traffic concentrates near the end of the window.
- `phases`: ordered behavioral phases such as teaser, waiting room, drop open, and sold-out pressure.
- `event_weights`: weighted event distribution per phase.
- `sold_out_event_weights`: event distribution after inventory reaches zero.
- `failure_weights`: realistic failure reasons for that phase.

## Why AI Helps

AI is useful for generating the load profile, not replacing the event generator.

Good AI-generated profiles can encode:

- Business context: coupon drop, product launch, checkout outage, influencer campaign.
- Behavioral phases: warmup, queue buildup, release, sold-out pressure, long tail.
- Event mix: refreshes, views, queue joins, claim attempts, successes, failures.
- Constraint causality: limited coupon inventory creates later `COUPON_SOLD_OUT` failures.
- User realism: many users, uneven activity, but no single user dominates the whole stream.
- Data quality cases: malformed records can still be injected to verify DLQ behavior.

## Feedback Loop

The feedback loop is implemented with Elasticsearch queries:

```bash
./scripts/score-load-profile.sh
```

The scorer checks:

- Total event volume is large enough for dashboard trends.
- Last 30 minutes have much more traffic than the first 30 minutes.
- Refreshes and waiting-room events exist.
- Coupon claims succeed until inventory reaches zero.
- Sold-out failures dominate after inventory depletion.
- User distribution is broad enough.
- Kafka Connect SMT fields exist on every indexed document: `pipeline=connect-search-demo` for provenance and `metadata_region` for dashboard grouping.

To generate and score the current profile:

```bash
./scripts/seed-ai-load-profile.sh
```

The script cleans demo state by default, recreates mappings, registers the connector, creates the Kibana dashboard, generates profile events through Kafka, waits for indexing, and prints the score. Cleanup removes the connector, Kafka data topics, Kafka Connect internal topics, and the Elasticsearch index. The included flash-sale profile generates 80 minutes of history against a deterministic default base time, `2026-05-01T12:00:00Z`, so repeated runs produce the same aggregate results.

## Extension Path

A real LLM integration can produce files with the same JSON contract:

```text
profiles/<scenario>.json
```

Example prompt shape:

```text
Generate a Kafka event load profile for a limited coupon flash sale.
Return JSON only. Include phases, event weights, failure weights, inventory,
duration, total event count, and expected dashboard signals.
```

The deterministic generator and scorer do not need to change when a new AI-generated profile is added.
