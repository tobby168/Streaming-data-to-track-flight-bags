# Local POC: Baggage Tracking Ops (Kafka + Flink + ClickHouse + Grafana) on Kubernetes

把完整 design doc 放進去

Below is a single, concrete plan (no branching options) you can hand to a coding agent. It assumes Kubernetes deployment and uses Kafka + Flink + ClickHouse + Grafana with a mock data generator pod. Transfer connections are explicitly modeled.

⸻

## 1) Goal

Build a local, runnable POC that:
- Generates realistic baggage events for a chosen airport/day, including transfer (multi-leg) itineraries.
- Processes events in real time with Flink (event-time, dedupe, state machine).
- Persists results to ClickHouse.
- Visualizes KPIs and triggers alarms in Grafana.

Deliverables:
- Kubernetes manifests (or Helm charts) to deploy everything to a local cluster.
- Working end-to-end data flow: Mock Generator → Kafka → Flink → ClickHouse → Grafana.

Assume local K8s: kind (recommended) + kubectl + helm.

⸻

## 2) Fixed Architecture (chosen, no alternatives)

### Components
1. Kafka (KRaft) via Helm chart
2. Flink cluster (JobManager + TaskManagers) deployed as K8s Deployments
3. ClickHouse via Helm chart
4. Grafana via Helm chart
5. Mock Generator: one Deployment producing to Kafka

### Data Flow
- Mock Generator publishes:
  - flight.schedule.v1 (compacted)
  - baggage.assignment.v1 (compacted)
  - baggage.events.v1 (retention)
- Flink Jobs:
  - Job A: BagLatestJob → ClickHouse bag_latest (+ Kafka baggage.bag.latest.v1 compacted)
  - Job B: FlightKpiAlertJob → ClickHouse flight_kpis, alerts (+ Kafka baggage.alerts.v1)
  - Job C: ItineraryStatusJob (transfer-aware) → ClickHouse itinerary_status, notifications (+ Kafka baggage.notifications.v1)
- Grafana dashboards query ClickHouse tables and Grafana alerting uses ClickHouse queries.

⸻

## 3) Kafka Topics (fixed)

Create topics at deployment time (use an init Job or Kafka chart topic provisioning).

### Topics
1. flight.schedule.v1 (compacted)
   - Key: flight_id
   - Value: schedule + cutoff + airport

2. baggage.assignment.v1 (compacted)
   - Key: bag_id
   - Value: itinerary_id, customer_id, legs[] metadata

3. baggage.events.v1 (retention)
   - Key: bag_id
   - Value: canonical baggage event

4. baggage.bag.latest.v1 (compacted)
   - Key: bag_id
   - Value: latest bag state

5. baggage.alerts.v1 (retention)
   - Key: flight_id|rule_id|window_start
   - Value: alert payload

6. baggage.notifications.v1 (retention)
   - Key: itinerary_id|type|version
   - Value: notification payload

⸻

## 4) Canonical Event Schemas (JSON)

### 4.1 baggage.events.v1 (required)

```
{
  "event_id": "uuid",
  "bag_id": "BAG000123",
  "itinerary_id": "ITI000045",
  "customer_id": "CUST000045",
  "flight_id": "BR025|2026-01-04|LHR->AMS",
  "leg_index": 0,
  "airport": "LHR",
  "event_type": "MakeupArrived",
  "event_time": "2026-01-04T08:21:00Z",
  "ingest_time": "2026-01-04T08:21:02Z",
  "scan_point_id": "LHR_MAKEUP_12",
  "reader_type": "RFID",
  "confidence": 0.99,
  "attributes": {
    "security_result": "CLEARED",
    "container_id": "AKE12345BR"
  }
}
```

event_type enum (fixed)
- CheckedIn
- Inducted
- ScreenedCleared
- ScreenedAlarm (rare)
- MakeupArrived
- LoadedOnAircraft
- Offloaded
- CarouselArrived
- Transfer-aware:
  - TransferIn
  - ReassociatedToNextLeg

### 4.2 flight.schedule.v1 (required)

```
{
  "flight_id": "BR025|2026-01-04|LHR->AMS",
  "airport_origin": "LHR",
  "airport_dest": "AMS",
  "dep_time": "2026-01-04T10:00:00Z",
  "arr_time": "2026-01-04T11:10:00Z",
  "bag_cutoff_time": "2026-01-04T09:20:00Z",
  "expected_bags": 220
}
```

### 4.3 baggage.assignment.v1 (required)

```
{
  "bag_id": "BAG000123",
  "itinerary_id": "ITI000045",
  "customer_id": "CUST000045",
  "bags_in_itinerary": 2,
  "legs": [
    {
      "leg_index": 0,
      "flight_id": "BR025|2026-01-04|LHR->AMS",
      "origin": "LHR",
      "dest": "AMS"
    },
    {
      "leg_index": 1,
      "flight_id": "BR610|2026-01-04|AMS->FRA",
      "origin": "AMS",
      "dest": "FRA"
    }
  ]
}
```

⸻

## 5) State Models (fixed)

### 5.1 Bag stage state machine (per leg)

Ordered stages:
1. CHECKED_IN
2. INDUCTED
3. CLEARED
4. MAKEUP
5. LOADED
6. OFFLOADED
7. CAROUSEL

Stage mapping from events:
- CheckedIn → CHECKED_IN
- Inducted → INDUCTED
- ScreenedCleared → CLEARED
- MakeupArrived → MAKEUP
- LoadedOnAircraft → LOADED
- Offloaded → OFFLOADED
- CarouselArrived → CAROUSEL

Rules:
- State is monotonic forward. Ignore backward transitions.
- Missing scans allowed: if a later stage appears, move forward directly.
- Dedupe by event_id.

### 5.2 Journey state (itinerary-level, transfer-aware)

journey_state enum (fixed):
- CHECKED_IN
- IN_ORIGIN_PROCESSING
- ON_FLIGHT
- ARRIVED_AT_HUB
- IN_TRANSFER_PROCESSING
- AT_RISK_FOR_CONNECTION
- LOADED_ON_NEXT_FLIGHT
- ARRIVED_AT_DESTINATION
- ON_CAROUSEL
- DELAYED_OR_REROUTED
- EXCEPTION

Journey derivation:
- Based on the worst/least advanced bag in the itinerary and the current leg.
- If any bag hits ScreenedAlarm → EXCEPTION (until cleared; for POC keep as terminal).
- When first leg OFFLOADED at hub and next leg not yet MAKEUP → IN_TRANSFER_PROCESSING.

AT_RISK rule (fixed, deterministic):
- Let t_cutoff_next from flight.schedule for next leg.
- Let now = processing time in Flink (POC).
- Let TRANSFER_P90_MINUTES = 35 and BUFFER_MINUTES = 10.
- If bag is not at least MAKEUP on next leg AND (t_cutoff_next - now) < (TRANSFER_P90_MINUTES + BUFFER_MINUTES)
  → AT_RISK_FOR_CONNECTION.
- If now > t_cutoff_next and bag not LOADED on next leg → DELAYED_OR_REROUTED.

⸻

## 6) Flink Jobs (fixed implementation details)

### Common settings
- Watermark strategy: bounded out-of-orderness = 10 minutes
- Checkpointing enabled: every 30 seconds
- State backend: RocksDB (embedded)
- Event-time uses event_time

### 6.1 Job A: BagLatestJob

Input: baggage.events.v1
Key: bag_id

State per bag:
- latest_stage (enum)
- last_event_time
- last_scan_point
- current_flight_id
- current_leg_index
- Dedupe: MapState<event_id, true> with TTL 48 hours

Outputs:
1. Kafka baggage.bag.latest.v1 (compacted) with full latest bag record
2. ClickHouse bag_latest append record every time the stage changes (or at least every update)

### 6.2 Job B: FlightKpiAlertJob

Inputs:
- baggage.events.v1
- flight.schedule.v1 as BroadcastState keyed by flight_id

Key: flight_id (use event.flight_id)

State per flight:
- MapState<bag_id, stage> (latest stage per bag for distinct counting)
- Counters derived on the fly: count of bags with stage >= each stage
- last_update_time

Emit KPI snapshot every 10 seconds to ClickHouse flight_kpis.

Alerts (emit to Kafka + ClickHouse) every 10 seconds evaluation:
- Rule LOW_LOAD_RATE (WARN):
  - if bag_cutoff_time - now < 20min AND loaded / expected_bags < 0.95
- Rule MISSED_CUTOFF (CRITICAL):
  - if now > bag_cutoff_time AND loaded < expected_bags

Alert payload includes:
- flight_id, severity, rule_id, now, expected_bags, loaded, missing, cutoff_time

### 6.3 Job C: ItineraryStatusJob

Inputs:
- baggage.bag.latest.v1
- baggage.assignment.v1
- flight.schedule.v1 as BroadcastState

Join:
- Keyed by bag_id to enrich bag_latest with assignment (itinerary_id, legs)

Then re-key by itinerary_id to compute:
- bags_total
- journey_state (per rules above)
- current_leg_index (from assignment + bag state)
- risk_flag if any bag AT_RISK_FOR_CONNECTION

Notification emission:
- Emit a notification only when journey_state changes for the itinerary.
- Output to Kafka baggage.notifications.v1 and ClickHouse notifications.

⸻

## 7) ClickHouse (fixed tables)

### 7.1 Database
- database: baggage_poc

### 7.2 Tables (Append-only)

Use MergeTree with partitions by date (toDate(ts)).

**flight_kpis**

Columns:
- ts DateTime
- flight_id String
- dep_time DateTime
- bag_cutoff_time DateTime
- expected_bags UInt32
- checked_in UInt32
- inducted UInt32
- cleared UInt32
- makeup UInt32
- loaded UInt32
- loaded_pct Float32
- missing UInt32
- freshness_sec UInt32

**bag_latest**

Columns:
- ts DateTime
- bag_id String
- itinerary_id String
- customer_id String
- flight_id String
- leg_index UInt8
- stage String
- last_scan_point String
- last_event_time DateTime

**alerts**

Columns:
- created_at DateTime
- alert_id String
- flight_id String
- severity String
- rule_id String
- payload_json String

**itinerary_status**

Columns:
- ts DateTime
- itinerary_id String
- customer_id String
- bags_total UInt8
- journey_state String
- current_leg_index UInt8
- risk_flag UInt8
- details_json String

**notifications**

Columns:
- created_at DateTime
- notification_id String
- itinerary_id String
- customer_id String
- journey_state String
- payload_json String

⸻

## 8) Grafana (fixed dashboards + alerts)

### 8.1 Data source
- ClickHouse data source pointing to baggage_poc

### 8.2 Dashboards

1. Flight Ops Overview
   - Table: flights in next 6 hours sorted by missing desc, loaded_pct asc
   - Time series: loaded_pct over time for selected flight
   - Funnel-ish panel: latest snapshot stage counts (checked_in→loaded)

2. Flight Drilldown
   - Missing bags list: join/filter bag_latest for bags not loaded for that flight (latest state only)

3. Itinerary View
   - Table: itineraries where journey_state in (AT_RISK_FOR_CONNECTION, DELAYED_OR_REROUTED, EXCEPTION)
   - Detail panel: latest bags and their stages for an itinerary

### 8.3 Grafana Alert rules
- CRITICAL: alerts table contains severity='CRITICAL' within last 2 minutes
- WARN: alerts table contains severity='WARN' within last 2 minutes
- Pipeline freshness: max(freshness_sec) > 60 seconds in last 2 minutes

⸻

## 9) Mock Generator (fixed behavior)

Fixed constants
- Airport: LHR
- Day: today (UTC)
- Flights generated: 120
- Passengers per flight: mean 160, std 30, truncated [80, 240]
- Bag count distribution per passenger:
  - 0 bags: 0.15
  - 1 bag : 0.55
  - 2 bags: 0.25
  - 3 bags: 0.05
- Transfer ratio: 0.30 itineraries have 2 legs (hub = AMS)
- MCT distribution (hub connection): truncated normal mean 80 min, std 20, min 45, max 180
- Event-time latency distributions (per bag):
  - CheckedIn occurs at dep_time - trunc_norm(mean 120m, std 30m, [60m, 240m])
  - Inducted: + lognormal(p50 5m, p90 15m)
  - ScreenedCleared: + lognormal(p50 3m, p90 10m)
  - MakeupArrived: + lognormal(p50 10m, p90 25m)
  - LoadedOnAircraft: scheduled so most bags are loaded before cutoff - 5m
- Data imperfections:
  - out_of_order_rate = 0.05 (5% of events delayed in ingest by 2–6 minutes)
  - duplicate_rate = 0.02 (2% of events duplicated)
  - missing_scan_rate = 0.02 (2% skip exactly one intermediate stage)
- Failure injection:
  - miss_connection_rate = 0.02 for transfer bags (causes next-leg Loaded missing)

Outputs
- Publish flight schedules to flight.schedule.v1
- Publish bag assignments to baggage.assignment.v1
- Publish events to baggage.events.v1 in event-time order, but with ingest-time delays for out-of-order simulation

⸻

## 10) Kubernetes Deployment Plan (fixed)

Use kind cluster named baggage-poc.

### Namespaces
- baggage-poc

### Helm installs (fixed)
- Kafka (Bitnami Kafka with KRaft)
- ClickHouse (Bitnami ClickHouse)
- Grafana (Grafana Helm chart)

### K8s manifests (custom)
- Flink: JobManager Deployment + TaskManager Deployment + Services
- Mock Generator: Deployment
- Flink Job submission: a Kubernetes Job that runs flink run for each jar (or use Flink REST API via JobManager service)
- ClickHouse init: Kubernetes Job runs DDL on startup
- Grafana provisioning: ConfigMaps for data source + dashboards + alert rules

⸻

## 11) Repository Layout (fixed)

```
repo/
  k8s/
    namespaces.yaml
    kafka-values.yaml
    clickhouse-values.yaml
    grafana-values.yaml
    flink/
      jobmanager.yaml
      taskmanager.yaml
      job-submit.yaml
    producer/
      deployment.yaml
      configmap.yaml
    clickhouse-init/
      ddl.sql
      init-job.yaml
    grafana/
      datasources.yaml
      dashboards/
        flight_ops.json
        itinerary.json
      alerts/
        alert-rules.yaml
  producer/
    src/...
    Dockerfile
  flink-jobs/
    bag-latest-job/
    flight-kpi-alert-job/
    itinerary-status-job/
    Dockerfile (optional for job submission tooling)
  docs/
    README.md
```

⸻

## 12) Acceptance Criteria (what “done” means)

1. baggage.events.v1 has continuous event flow and looks realistic.
2. ClickHouse tables populate:
   - flight_kpis updates every 10s for active flights
   - bag_latest shows latest stage per bag
   - alerts gets WARN/CRITICAL for injected failures
   - itinerary_status shows transfer states and AT_RISK/DELAYED cases
3. Grafana shows:
   - flights sorted by missing/loaded_pct
   - a selected flight funnel progression
   - itineraries flagged AT_RISK/DELAYED
4. Grafana alerts fire when CRITICAL alerts appear.
