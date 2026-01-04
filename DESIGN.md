# Local POC: Baggage Tracking Ops (Kafka + Flink + ClickHouse + Grafana) on Kubernetes

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

---

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

---

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

---

## 4) Canonical Event Schemas (JSON)

### 4.1 baggage.events.v1 (required)
