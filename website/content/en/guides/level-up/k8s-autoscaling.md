---
date: "2026-07-01"
title: Load balancing and scaling Vector on Kubernetes
short: K8s autoscaling
description: Observe a single Vector pod hit its CPU ceiling, eliminate it by manually scaling horizontally behind an L7 load balancer, then automate that scaling with the Kubernetes HPA finding its own equilibrium.
authors: ["thomasqueirozb"]
domain: platforms
weight: 7
tags: ["level up", "guides", "guide", "kubernetes", "load balancing", "nginx"]
---

This guide walks through observing a single Vector pod hit its CPU ceiling while
parsing [Apache Common Log format](https://httpd.apache.org/docs/current/logs.html#common), then eliminating that ceiling by manually
scaling horizontally behind [Nginx](https://www.nginx.com/). Then we're going to set up automatic
scaling using Kubernetes [Horizontal Pod Autoscaler](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/)
(HPA) and let it find its own equilibrium.

All steps are reproducible using the manifests and Helm values in this repository.

## Background

Vector's `parse_regex!` transform is CPU-bound: for every incoming log line it
executes a compiled Rust regex, allocates capture-group values, and writes a
structured event downstream.  A single Vector pod limited to 1 vCPU will
saturate that core under sustained parallel HTTP load due to the regex
parsing.

When saturation is reached, Vector applies **backpressure rather than dropping
events**. The HTTP source stops accepting new requests; Nginx stalls the load
generator's connections.

## Test environment

The benchmark was measured on a **[K3s](https://k3s.io/) single-node cluster on an [EC2](https://aws.amazon.com/ec2/) c5.4xlarge**
(16 vCPU, 32 GiB RAM). A single-node cluster was chosen so that latency and
network overhead are not a factor and collected metrics are precise.

- **Load generator:** [lading](https://github.com/DataDog/lading),
  generating `apache_common` log lines at a configurable byte rate. It
  maintains persistent parallel connections and is capable of sustained
  high-throughput HTTP load.
- **Load level:** **55 MiB/s** is used across all tests to get comparable
  throughput measurements.
- **Vector pod resources:** **1 vCPU / 2 GiB**, with `requests == limits`
  (Guaranteed QoS), so CPU throttling, not memory pressure or scheduling
  variance, is the only bottleneck under test.

## Architecture

```text
1 × lading pod  (100 parallel connections, 55 MiB/s)
        │  HTTP POST → ingress-nginx ClusterIP :80
        ▼
   Nginx ingress controller  (L7 round-robin per request)
        │
        ▼ (1, 3, or 8 pods depending on phase)
   Vector pod(s)  (1 vCPU / 2 GiB each)
   ┌──────────────────────────────────────┐
   │ source:    http_server :9000         │
   │ transform: parse_regex! (apache_clf) │
   │ sink:      socket TCP → consumer     │
   └──────────────────────────────────────┘
        │  TCP → consumer Service
        ▼
   consumer pod  (socat -u, drains to /dev/null)
```

## Why HTTP + L7 load balancing?

A plain TCP connection has no request boundary: once a client is connected to
a pod, a Kubernetes ClusterIP Service (which load-balances at L4) has no
opportunity to redistribute that traffic to a newly scaled-up pod. HTTP
defines a request boundary, so an L7 load balancer like Nginx can dispatch
each request independently, letting new pods pick up load as soon as they're
Ready.

A similar setup using [HAProxy](https://www.haproxy.org/) in TCP mode has the same problem: it
load-balances at the connection level, so a single producer's connection stays
pinned to one consumer for its lifetime, and can leave some consumers starved
of data entirely.

This is why we install an Nginx ingress in front of Vector instead of exposing
it through a plain ClusterIP Service.

## Prerequisites

- [`kubectl`](https://kubernetes.io/docs/reference/kubectl/) configured against a target cluster
- [`helm`](https://helm.sh/) ≥ 3.0
- At least 9 allocatable CPUs total (8 for Vector at max scale, 0.5 for the consumer, 0.2 for the producer)
- [`grpcurl`](https://github.com/fullstorydev/grpcurl) for metric collection
- [Kubernetes Metrics API](https://github.com/kubernetes-sigs/metrics-server) (`metrics-server`) installed — required for `kubectl top pods` and HPA CPU targets. K3s bundles it by default; on other clusters run `kubectl top nodes` to verify it is available before starting.

## How the metrics are collected

Each Vector pod exposes [`ObservabilityService`](https://github.com/vectordotdev/vector/blob/master/proto/vector/observability.proto) on port 8686 ([gRPC](https://grpc.io/)). The
measurement approach used for every phase below is: port-forward to a pod,
take two `GetComponents` samples 30 s apart, and diff `receivedBytesTotal` on
the `in` source component to get a per-pod throughput rate. Per-pod CPU is
read via `kubectl top pods` and averaged across all Vector pods.

For example, against a single pod:

```bash
kubectl port-forward -n vector-perf pod/<pod-name> 18686:8686 &

grpcurl -plaintext -d '{}' localhost:18686 \
  vector.observability.v1.ObservabilityService/GetComponents > t0.json
sleep 30
grpcurl -plaintext -d '{}' localhost:18686 \
  vector.observability.v1.ObservabilityService/GetComponents > t30.json
```

Diffing `receivedBytesTotal` for the `in` component between `t0.json` and
`t30.json`, then dividing by 30 s, gives that pod's throughput.

See [Replicating these results](#replicating-these-results) below for a link to the script that
automates this.

## Setup

Create the namespace and the consumer that drains everything Vector forwards to it:

{{< embed file="content/en/guides/level-up/k8s-autoscaling/manifests/namespace.yaml" dir="true" >}}

{{< embed file="content/en/guides/level-up/k8s-autoscaling/manifests/consumer.yaml" dir="true" >}}

```bash
kubectl apply -f manifests/namespace.yaml
kubectl apply -f manifests/consumer.yaml

helm repo add ingress-nginx https://kubernetes.github.io/ingress-nginx
helm upgrade --install ingress-nginx ingress-nginx/ingress-nginx \
  -n ingress-nginx --create-namespace \
  --version 4.15.1 \
  --set controller.service.type=ClusterIP \
  --set controller.replicaCount=1 \
  --wait --timeout=3m

helm repo add vectordotdev https://helm.vector.dev
helm repo update
```

## Phase 1 — Single pod

Vector is installed with the shared base Helm values, which configure the
`http_server` source, the `parse_regex!` transform, and the `socket` sink to
the consumer:

{{< embed file="content/en/guides/level-up/k8s-autoscaling/values.yaml" dir="true" >}}

```bash
helm upgrade --install vector vectordotdev/vector --namespace vector-perf --version 0.56.0 -f values.yaml --set replicas=1

kubectl apply -f manifests/ingress.yaml
kubectl apply -f manifests/producer.yaml
```

The ingress routes HTTP POSTs to the Vector service at the request level (L7),
which is what lets the HPA find equilibrium in Phase 4:

{{< embed file="content/en/guides/level-up/k8s-autoscaling/manifests/ingress.yaml" dir="true" >}}

The producer is [lading](https://github.com/DataDog/lading), configured to
generate `apache_common` log lines at 55 MiB/s across 100 parallel connections:

{{< embed file="content/en/guides/level-up/k8s-autoscaling/manifests/producer.yaml" dir="true" >}}

55 MiB/s is expected to overwhelm a single pod's regex-parsing capacity, so
Vector should back-pressure lading down to whatever it can actually process.

<!-- RESULTS-SINGLE-START -->

| Metric | Value |
| ------ | ----- |
| Throughput | **16.64 MiB/s** |
| Events/s | **130,863 ev/s** |
| Pod CPU | **1000m (100%)** |
| Bottleneck | **Vector CPU** |

<!-- RESULTS-SINGLE-END -->

The pod is pinned at its 1000m CPU limit and throughput tops out at
16.64 MiB/s, confirming the expected CPU ceiling. That per-pod figure is the
baseline the next two phases are measured against.

## Phase 2 — Three pods

```bash
kubectl scale deployment vector -n vector-perf --replicas=3
kubectl rollout status deployment/vector -n vector-perf
```

55 MiB/s > 3 × 16.64 MiB/s = 49.92 MiB/s combined capacity.  All three pods are
still fully saturated. Adding pods increases throughput, but the ceiling
hasn't been reached yet.

<!-- RESULTS-LB-START -->

| Metric | Value |
| ------ | ----- |
| Throughput | **50.47 MiB/s** |
| Events/s | **396,846 ev/s** |
| Pod CPU | **~1000m (100%)** |
| Scaling vs Phase 1 | **3.03×** |
| Bottleneck | **Vector CPU** |

<!-- RESULTS-LB-END -->

## Phase 3 — Eight pods

```bash
kubectl scale deployment vector -n vector-perf --replicas=8
kubectl rollout status deployment/vector -n vector-perf
```

8 × 16.64 MiB/s = 133.1 MiB/s combined capacity >> 55 MiB/s.  Vector is no longer
the bottleneck; all 55 MiB/s flows through and pods have ample headroom.

<!-- RESULTS-8W-START -->

| Metric | Value |
| ------ | ----- |
| Throughput | **56.80 MiB/s** |
| Events/s | **446,650 ev/s** |
| Pod CPU | **~470m (47%)** |
| Bottleneck | **None, spare capacity** |

The bottleneck has been eliminated.  Each pod handles ~7.1 MiB/s at ~47% CPU,
leaving over half of each pod's capacity unused.  With L7 per-request routing,
load is distributed evenly across all 8 pods.

<!-- RESULTS-8W-END -->

## Comparison

<!-- RESULTS-COMPARE-START -->

All phases: **55 MiB/s lading** (100 parallel connections, Nginx L7 ingress),
pods limited to **1 vCPU / 2 GiB**.

| | Phase 1 (1 pod) | Phase 2 (3 pods) | Phase 3 (8 pods) |
| - | ----------------- | ------------------ | ------------------ |
| Throughput | 16.64 MiB/s | 50.47 MiB/s | **56.80 MiB/s** |
| Events/s | 130,863 | 396,846 | 446,650 |
| CPU per pod | 1000m (100%) | ~1000m (100%) | ~470m (47%) |
| Bottleneck | Vector CPU | Vector CPU | **None** |
| Scaling vs Phase 1 | 1× | 3.03× | **3.41×** |

<!-- RESULTS-COMPARE-END -->

We can see that 8 pods is too many, but 3 is too few. At 8 pods we're not
properly utilizing each pod's capacity, at only 47% average CPU utilization.

## Phase 4 — HPA finds equilibrium

Based on the results of Phase 1, we can estimate how many pods we would need
to spin up to stay under CPU saturation while keeping some headroom. The
saturation crossover is 55 / 16.64 ≈ **3.3 pods** at 100% CPU. At a 70%
utilization target, the expected equilibrium is ⌈3.3 / 0.70⌉ = ⌈4.71⌉ = **5 pods**.

We can now configure the HPA to find the minimum pod count that keeps CPU
utilization around the 70% target.

```bash
# Reset to 1 pod
kubectl scale deployment vector -n vector-perf --replicas=1

# Create HPA (70% CPU target, 1–8 replicas)
kubectl autoscale deployment vector -n vector-perf \
  --cpu-percent=70 --min=1 --max=8
```

### Phase 4 results

<!-- RESULTS-HPA-START -->

**Scale-up timeline (no manual intervention):**

| Time | Replicas | Avg CPU | Event |
| ---- | -------- | ------- | ----- |
| t=0 s | **1** | 100% | load starts |
| t=30 s | **2** | 100% | HPA scales 1→2 |
| t=90 s | **3** | 98% | HPA scales 2→3 |
| t=136 s | **5** | 100% | HPA scales 3→5 |
| t=196 s | **5** | **71%** | **Stable, equilibrium** |

Time to equilibrium: **196 s (~3 min)**, 3 scale events, 0 manual cycling.

**Throughput at equilibrium: 56.56 MiB/s, 444,744 ev/s, 5 pods, 71% avg CPU.**

The HPA settled at 5 pods: CPU converged from 97% immediately after the 3→5
scale event down to 71%, within the ±10% tolerance band (63–77%), and held
stable for three consecutive 15 s intervals.

<!-- RESULTS-HPA-END -->

## Results summary

| | Phase 1 (1 pod) | Phase 2 (3 pods) | Phase 3 (8 pods) | Phase 4 (HPA) |
| - | ----------------- | ------------------ | ------------------ | ------------------ |
| Throughput | 16.64 MiB/s | 50.47 MiB/s | 56.80 MiB/s | **56.56 MiB/s** |
| Events/s | 130,863 | 396,846 | 446,650 | **444,744** |
| CPU per pod | 1000m (100%) | ~1000m (100%) | ~470m (47%) | **~710m (71%)** |
| Bottleneck | Vector CPU | Vector CPU | None | None |
| Scaling vs Phase 1 | 1× | 3.03× | 3.41× | **3.40×** |
| Pod count | manual (1) | manual (3) | manual (8) | **auto (5)** |

Phase 4 reaches Phase 3's throughput with 3 fewer pods and no manual scaling:
the HPA found exactly 5 pods, matching the theoretical prediction of
⌈(55 / 16.64) / 0.70⌉ = 5, and kept CPU near its 70% target instead of
leaving ~53% headroom idle on every pod.

## Key takeaways

1. **A single pod caps at its CPU limit.**  At 55 MiB/s load, 1 pod can absorb
   only ~16.6 MiB/s.  Back-pressure prevents any event loss.

2. **L7 per-request routing distributes load uniformly.**  Because Nginx
   dispatches each HTTP request independently, every pod, old or newly
   Ready, receives a share of traffic proportional to the current replica
   count, with no idle pods.

3. **Adding pods beyond the saturation point removes the bottleneck entirely.**
   Phase 3 (8 pods) delivers the full 55 MiB/s with each pod at ~47% CPU.
   The saturation crossover is at ~3.3 pods; at the 70% HPA target that
   predicts ⌈3.3 / 0.70⌉ = 5 pods, exactly what Phase 4 observed.

4. **HPA finds the right pod count automatically.**  With HTTP + L7 routing,
   every new pod starts receiving traffic immediately after becoming Ready.
   HPA converged at 5 pods in 196 s with zero manual intervention.

---

## Replicating these results

The [`terraform/`](https://github.com/vectordotdev/vector/tree/master/website/content/en/guides/level-up/k8s-autoscaling/terraform)
directory provisions the K3s single-node cluster (EC2 `c5.4xlarge`) the
benchmark above was measured on, if you don't already have a cluster to test
against.

Once the [Setup](#setup) steps are complete and Phase 1's producer and ingress
are deployed, `run-experiment.sh` runs all four phases end to end: scaling the
deployment, waiting for each rollout, measuring throughput, and creating the
HPA for Phase 4, then prints a single results table.

{{< embed file="content/en/guides/level-up/k8s-autoscaling/scripts/run-experiment.sh" open="false" >}}

```bash
KUBECONFIG=/path/to/kubeconfig ./scripts/run-experiment.sh
```
