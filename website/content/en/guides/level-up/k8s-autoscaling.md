---
date: "2026-07-01"
title: Load balancing and scaling Vector on Kubernetes
short: K8s autoscaling
description: Observe a single Vector pod reaching its CPU ceiling, eliminate the ceiling by manually scaling horizontally behind an L7 load balancer, and then automate that scaling with the Kubernetes HPA to reach a stable replica count that maintains target average CPU utilization.
authors: ["thomasqueirozb"]
domain: platforms
weight: 7
tags: ["level up", "guides", "guide", "kubernetes", "load balancing", "nginx"]
---

In this guide, we'll show how a single Vector pod reaches its CPU ceiling while
parsing [Apache Common Log Format](https://httpd.apache.org/docs/current/logs.html#common) data. We'll then eliminate that ceiling by manually
scaling Vector horizontally behind the [NGINX](https://www.nginx.com/) Ingress Controller, an L7 load balancer. Finally, we'll set up automatic
scaling by using the Kubernetes [Horizontal Pod Autoscaler (HPA)](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/)
to reach a stable replica count that maintains a target average CPU utilization of 70%.

All steps in this guide are reproducible. See [Replicating these results](#replicating-these-results)
for the manifests and Helm values used.

## Background

Vector's `parse_regex!` transform is CPU-bound: For every incoming log line, the transform
executes a compiled Rust regex, allocates capture-group values, and writes a
structured event downstream. Under sustained parallel HTTP load, a single Vector pod limited to 1 vCPU will
saturate that core due to the regex
parsing.

When CPU saturation occurs, Vector applies **backpressure instead of dropping
events**. Vector's `http_server` source keeps accepting connections but stalls
on responses until it can process the backlog, so the NGINX Ingress
Controller and the load generator experience stalled connections.

## Test environment

To evaluate Vector's scaling behavior under a sustained CPU-bound workload, we used a **[K3s](https://k3s.io/) single-node cluster hosted on an [Amazon EC2](https://aws.amazon.com/ec2/) c5.4xlarge** instance
(16 vCPU, 32 GiB RAM). We chose a single-node cluster to eliminate latency and
network overhead as factors, making the collected metrics more precise.
We used the following configuration for the tests:
- **Load generator:** [lading](https://github.com/DataDog/lading),
  generating `apache_common` log lines at a configurable byte rate. It
  maintains persistent parallel connections and is capable of generating sustained
  high-throughput HTTP load.
- **Load level:** **55 MiB/s** across all tests to get comparable
  throughput measurements.
- **Vector pod resources:** **1 vCPU and 2 GiB of memory**, with `requests == limits`
  (Guaranteed QoS) to ensure that CPU throttling, not memory pressure or scheduling
  variance, was the only bottleneck tested.

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

### Why HTTP with L7 load balancing?

A plain TCP connection has no request boundary: Once a client is connected to
a pod, a Kubernetes ClusterIP Service (which load-balances at L4) cannot
redistribute that traffic to a newly created pod. By contrast, HTTP
defines a request boundary, so an L7 load balancer such as the NGINX Ingress Controller can route
each request independently. As new pods become Ready, they can pick up load immediately.

A similar setup using [HAProxy](https://www.haproxy.org/) in TCP mode has the same limitation as a Kubernetes ClusterIP Service: It
load-balances at the connection level, so a single producer's connection stays
pinned to one consumer for its lifetime and can leave some consumers starved
of data entirely.

This is why we installed an NGINX Ingress Controller in front of Vector instead of exposing
Vector through a ClusterIP Service.

## Prerequisites

- [`kubectl`](https://kubernetes.io/docs/reference/kubectl/) configured against a target cluster
- [`helm`](https://helm.sh/) version 3.0 or later
- At least 9 allocatable CPUs total (8 for Vector at max scale, 0.5 for the consumer, 0.2 for the producer)
- [`grpcurl`](https://github.com/fullstorydev/grpcurl) for metric collection
- [Kubernetes Metrics API](https://github.com/kubernetes-sigs/metrics-server) (`metrics-server`) installed (This is required for `kubectl top pods` and HPA CPU targets. K3s bundles `metrics-server` by default. On other clusters, run `kubectl top nodes` to verify that `metrics-server` is available before you start.)

## Collecting throughput and CPU metrics

Each Vector pod exposes [`ObservabilityService`](https://github.com/vectordotdev/vector/blob/master/proto/vector/observability.proto) on port 8686 ([gRPC](https://grpc.io/)). For
each phase of our testing, we measured throughput by port-forwarding to a pod,
capturing two `GetComponents` samples 30 seconds apart, and calculating the difference in `receivedBytesTotal` for
the `in` source component to determine a per-pod throughput rate. Per-pod CPU was
read via `kubectl top pods` and averaged across all Vector pods.

The following commands collect the data used to calculate throughput for a single pod:

```bash
kubectl port-forward -n vector-perf pod/<pod-name> 18686:8686 &

grpcurl -plaintext -d '{}' localhost:18686 \
  vector.observability.v1.ObservabilityService/GetComponents > t0.json
sleep 30
grpcurl -plaintext -d '{}' localhost:18686 \
  vector.observability.v1.ObservabilityService/GetComponents > t30.json
```

The difference in `receivedBytesTotal` for the `in` component between `t0.json` and
`t30.json`, divided by 30 seconds, gives that pod's throughput.

See [Replicating these results](#replicating-these-results) for a link to the script that
automates this process.

## Setup

The following manifests create the namespace and deploy the consumer that drains all data forwarded by Vector:

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

## Phase 1: Single pod

The following Helm values configure Vector with an
`http_server` source, the `parse_regex!` transform, and the `socket` sink that forwards data to
the consumer:

{{< embed file="content/en/guides/level-up/k8s-autoscaling/values.yaml" dir="true" >}}

```bash
helm upgrade --install vector vectordotdev/vector --namespace vector-perf --version 0.56.0 -f values.yaml --set replicas=1

kubectl apply -f manifests/ingress.yaml
kubectl apply -f manifests/producer.yaml
```

The following Ingress routes HTTP POST requests to the Vector Service at the request level (L7),
so every pod receives a share of traffic as soon as it's Ready, independent of how or why the replica count changes:

{{< embed file="content/en/guides/level-up/k8s-autoscaling/manifests/ingress.yaml" dir="true" >}}

The producer is [lading](https://github.com/DataDog/lading), configured to
generate `apache_common` log lines at 55 MiB/s across 100 parallel connections:

{{< embed file="content/en/guides/level-up/k8s-autoscaling/manifests/producer.yaml" dir="true" >}}

At 55 MiB/s, the workload is expected to overwhelm a single pod's regex-parsing capacity.
When the pod reaches CPU saturation, Vector applies backpressure, reducing the rate at which lading can send data.

The resulting throughput and CPU utilization are shown in the following table:

<!-- RESULTS-SINGLE-START -->

| Metric | Value |
| ------ | ----- |
| Throughput | **16.64 MiB/s** |
| Events/s | **130,863 ev/s** |
| Pod CPU | **1000m (100%)** |
| Bottleneck | **Vector CPU** |

<!-- RESULTS-SINGLE-END -->

The pod is pinned at its 1000m CPU limit, and throughput tops out at
16.64 MiB/s, confirming the expected CPU ceiling. This per-pod throughput is the
baseline that the next two phases are measured against.

## Phase 2: 3 pods

The following commands scale the deployment to three replicas:
```bash
kubectl scale deployment vector -n vector-perf --replicas=3
kubectl rollout status deployment/vector -n vector-perf
```

At 55 MiB/s, the workload still exceeds the combined throughput ceiling of three
pods (3 × 16.64 MiB/s = 49.92 MiB/s). All three pods remain still fully saturated.

<!-- RESULTS-LB-START -->

| Metric | Value |
| ------ | ----- |
| Throughput | **50.47 MiB/s** |
| Events/s | **396,846 ev/s** |
| Pod CPU | **~1000m (100%)** |
| Scaling vs. Phase 1 | **3.03×** |
| Bottleneck | **Vector CPU** |

<!-- RESULTS-LB-END -->

## Phase 3: 8 pods
The following commands scale the deployment to eight replicas:
```bash
kubectl scale deployment vector -n vector-perf --replicas=8
kubectl rollout status deployment/vector -n vector-perf
```

Eight pods provide a combined throughput ceiling of approximately 133.1 MiB/s (8 × 16.64 MiB/s = 133.1 MiB/s), well above the workload's 55 MiB/s. The bottleneck is
eliminated. All 55 MiB/s flows through, and the pods have ample CPU headroom.

<!-- RESULTS-8W-START -->

| Metric | Value |
| ------ | ----- |
| Throughput | **56.80 MiB/s** |
| Events/s | **446,650 ev/s** |
| Pod CPU | **~470m (47%)** |
| Bottleneck | **None, spare capacity** |

Each pod handles approximately 7.1 MiB/s at about 47% CPU utilization,
leaving over half of each pod's capacity unused. With L7 per-request routing,
load is distributed evenly across all eight pods.

<!-- RESULTS-8W-END -->

## Comparison: Phases 1–3

<!-- RESULTS-COMPARE-START -->

All phases use a **55 MiB/s lading workload** (100 parallel connections through the L7 NGINX Ingress Controller),
with Vector pods limited to **1 vCPU and 2 GiB of memory**.

| | Phase 1 (1 pod) | Phase 2 (3 pods) | Phase 3 (8 pods) |
| - | ----------------- | ------------------ | ------------------ |
| Throughput | 16.64 MiB/s | 50.47 MiB/s | **56.80 MiB/s** |
| Events/s | 130,863 | 396,846 | 446,650 |
| CPU per pod | 1000m (100%) | ~1000m (100%) | ~470m (47%) |
| Bottleneck | Vector CPU | Vector CPU | **None** |
| Scaling vs. Phase 1 | 1× | 3.03× | **3.41×** |

<!-- RESULTS-COMPARE-END -->

We can see that eight pods is too many, but three pods is too few. At eight pods, we're not
properly utilizing each pod's capacity (only 47% average CPU utilization).

## Phase 4: HPA finds equilibrium

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

The following timeline shows how the HPA scales the deployment from one replica to five replicas
<!-- RESULTS-HPA-START -->

**Scale-up timeline (no manual intervention):**

| Time | Replicas | Avg CPU | Event |
| ---- | -------- | ------- | ----- |
| t=0 s | **1** | 100% | load starts |
| t=30 s | **2** | 100% | HPA scales 1→2 |
| t=90 s | **3** | 98% | HPA scales 2→3 |
| t=136 s | **5** | 100% | HPA scales 3→5 |
| t=196 s | **5** | **71%** | **Stable, equilibrium** |

Time to equilibrium: **196 seconds (approximately 3 minutes)**, 3 scale events, no manual scaling.

**Throughput at equilibrium: 56.56 MiB/s, 444,744 ev/s, 5 pods, 71% average CPU.**

The HPA settles at five pods: CPU converges from 97% immediately after the 3→5
scale-up event to 71%, within the ±10% tolerance band (63–77%) set by the
[`--horizontal-pod-autoscaler-tolerance`](https://kubernetes.io/docs/reference/command-line-tools-reference/kube-controller-manager/)
flag's `0.1` default, and holds stable for three consecutive 15-second intervals.

<!-- RESULTS-HPA-END -->

## Results summary

| | Phase 1 (1 pod) | Phase 2 (3 pods) | Phase 3 (8 pods) | Phase 4 (HPA) |
| - | ----------------- | ------------------ | ------------------ | ------------------ |
| Throughput | 16.64 MiB/s | 50.47 MiB/s | 56.80 MiB/s | **56.56 MiB/s** |
| Events/s | 130,863 | 396,846 | 446,650 | **444,744** |
| CPU per pod | 1000m (100%) | ~1000m (100%) | ~470m (47%) | **~710m (71%)** |
| Bottleneck | Vector CPU | Vector CPU | None | None |
| Scaling vs. Phase 1 | 1× | 3.03× | 3.41× | **3.40×** |
| Pod count | manual (1) | manual (3) | manual (8) | **auto (5)** |

Phase 4 reaches Phase 3's throughput with three fewer pods and no manual scaling.
The HPA scales to five pods, matching the prediction
and keeping CPU near its 70% target instead of
leaving each pod with roughly 53% of unused CPU capacity.

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

4. **The HPA determines the right pod count automatically.**  With HTTP and L7 routing,
   each new pod starts receiving traffic immediately after becoming Ready.


---

## Replicating these results

The manifests, Helm values, and scripts used throughout this guide live in
[`k8s-autoscaling/`](https://github.com/vectordotdev/vector/tree/master/website/content/en/guides/level-up/k8s-autoscaling).

The [`terraform/`](https://github.com/vectordotdev/vector/tree/master/website/content/en/guides/level-up/k8s-autoscaling/terraform)
directory provisions the K3s single-node cluster (EC2 `c5.4xlarge`) that
we used, if you don't already have a cluster to test
against.

Once the [Setup](#setup) steps are complete and Phase 1's producer and ingress
are deployed, `run-experiment.sh` runs all four phases end to end: scaling the
deployment, waiting for each rollout, measuring throughput, and creating the
HPA for Phase 4. It then prints a single results table.

{{< embed file="content/en/guides/level-up/k8s-autoscaling/scripts/run-experiment.sh" open="false" >}}

```bash
KUBECONFIG=/path/to/kubeconfig ./scripts/run-experiment.sh
```
