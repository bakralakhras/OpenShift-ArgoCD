# GitOps Delivery

## Overview

The platform uses ArgoCD through OpenShift GitOps.

The intended model is:

```text
Git repository -> ArgoCD root app -> child applications -> OpenShift resources
```

---

## Diagram

![GitOps Flow](diagrams/gitops-flow.svg)

---

## App of Apps

The intended bootstrap file is:

```bash
oc apply -f apps/root.yaml
```

The root application points to the `apps/` directory.

Current validation found 27 application manifests in `apps/`.

---

## Current Application State

The platform is under active development, so not every ArgoCD application is perfectly Synced and Healthy at all times.

Validated non-clean states included:

```text
airflow             OutOfSync / Missing
keycloak            OutOfSync / Healthy
rook-ceph-cluster   Synced / Degraded
root                OutOfSync / Healthy
schema-registry     Synced / Progressing
trino               OutOfSync / Healthy
```

This does not invalidate the data platform, but it must be documented honestly.

---

## Why Drift Exists

In this project, drift can happen for normal engineering reasons:

- one-shot Spark jobs completing and being removed from active reconciliation
- Helm-generated resources changing live state
- components being rebuilt during documentation
- routes, images, or runtime-generated objects differing from Git
- storage components still settling
- lab cluster state changing during active testing

---

## Design Principle

A new component should be added with:

```text
apps/<component>.yaml
platform/<component>/
```

This keeps the deployment model simple and easy to explain.

---

## Useful Commands

```bash
oc get applications -n openshift-gitops
```

```bash
oc annotate application <app-name> \
  -n openshift-gitops \
  argocd.argoproj.io/refresh=hard \
  --overwrite
```

```bash
oc get application <app-name> -n openshift-gitops -o yaml
```

---

## Lesson

GitOps is excellent for platform state, but not every data job should be managed like a long-running service.

One-shot jobs need careful lifecycle handling. Airflow is a better place for recurring data workflows.
