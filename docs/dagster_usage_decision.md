# Adopt Dagster and Dask for ingress workflows

## Decision

Use **Dagster** to define and monitor ingestion workflows and **Dask** to plan and execute parallel computations within workflow stages. Store datasets, inspectable intermediates, and durable failure records in **Icechunk on S3**. We decided against using Sentry for workflow monitoring.

The primary requirement is a web UI where we can inspect the workflow graph, follow running and completed jobs, and open the exact inputs and outputs of a filter in our data viewer. Sentry’s exception grouping and alerting address service monitoring. Dataset-linked failure records need to remain accessible for the lifetime of the dataset.

## Workflow structure and data inspection

- Define workflows in Python using Dagster `@graph` and `@op`. Make each stage that users need to inspect a Dagster op.
- Keep related Dask operations in the same lazy graph and submit them together. This lets Dask optimize within a stage. Materializing an Icechunk checkpoint makes the intermediate inspectable and can introduce an I/O boundary.
- Pass versioned Icechunk references and ordinary Python configuration between stages. A dataset reference should identify the repository, snapshot, and Zarr path, with an optional item or slice selector.
- Use a shared filter wrapper to record resolved inputs and effective configuration. Publish **Open input**, **Open output**, and **Open failure** links in Dagster event metadata as they become available.
- Give every persisted failure an application-owned `failure_id`. If a rejected item is omitted from the dataset, retain an `item_key → failure_id` mapping. Where the output and its rejection records share an Icechunk repository, commit them together.
- Treat expected validation rejections as structured results. For an unexpected exception, record the failed input reference and traceback, then re-raise so Dagster marks the step failed.

Dagster shows workflow-level structure and state. Dask’s dashboard provides diagnostics for the underlying chunks, partitions, and workers.

## Preliminary Kubernetes scaling

Start with **one Kubernetes Job per Dagster run and one Dask cluster reused across that run’s stages**. The user selects an approved profile specifying worker count, CPU, and memory per worker. Dagster’s `K8sRunLauncher` launches the run, and the Dask Kubernetes Operator provisions the scheduler and worker Pods. The resources assigned to the Dagster run Pod are separate from the Dask worker resources.

Requesting a different worker count is straightforward. Predicting how much faster the run will finish requires measurements. Dagster provides historical stage timings, and Dask provides task and transfer diagnostics. Neither supplies a reliable pre-run ETA for a new input and worker profile.

Benchmark representative inputs at a few fixed worker counts, such as 2, 4, and 8. Record the stage and code version, bytes or chunks touched, worker profile, elapsed time, and time spent writing or committing to Icechunk. Track queueing and Pod startup separately. Use the results to show a runtime range for each profile. For branching workflows, account for the longest dependency path and for branches competing for the same workers or S3 bandwidth.

Adaptive scaling or adding workers during a run can be evaluated later. Extra workers help when the remaining computation has enough parallel work and Kubernetes can schedule the requested Pods.

## Why Dagster over Prefect

| Criterion | Dagster | Prefect |
|---|---|---|
| Workflow definition | Python ops composed into a predefined graph, or assets with declared dependencies | Python flows calling or submitting tasks, with branches and loops evaluated at runtime |
| Graph inspection | The job definition or asset lineage can be browsed before launch | The run graph shows the dependencies captured during that execution |
| Primary UI entry points | Runs and named datasets, with asset lineage and materialization history | Flow and task runs, with optional asset tracking |
| Links to our viewer | Clickable URLs in structured event or asset metadata | Links and summaries published as artifacts |

Both projects target overlapping data engineering and scientific workflow use cases and have active public release histories. Prefect is a good fit when the starting point is an existing Python service and users mainly browse individual runs. We chose Dagster because we want to inspect the defined graph before execution and expect some intermediate datasets to be worth browsing across runs.

## Acquisition, license, and maintenance risk

Prefect announced its acquisition of Dagster Labs on **13 July 2026** and committed to continuing Dagster under its existing name and open-source license. Both projects remain publicly developed. Long-term investment in two overlapping products and the direction of their respective roadmaps remain uncertain.

**Dagster OSS is licensed under Apache License 2.0.** Prefect’s open-source core uses the same license. The community can fork the Apache-licensed Dagster code, modify it, and redistribute it under the license terms. A distributed fork must preserve applicable notices, include the license, and mark modified files. The license does not grant general rights to the Dagster trademarks or cover proprietary Dagster+ components. A sustainable fork would still need maintainers for releases, security fixes, and integrations.

Keep the filters as ordinary Python functions, the Dask computation independent of the orchestrator, and snapshot references and failure records in Icechunk. This keeps the cost of switching orchestrators manageable.

## Other workflow runners considered

- **Apache Airflow** supports decorated Python tasks through TaskFlow, builds a DAG from task dependencies, and provides a web UI for live and historical runs. We could pass Icechunk snapshot references between tasks and run Dask inside them. Airflow’s task views focus on execution state, logs, and values passed between tasks. Giving reviewers a place to browse named intermediates across runs and open their exact snapshots would require more integration than using Dagster’s asset metadata and materialization history. [TaskFlow](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/taskflow.html) · [Airflow UI](https://airflow.apache.org/docs/apache-airflow/stable/ui.html)
- **Galaxy** provides a browser-based workflow editor, run inspection, and dataset histories. A typical integration exposes each Python filter as a command-line tool through an XML wrapper, with inputs and outputs declared in Galaxy’s dataset model. For our code-defined pipelines, this adds a tool packaging layer and requires adapting Icechunk snapshot references and viewer links to Galaxy’s history model. Dagster lets us expose the Python filters directly and attach viewer URLs to stage events. [Galaxy tool wrappers](https://docs.galaxyproject.org/en/master/dev/schema.html)
- **Argo Workflows** provides Kubernetes-native DAG execution and direct control over Pod resources. Its steps run as container invocations, and dependencies and data handoffs are expressed in workflow specs. We could share one image across filters, but would still need a command entry point and parameter or artifact wiring for each exposed stage. Dagster keeps stage composition in Python and lets Dask manage worker-level parallelism. [Argo workflow specs](https://argoproj.github.io/argo-workflows/walk-through/the-structure-of-workflow-specs/)
- **Dask alone** gives us lazy graph construction, optimization, and a live dashboard for tasks and workers. It can export diagnostics, but we would need to build the durable run browser and the links from stages to Icechunk snapshots and failure records. Dagster supplies the workflow-level UI while Dask handles computation and worker diagnostics. [Dask dashboard](https://docs.dask.org/en/stable/dashboard.html) · [Exporting diagnostics](https://distributed.dask.org/en/latest/web.html)
- **Parsl** supports Python functions decorated with `@python_app`, future-based dependencies, and a web dashboard backed by SQLite monitoring records. It is a plausible choice if HPC job execution becomes the main requirement. For ingestion review, we would still need to build the dataset-oriented views and integrate Icechunk viewer links. Dagster gives us structured event metadata and materialization history for those checkpoints. [Parsl Python apps](https://parsl.readthedocs.io/en/latest/userguide/apps/python.html) · [Parsl monitoring](https://parsl.readthedocs.io/en/stable/userguide/advanced/monitoring.html)

## References

- [Dagster op graphs](https://docs.dagster.io/guides/build/ops/graphs)
- [Dagster web UI](https://docs.dagster.io/guides/operate/webserver)
- [Dagster on Kubernetes](https://docs.dagster.io/deployment/oss/deployment-options/kubernetes/customizing-your-deployment)
- [Dask Kubernetes Operator](https://kubernetes.dask.org/en/latest/)
- [Prefect tasks](https://docs.prefect.io/v3/concepts/tasks)
- [Prefect artifacts](https://docs.prefect.io/v3/concepts/artifacts)
- [Dagster acquisition announcement](https://dagster.io/prefect)
- [Dagster license](https://github.com/dagster-io/dagster/blob/master/LICENSE)
