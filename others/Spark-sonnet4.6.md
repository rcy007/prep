# Apache Spark -- A Deep-Dive Guide for Data Engineers

> **Audience**: Senior Data Engineers coming from Amazon Redshift.
> **Language**: All examples are in **PySpark** (Python).
> **Spark Version Focus**: 3.x / 4.x (concepts apply broadly; version-specific features are noted).

---

## Table of Contents

- [Level 0: Spark vs Redshift -- A Mental Model Shift](#level-0-spark-vs-redshift----a-mental-model-shift)
- [Level 1: Architecture Internals](#level-1-architecture-internals)
- [Level 2: The Three APIs](#level-2-the-three-apis)
- [Level 3: The Catalyst Optimizer -- Deep Internals](#level-3-the-catalyst-optimizer----deep-internals)
- [Level 4: Storage & File Formats](#level-4-storage--file-formats)
- [Level 5: Shuffle -- The Most Expensive Operation](#level-5-shuffle----the-most-expensive-operation)
- [Level 6: Joins -- Strategies and Optimization](#level-6-joins----strategies-and-optimization)
- [Level 7: Memory Management & Caching](#level-7-memory-management--caching)
- [Level 8: Adaptive Query Execution (AQE)](#level-8-adaptive-query-execution-aqe)
- [Level 9: Performance Tuning Playbook](#level-9-performance-tuning-playbook)
- [Level 10: Cluster Management & Deployment](#level-10-cluster-management--deployment)
- [Level 11: Structured Streaming](#level-11-structured-streaming)

---

## Level 0: Spark vs Redshift -- A Mental Model Shift

If you are coming from Redshift, the single biggest thing to internalize is this: **Spark is not a database. It is a distributed compute engine.** It has no storage of its own, no persistent tables by default, no always-on cluster, and no built-in concurrency control. You bring the data (S3, HDFS, Delta Lake, etc.), you bring the cluster (EMR, Databricks, Kubernetes, etc.), and Spark provides the engine that reads, transforms, and writes that data in a massively parallel fashion.

### Key Architectural Differences

| Dimension | Amazon Redshift | Apache Spark |
|---|---|---|
| **What it is** | Managed columnar MPP data warehouse | Open-source distributed compute engine |
| **Storage** | Tightly coupled (managed disks, RA3 + S3) | Decoupled -- reads/writes to external storage (S3, HDFS, ADLS, GCS) |
| **Query language** | SQL only | SQL, Python, Scala, Java, R |
| **Always-on?** | Yes (cluster is provisioned and running) | No -- clusters are typically ephemeral and spun up per job |
| **Optimizer** | Cost-based query optimizer | Catalyst (rule-based + optional cost-based) + Adaptive Query Execution |
| **Data format** | Proprietary columnar blocks | Open formats: Parquet, ORC, Delta Lake, Iceberg, etc. |
| **Concurrency model** | WLM queues, automatic concurrency scaling | One SparkSession per application; parallelism within the job |
| **Distribution keys** | `DISTKEY`, `DISTSTYLE` | Hash/range partitioning, bucketing |
| **Sort keys** | `SORTKEY` (compound/interleaved) | Z-Ordering (Delta Lake), bucketing with sort |
| **Scaling** | Resize cluster or enable concurrency scaling | Add/remove executors dynamically |

### Mindset Shifts

1. **SQL-first --> DAG-first**: In Redshift, everything is a SQL statement submitted to a query planner. In Spark, your code (Python, SQL, or otherwise) builds a **Directed Acyclic Graph (DAG)** of transformations. Nothing executes until you call an *action*. Understanding the DAG is how you debug and optimize Spark.

2. **Provisioned --> Elastic**: Redshift clusters are long-running. Spark clusters are often **ephemeral** -- spun up for a pipeline, torn down when it finishes. You pay for what you use, but you also have to think about cluster startup time and resource sizing.

3. **Storage = Compute --> Storage ≠ Compute**: In Redshift, data lives on the compute nodes (or in RA3 managed storage). In Spark, storage is *external*. This decoupling gives you flexibility (same data, many engines) but means you must think carefully about **data locality**, **file formats**, and **I/O costs**.

4. **Implicit optimization --> Explicit control**: Redshift's optimizer handles distribution, sorting, and compression somewhat transparently. In Spark, you have far more knobs: partition counts, join hints, broadcast thresholds, caching strategies, file layout, etc. More power, but more responsibility.

> **Citation**: [Apache Spark vs. Amazon Redshift: Which is better for big data? -- Integrate.io](https://www.integrate.io/blog/apache-spark-vs-amazon-redshift-which-is-better-for-big-data/)

---

## Level 1: Architecture Internals

### 1.1 The Master-Worker Topology

Every Spark application consists of three components:

```
┌──────────────────────────────────────────────────────┐
│                   CLUSTER MANAGER                    │
│              (YARN / K8s / Standalone)               │
└──────────┬───────────────────────────┬───────────────┘
           │  allocate resources       │
     ┌─────▼──────┐            ┌──────▼──────┐
     │   DRIVER   │            │  EXECUTOR 1 │
     │            │◄──────────►│  (JVM)      │
     │ - SparkCtx │   tasks/   │  - Task 1   │
     │ - DAG Sched│   results  │  - Task 2   │
     │ - Task Sche│            │  - Cache     │
     └────────────┘            └─────────────┘
           │                          ...
           │                   ┌─────────────┐
           └──────────────────►│  EXECUTOR N │
                               │  (JVM)      │
                               │  - Task 1   │
                               │  - Task 2   │
                               │  - Cache     │
                               └─────────────┘
```

**Driver Program**: The "brain." It runs your `main()` function, holds the `SparkSession` / `SparkContext`, and orchestrates everything. The Driver:
- Translates your high-level code (DataFrame operations, SQL) into a logical plan.
- Passes the logical plan through the Catalyst Optimizer.
- Converts the optimized logical plan into a **physical plan** -- a DAG of stages and tasks.
- Hands tasks to the **Task Scheduler**, which assigns them to executors.
- Collects results (for actions like `collect()`) and tracks task status.

**Executors**: The "muscles." Each executor is a JVM process on a worker node. It:
- Receives tasks from the Driver and runs them.
- Stores data in memory or disk (for caching / shuffle).
- Reports results and status back to the Driver.
- Each executor has a fixed number of **cores** (slots for parallel tasks) and a fixed amount of **memory**.

**Cluster Manager**: The "HR department." It allocates resources (containers / pods) for the Driver and Executors. Spark supports several:
- **YARN** (Hadoop)
- **Kubernetes**
- **Standalone** (Spark's own built-in manager)
- **Mesos** (deprecated in Spark 3.2+)

> **Citation**: [Cluster Mode Overview -- Spark 4.1.1 Documentation](https://spark.apache.org/docs/latest/cluster-overview.html)
>
> **Citation**: [Apache Spark Architecture Deep Dive -- Medium (Kuldeepsinh Vaghela)](https://medium.com/@KuldeepsinhVaghela/apache-spark-architecture-deep-dive-how-the-driver-executors-and-cluster-manager-work-together-9569ea48e9e5)

### 1.2 SparkSession & SparkContext

**`SparkContext`** was the original entry point (Spark 1.x). It represents the connection to the cluster and is used to create RDDs and broadcast variables. Only **one** SparkContext can be active per JVM.

**`SparkSession`** (introduced in Spark 2.0) is the **modern, unified entry point**. It wraps SparkContext and adds the DataFrame/SQL API. In PySpark, you almost always work with `SparkSession`:

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("MyPipeline")
    .config("spark.sql.shuffle.partitions", "200")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()
)

# Access the underlying SparkContext if needed
sc = spark.sparkContext
```

Configuration can be set in three places (in order of precedence):
1. **Programmatically** via `SparkSession.builder.config(...)` or `SparkConf`
2. **`spark-submit` flags**: `--conf spark.executor.memory=8g`
3. **`spark-defaults.conf`** file on the cluster

> **Important**: Once a `SparkContext` is created, most configuration properties are **immutable**. Set them *before* calling `.getOrCreate()`.

> **Citation**: [Configuration -- Spark 4.1.1 Documentation](https://spark.apache.org/docs/latest/configuration.html)

### 1.3 Jobs, Stages, and Tasks

When you call an **action** (e.g., `df.count()`, `df.write.parquet(...)`), Spark submits a **Job**. Here is exactly what happens:

1. **Job**: One action = one job. The DAG Scheduler receives the job.

2. **Stages**: The DAG Scheduler breaks the job's DAG into **stages** at **shuffle boundaries** (wide dependencies). Within a stage, all transformations can be *pipelined* -- executed in a single pass without data exchange between nodes.

3. **Tasks**: Each stage is divided into **tasks** -- one task per partition. If a stage operates on 200 partitions, Spark creates 200 tasks. Each task runs the stage's pipeline on a single partition of data.

```
Action called: df.groupBy("region").count().show()

Job 0
├── Stage 0 (Read + Filter)      ← narrow transformations, pipelined
│   ├── Task 0 (partition 0)
│   ├── Task 1 (partition 1)
│   └── ... Task N
│
├── ── SHUFFLE BOUNDARY ── (data exchanged for groupBy)
│
└── Stage 1 (Aggregate + Show)   ← post-shuffle stage
    ├── Task 0 (partition 0)
    ├── Task 1 (partition 1)
    └── ... Task M
```

### 1.4 DAG Scheduler & Task Scheduler

These are two distinct components that live on the **Driver**:

**DAG Scheduler** (stage-oriented):
- Receives a job (triggered by an action).
- Builds a DAG of **RDD lineage** (or DataFrame logical plan nodes).
- Identifies **shuffle dependencies** (wide dependencies) and splits the DAG into stages.
- Computes stages in **topological order** -- a downstream stage cannot start until all upstream stages complete.
- Handles **failure recovery**: if a shuffle output is lost (executor dies), it resubmits the failed stage.
- Determines **preferred task locations** based on data locality (e.g., "run this task on the node where its HDFS block lives").

**Task Scheduler** (task-oriented):
- Receives a set of tasks (a `TaskSet`) from the DAG Scheduler for a given stage.
- Assigns tasks to available executor slots, respecting **data locality preferences** (PROCESS_LOCAL > NODE_LOCAL > RACK_LOCAL > ANY).
- Monitors task execution: retries failed tasks (default: 4 retries), handles speculative execution.
- Reports results back to the DAG Scheduler.

> **Citation**: [DAGScheduler -- Spark Internals](https://xkx9431.github.io/spark-internals/scheduler/DAGScheduler/)

### 1.5 Narrow vs Wide Dependencies

This is one of the most important concepts for understanding Spark performance.

**Narrow Dependency**: Each partition of the child RDD depends on a **constant number** of parent partitions (typically one). Examples: `map()`, `filter()`, `flatMap()`, `union()`. Narrow transformations can be **pipelined** within a single stage -- no data needs to move between nodes.

**Wide Dependency (Shuffle Dependency)**: Each partition of the child RDD may depend on **all** partitions of the parent. Examples: `groupBy()`, `join()`, `distinct()`, `orderBy()`, `repartition()`. Wide transformations require a **shuffle** -- data must be redistributed across the cluster. Each wide dependency creates a **stage boundary**.

```
Narrow (pipelined)           Wide (shuffle boundary)
┌─────────┐                  ┌─────────┐
│ Part 0  │──► map ──► filter   │ Part 0  │──┐
└─────────┘                  └─────────┘  │  ┌─────────┐
┌─────────┐                  ┌─────────┐  ├─►│ Part 0' │ (groupBy)
│ Part 1  │──► map ──► filter   │ Part 1  │──┤  └─────────┘
└─────────┘                  └─────────┘  │  ┌─────────┐
┌─────────┐                  ┌─────────┐  ├─►│ Part 1' │
│ Part 2  │──► map ──► filter   │ Part 2  │──┘  └─────────┘
└─────────┘                  └─────────┘

No data movement needed      Data must be redistributed
```

**Why this matters**: Every shuffle is disk I/O + network I/O + serialization. Minimizing shuffles is the #1 optimization lever in Spark. Think of it this way: in Redshift, the optimizer handles redistribution silently. In Spark, *you* control it.

### 1.6 Lazy Evaluation & Lineage

Spark uses **lazy evaluation**: when you write `df.filter(...)` or `df.withColumn(...)`, Spark does **not** execute anything. Instead, it records the transformation in a **lineage graph** -- a DAG that describes *how* to compute each RDD/DataFrame from its parents.

Execution only happens when you call an **action** -- an operation that requires a result to be returned to the Driver or written to storage.

**Common Transformations** (lazy -- nothing happens):
- `select()`, `filter()` / `where()`, `withColumn()`, `groupBy()`, `join()`, `orderBy()`, `distinct()`, `union()`, `repartition()`

**Common Actions** (trigger execution):
- `count()`, `show()`, `collect()`, `take()`, `first()`, `write.parquet()`, `write.saveAsTable()`, `foreach()`

**Why lazy evaluation?**

1. **Optimization**: By seeing the entire plan before executing, Catalyst can reorder operations, push filters down, prune columns, and pick optimal join strategies.

2. **Fault Tolerance via Lineage**: If an executor dies mid-computation, Spark does *not* need replicated copies of intermediate data. It simply replays the lineage from the last materialized point (e.g., the source file, or a cached DataFrame). This is fundamentally different from systems that rely on data replication (like HDFS block replication for durability). Spark trades *recomputation* for *storage savings*.

```python
# Nothing executes here -- Spark just builds the DAG
df = spark.read.parquet("s3://bucket/events/")
filtered = df.filter(df.event_type == "purchase")
grouped = filtered.groupBy("user_id").agg({"amount": "sum"})

# NOW Spark submits a job and executes the entire DAG
grouped.show()
```

> **Citation**: [RDD Programming Guide -- Spark Documentation](https://spark.apache.org/docs/latest/rdd-programming-guide.html)
>
> **Citation**: [Understanding Fault Tolerance and Lineage -- SparkCodeHub](https://www.sparkcodehub.com/spark/fundamentals/understanding-fault-tolerance-lineage)

---

## Level 2: The Three APIs

### 2.1 RDDs (Resilient Distributed Datasets)

RDDs are Spark's **original** and lowest-level API. An RDD is an immutable, partitioned collection of objects distributed across the cluster.

```python
sc = spark.sparkContext
rdd = sc.parallelize([1, 2, 3, 4, 5], numSlices=3)
squared = rdd.map(lambda x: x * x)
result = squared.collect()  # [1, 4, 9, 16, 25]
```

**Key characteristics**:
- **No schema**: RDDs are just collections of Python/Java/Scala objects. Spark knows nothing about the structure of the data inside.
- **No optimizer**: Because there is no schema, the Catalyst optimizer **cannot** optimize RDD operations. What you write is what runs.
- **Full control**: You can do anything -- arbitrary Python logic, custom partitioners, low-level accumulator/broadcast patterns.
- **Fault tolerant**: Each RDD tracks its lineage. Lost partitions are recomputed from parent RDDs.

**When to use RDDs in 2025**: Almost never. The only remaining use cases are:
- You need fine-grained control over physical data placement (custom `Partitioner`).
- You are working with truly unstructured data that cannot fit a schema.
- You are maintaining legacy code.

> **Note**: As of Spark 4.0, RDDs are **no longer supported on Spark Connect** (the new client-server architecture). This signals the direction: DataFrames are the future.

> **Citation**: [A Tale of Three Apache Spark APIs: RDDs, DataFrames, and Datasets -- Databricks Blog](https://databricks.com/blog/2016/07/14/a-tale-of-three-apache-spark-apis-rdds-dataframes-and-datasets.html)

### 2.2 DataFrames

DataFrames are Spark's **primary API** for structured data processing. A DataFrame is a distributed collection of rows organized into **named columns** with **data types** -- conceptually identical to a table in Redshift or a `pandas` DataFrame, but distributed across a cluster.

```python
df = spark.read.parquet("s3://bucket/orders/")
df.printSchema()
# root
#  |-- order_id: long
#  |-- user_id: long
#  |-- amount: double
#  |-- order_date: date
#  |-- region: string

result = (
    df
    .filter(df.region == "US")
    .groupBy("user_id")
    .agg({"amount": "sum"})
    .withColumnRenamed("sum(amount)", "total_spend")
    .orderBy("total_spend", ascending=False)
)
result.show(10)
```

**Why DataFrames are superior to RDDs for structured data**:

1. **Catalyst Optimization**: Every DataFrame operation goes through the Catalyst optimizer. Spark can reorder joins, push filters to the data source, prune unused columns, and generate optimized JVM bytecode -- none of which is possible with RDDs.

2. **Tungsten Memory Format**: DataFrames store data in Spark's internal `UnsafeRow` binary format, which is far more memory-efficient than serialized Python objects. This means less GC pressure and better cache utilization.

3. **Language parity**: Whether you write your pipeline in Python, Scala, or SQL, the same optimized physical plan is generated. With RDDs, Python is significantly slower than Scala because data must be serialized between the JVM and the Python process.

4. **Data source integration**: DataFrames natively support reading/writing Parquet, ORC, JSON, CSV, JDBC, Delta Lake, Iceberg, and more with pushdown optimizations.

### 2.3 Datasets (Scala/Java Only)

Datasets were introduced in Spark 1.6 as a **type-safe** version of DataFrames. They combine the optimization benefits of DataFrames with compile-time type checking.

**In Python, there is no Dataset API.** Python is dynamically typed, so the compile-time type safety that Datasets provide has no meaning. In PySpark, `DataFrame` is the only structured API, and it is technically equivalent to `Dataset[Row]` in Scala.

This is one less thing to worry about: **just use DataFrames in PySpark.**

### 2.4 Transformations and Actions -- The Complete Picture

**Transformations** return a new DataFrame/RDD and are **lazy**:

| Type | Transformation | Description | Narrow/Wide |
|---|---|---|---|
| Selection | `select()`, `selectExpr()` | Choose columns | Narrow |
| Filtering | `filter()` / `where()` | Filter rows | Narrow |
| Projection | `withColumn()`, `drop()` | Add/remove columns | Narrow |
| Mapping | `map()`, `flatMap()` (RDD) | Transform each element | Narrow |
| Set ops | `union()`, `unionByName()` | Combine DataFrames | Narrow |
| Grouping | `groupBy().agg()` | Aggregate by key | **Wide** |
| Joining | `join()` | Combine two DataFrames | **Wide** (usually) |
| Sorting | `orderBy()` / `sort()` | Global sort | **Wide** |
| Dedup | `distinct()`, `dropDuplicates()` | Remove duplicates | **Wide** |
| Repartition | `repartition()` | Redistribute data | **Wide** |
| Coalesce | `coalesce()` | Reduce partitions | Narrow |

**Actions** trigger execution and return results:

| Action | Description | Where result goes |
|---|---|---|
| `show(n)` | Print first `n` rows | Driver (stdout) |
| `count()` | Count rows | Driver (single number) |
| `collect()` | Return all rows as list | Driver (memory!) |
| `take(n)` / `head(n)` | Return first `n` rows | Driver |
| `first()` | Return first row | Driver |
| `toPandas()` | Convert to pandas DataFrame | Driver (memory!) |
| `write.parquet(...)` | Write to Parquet | External storage |
| `write.saveAsTable(...)` | Write to metastore table | External storage |
| `foreach()` / `foreachBatch()` | Apply function to each row/batch | Executors |

> **Warning**: `collect()` and `toPandas()` pull **all data** to the Driver. On a large DataFrame, this will OOM the Driver. Use these only on small results (aggregated data, samples).

> **Citation**: [Quickstart: DataFrame -- PySpark Documentation](https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_df.html)

---

## Level 3: The Catalyst Optimizer -- Deep Internals

The Catalyst Optimizer is the heart of Spark SQL. When you use the DataFrame API or Spark SQL, **every query** passes through Catalyst before a single byte of data is processed. Understanding Catalyst is understanding *why* DataFrames are fast.

### 3.1 The Four-Phase Pipeline

```
Your Code (Python/SQL)
        │
        ▼
┌─────────────────────┐
│ 1. PARSED LOGICAL   │  SQL parser → Abstract Syntax Tree (AST)
│    PLAN             │  Column/table names are UNRESOLVED
└────────┬────────────┘
         ▼
┌─────────────────────┐
│ 2. ANALYZED LOGICAL │  Analyzer resolves names against the catalog
│    PLAN             │  Validates types, expands wildcards (SELECT *)
└────────┬────────────┘
         ▼
┌─────────────────────┐
│ 3. OPTIMIZED        │  Rule-based + cost-based optimizations
│    LOGICAL PLAN     │  Predicate pushdown, column pruning, etc.
└────────┬────────────┘
         ▼
┌─────────────────────┐
│ 4. PHYSICAL PLAN(S) │  Generates candidate physical plans
│                     │  Picks the best one via cost model
│                     │  Whole-stage code generation
└─────────────────────┘
```

**Phase 1 -- Parsing**: The SQL parser (or DataFrame API translator) converts your code into an unresolved logical plan -- an AST where column and table references are just strings, not yet validated.

**Phase 2 -- Analysis**: The Analyzer resolves all references against the **SessionCatalog** (Spark's metastore). It:
- Resolves column names to fully qualified attributes with data types.
- Expands `SELECT *` into explicit column lists.
- Resolves function names to their implementations.
- Validates that operations are type-compatible (e.g., you cannot `SUM` a string column).

If your query references a column that does not exist, this is where you get the error -- at analysis time, *before* any data is read.

**Phase 3 -- Logical Optimization**: The optimizer applies **batches of rules** iteratively until the plan reaches a fixed point (no more rules apply). Key optimizations:

- **Predicate Pushdown**: Moves `WHERE` filters as close to the data source as possible. If you read Parquet and then filter, Spark pushes the filter into the Parquet reader so it can skip entire row groups using min/max statistics.

  ```python
  # You write:
  df = spark.read.parquet("s3://bucket/events/")
  result = df.filter(df.year == 2025).select("user_id", "event_type")

  # Catalyst rewrites to (conceptually):
  # Read Parquet with pushed filter (year == 2025), only columns [user_id, event_type]
  ```

- **Projection Pruning (Column Pruning)**: Only reads the columns actually needed by the query. For columnar formats like Parquet, this means physically skipping entire column chunks -- massive I/O savings.

- **Constant Folding**: Evaluates constant expressions at compile time. `WHERE year = 2020 + 5` becomes `WHERE year = 2025`.

- **Boolean Simplification**: Simplifies boolean expressions. `WHERE true AND x > 5` becomes `WHERE x > 5`.

- **Join Reordering**: Reorders joins to minimize intermediate result sizes (when CBO is enabled).

**Phase 4 -- Physical Planning**: Converts the optimized logical plan into one or more **physical plans**, each specifying concrete algorithms:
- Which join algorithm? (BroadcastHashJoin, SortMergeJoin, ShuffleHashJoin)
- Which scan method? (FileScan, InMemoryTableScan for cached data)
- Where to insert Exchange (shuffle) operators?

A **cost model** selects the cheapest physical plan. Then **Whole-Stage Code Generation** fuses compatible operators into optimized JVM bytecode.

### 3.2 Viewing the Plans

```python
df = spark.read.parquet("s3://bucket/events/")
result = df.filter(df.year == 2025).groupBy("region").count()

# Show only physical plan (default)
result.explain()

# Show all 4 phases
result.explain(mode="extended")

# Show physical plan in a formatted, readable way
result.explain(mode="formatted")

# Show cost estimates (if CBO is enabled)
result.explain(mode="cost")

# Show generated Java code
result.explain(mode="codegen")
```

The `formatted` mode is the most useful for day-to-day debugging. It splits the output into a plan outline and detailed node descriptions.

> **Citation**: [EXPLAIN -- Spark SQL Documentation](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-explain.html)
>
> **Citation**: [Spark Execution Plan Deep Dive: Reading EXPLAIN Like a Pro -- Cazpian](https://cazpian.ai/blog/spark-execution-plan-deep-dive-reading-explain-like-a-pro)
>
> **Citation**: [Understanding Spark's Catalyst Optimizer -- Medium (Parth)](https://medium.com/@pparth967/understanding-sparks-catalyst-optimizer-how-your-code-gets-transformed-054da99c35ec)

### 3.3 The Cost-Based Optimizer (CBO)

Catalyst's default optimizations are **rule-based** -- they apply pattern-matching transformations without knowing the actual size of the data. The Cost-Based Optimizer (CBO) adds **statistics-aware** optimization.

CBO is **disabled by default**. To enable it:

```python
spark.conf.set("spark.sql.cbo.enabled", "true")
```

For CBO to be effective, you need to **compute statistics** on your tables:

```sql
-- Table-level stats (row count, size in bytes)
ANALYZE TABLE my_database.my_table COMPUTE STATISTICS;

-- Column-level stats (min, max, null count, distinct count, avg length)
ANALYZE TABLE my_database.my_table COMPUTE STATISTICS FOR COLUMNS user_id, amount, region;
```

With statistics, CBO can:
- **Reorder joins**: Put the smallest table first to minimize intermediate data sizes.
- **Pick better join strategies**: If it knows a table is small enough, it can choose BroadcastHashJoin instead of SortMergeJoin.
- **Estimate output sizes**: More accurate cost estimates for each plan alternative.

**Column histograms** (equi-height) provide even finer-grained statistics but are disabled by default because they require an extra table scan:

```python
spark.conf.set("spark.sql.statistics.histogram.enabled", "true")
```

> **Redshift analogy**: This is similar to `ANALYZE` in Redshift, which collects statistics for the query planner. The difference is that Redshift does this automatically (or you run `ANALYZE`), while in Spark you must explicitly run `ANALYZE TABLE`.

> **Citation**: [Cost-Based Optimization -- The Internals of Spark SQL (Jacek Laskowski)](https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-cost-based-optimization.html)

### 3.4 Whole-Stage Code Generation (Project Tungsten)

After Catalyst produces a physical plan, Spark does not interpret the plan node-by-node at runtime. Instead, it **generates Java source code** that fuses multiple operators into a single optimized function, then compiles it at runtime using the **Janino** compiler.

**Why this matters**: Traditional database engines (and early Spark) process data using an iterator model -- each operator calls `next()` on its child, which calls `next()` on *its* child, and so on. This means a virtual function call per row per operator. For a pipeline of 5 operators processing a billion rows, that is 5 billion virtual calls -- a massive CPU overhead.

Whole-stage code generation eliminates this by collapsing the entire pipeline into a **tight loop** in a single generated Java method. The loop iterates over rows, and all operations (filter, project, aggregate) are inlined. This:
- Eliminates virtual function dispatch overhead.
- Keeps intermediate values in CPU registers instead of heap memory.
- Enables the JVM JIT compiler to further optimize the generated code.

**In the physical plan**, whole-stage codegen is indicated by `*(n)` where `n` is the codegen stage ID:

```
*(2) HashAggregate(keys=[region], functions=[count(1)])
+- Exchange hashpartitioning(region, 200)
   +- *(1) HashAggregate(keys=[region], functions=[partial_count(1)])
      +- *(1) FileScan parquet [region]
```

`*(1)` means FileScan, partial HashAggregate, and any filters are all fused into one generated Java function. `*(2)` is a separate codegen stage (after the shuffle).

**Operators that break codegen**: Not all operators support code generation. `SortMergeJoin` (the sort phase), external sorts that spill to disk, and any operator involving Python UDFs will break the codegen boundary. You will see an `InputAdapter` node in the plan where codegen stops and a new stage begins.

The feature is controlled by `spark.sql.codegen.wholeStage` (enabled by default). The maximum number of fields in the output schema that codegen supports is 100 (configurable via `spark.sql.codegen.maxFields`).

> **Citation**: [Project Tungsten: Bringing Apache Spark Closer to Bare Metal -- Databricks Blog](https://www.databricks.com/blog/2015/04/28/project-tungsten-bringing-spark-closer-to-bare-metal.html)
>
> **Citation**: [Whole-Stage Java Code Generation -- The Internals of Spark SQL (Japila)](https://books.japila.pl/spark-sql-internals/whole-stage-code-generation/)

### 3.5 UDFs: Why They Kill Performance (And What to Do Instead)

A **User Defined Function (UDF)** lets you run arbitrary Python code on each row. The problem is that UDFs are a **black box** to Catalyst -- the optimizer cannot push filters through them, prune columns, or fold constants.

But the performance problem goes deeper than just optimizer bypass. When you use a standard Python UDF in PySpark, here is what happens for **every row**:

1. The row is serialized from the JVM's internal `UnsafeRow` format using cloudpickle.
2. The serialized bytes are sent from the JVM to a **Python worker process** via a socket.
3. The Python worker deserializes the row, runs your function, and serializes the result.
4. The result is sent back to the JVM and deserialized.

This JVM-to-Python round-trip **per row** is catastrophically expensive.

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Standard UDF -- SLOW. Avoid this.
@udf(returnType=StringType())
def classify_amount(amount):
    if amount > 1000:
        return "high"
    elif amount > 100:
        return "medium"
    return "low"

df.withColumn("tier", classify_amount(df.amount))
```

**Better alternatives, in order of preference**:

**1. Use built-in functions (best)**: Spark has hundreds of built-in functions (`pyspark.sql.functions`) that are implemented in the JVM and fully optimized by Catalyst.

```python
from pyspark.sql.functions import when

df.withColumn(
    "tier",
    when(df.amount > 1000, "high")
    .when(df.amount > 100, "medium")
    .otherwise("low")
)
```

**2. Use `expr()` or `selectExpr()` for SQL expressions**:

```python
df.withColumn(
    "tier",
    expr("CASE WHEN amount > 1000 THEN 'high' WHEN amount > 100 THEN 'medium' ELSE 'low' END")
)
```

**3. Pandas UDFs (Vectorized UDFs)**: If you absolutely need custom Python logic, use Pandas UDFs. They operate on **batches of rows** (pandas Series/DataFrames) instead of one row at a time, using Apache Arrow for fast serialization.

```python
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf("string")
def classify_amount_vec(amount: pd.Series) -> pd.Series:
    return pd.cut(
        amount,
        bins=[-float("inf"), 100, 1000, float("inf")],
        labels=["low", "medium", "high"]
    )

df.withColumn("tier", classify_amount_vec(df.amount))
```

Pandas UDFs can be **up to 100x faster** than row-at-a-time UDFs because Arrow eliminates per-row serialization, and pandas operations use vectorized NumPy under the hood.

**4. Arrow-Optimized UDFs (Spark 3.5+)**: A middle ground -- standard UDF syntax but using Arrow for serialization:

```python
@udf(returnType="string", useArrow=True)
def classify_arrow(amount):
    if amount > 1000:
        return "high"
    elif amount > 100:
        return "medium"
    return "low"
```

Or enable globally:

```python
spark.conf.set("spark.sql.execution.pythonUDF.arrow.enabled", "true")
```

Arrow-optimized UDFs are ~1.6-1.9x faster than cloudpickle UDFs but still not as fast as Pandas UDFs for batch operations.

> **Citation**: [Chapter 5: Unleashing UDFs & UDTFs -- PySpark 4.1.1 Documentation](https://spark.apache.org/docs/latest/api/python/user_guide/udfandudtf.html)
>
> **Citation**: [Arrow-optimized Python UDFs in Apache Spark 3.5 -- Databricks Blog](https://databricks.com/blog/arrow-optimized-python-udfs-apache-sparktm-35)
>
> **Citation**: [Pandas User-Defined Functions -- Databricks Documentation](https://docs.databricks.com/en/udf/pandas.html)

---

## Level 4: Storage & File Formats

### 4.1 Columnar vs Row-Based Storage

This is familiar territory from Redshift, which uses columnar storage internally. The same principle applies to Spark's file format choices.

**Row-based** (CSV, JSON, Avro): Each record stores all its fields contiguously. Good for write-heavy workloads and full-record access. Bad for analytical queries that only need a few columns.

**Columnar** (Parquet, ORC): Data is organized by column. All values for a single column are stored together. This enables:
- **Column pruning**: Read only the columns you need. A query on 5 columns out of 200 reads ~2.5% of the data.
- **Better compression**: Same-type values compress much better (run-length encoding, dictionary encoding, delta encoding).
- **Predicate pushdown with statistics**: Each column chunk stores min/max values, allowing Spark to skip entire row groups that cannot match a filter.

### 4.2 File Format Comparison

| Format | Type | Compression | Schema Evolution | Predicate Pushdown | Best For |
|---|---|---|---|---|---|
| **Parquet** | Columnar | Excellent (Snappy, Zstd, Gzip) | Limited (add columns OK, rename/delete tricky) | Yes (min/max stats, dictionary, bloom filters) | Analytical workloads (the default choice) |
| **ORC** | Columnar | Excellent (Zlib, Snappy, LZO) | Good | Yes (built-in indexes, bloom filters) | Hive-heavy environments |
| **Avro** | Row-based | Good (Snappy, Deflate) | Excellent (full schema evolution) | No | Streaming ingestion, write-heavy workloads, Kafka |
| **CSV** | Row-based (text) | Poor (none native; gzip externally) | None | No | Data exchange with non-Spark systems |
| **JSON** | Row-based (text) | Poor | Flexible (schema-on-read) | No | APIs, semi-structured data |

**Recommendation**: Use **Parquet** as your default. It is the native format for Spark, Delta Lake, and Iceberg. If you are migrating from Redshift's `UNLOAD` to S3, `UNLOAD ... FORMAT PARQUET` gives you the best starting point.

### 4.3 How Predicate Pushdown Works with Parquet

Parquet files are organized into **row groups** (typically 128MB each), and each row group contains **column chunks**. Each column chunk has a **page header** with statistics: min value, max value, null count.

When Spark reads Parquet with a filter like `WHERE year = 2025`:

1. **Row group pruning**: Spark reads the footer metadata of each Parquet file. If a row group's min/max for `year` is [2020, 2023], it knows no rows can match and skips the entire row group -- without reading any data bytes.

2. **Page-level filtering** (Parquet 2.0+): Within a row group, individual pages can be skipped using page-level statistics.

3. **Dictionary filtering**: If a column uses dictionary encoding, Spark can check whether the filter value exists in the dictionary before decoding any rows.

4. **Bloom filter pushdown**: For high-cardinality columns, Parquet supports bloom filters that can definitively say "this value is NOT in this row group."

```python
# Spark automatically pushes this filter into the Parquet reader
df = spark.read.parquet("s3://bucket/events/")
filtered = df.filter(df.year == 2025)

# Verify with explain:
filtered.explain()
# FileScan parquet [...] PushedFilters: [IsNotNull(year), EqualTo(year,2025)]
```

> **Redshift analogy**: This is similar to how Redshift uses zone maps (min/max per 1MB block) to skip blocks during scans. Parquet row group statistics serve the same purpose, but you control the row group size and can add bloom filters.

> **Citation**: [Predicate Pushdown in Parquet and Apache Spark -- CWI Research (Braams)](https://homepages.cwi.nl/~boncz/msc/2018-BoudewijnBraams.pdf)

### 4.4 Table Formats: Delta Lake and Iceberg

Raw Parquet files have significant limitations: no ACID transactions, no schema enforcement, no time travel, and painful handling of updates/deletes. **Table formats** solve this by adding a metadata layer on top of data files.

**Delta Lake** (by Databricks):
- Transaction log (`_delta_log/`) records every change as a JSON file.
- ACID transactions, time travel (`VERSION AS OF`), schema enforcement and evolution.
- Optimizations: Z-Ordering for multi-dimensional clustering, data skipping via file-level statistics, `OPTIMIZE` for compaction.
- Tightly integrated with Databricks; open-source version available.

**Apache Iceberg**:
- Metadata layer with manifest files tracking individual data files and their statistics.
- Engine-agnostic: works with Spark, Flink, Trino, Snowflake, BigQuery.
- Hidden partitioning (no need to include partition columns in queries).
- Partition evolution without rewriting data.

**When to choose which**:
- If you are on Databricks: Delta Lake is the natural choice (first-class support, Unity Catalog integration).
- If you need multi-engine access or are on a open lakehouse: Iceberg is more portable.
- Both are vast improvements over raw Parquet files for production data lakes.

### 4.5 Compression Codecs

| Codec | Speed | Compression Ratio | Splittable | Use When |
|---|---|---|---|---|
| **Snappy** | Very fast | Moderate | No (but Parquet handles it) | Default for most workloads; balanced speed/size |
| **Zstd** | Fast | High | No | Want better compression with acceptable speed |
| **Gzip** | Slow | High | No | Archival, cold storage where read speed matters less |
| **LZ4** | Fastest | Low | No | Shuffle-heavy workloads (internal use) |
| **Uncompressed** | N/A | None | Yes | Debugging, or when CPU is the bottleneck |

```python
# Write with Zstandard compression
df.write.option("compression", "zstd").parquet("s3://bucket/output/")

# Set default compression for the session
spark.conf.set("spark.sql.parquet.compression.codec", "zstd")
```

> **Citation**: [The Data Engineer's Guide to File Formats -- Medium (Towards Data Engineering)](https://medium.com/towards-data-engineering/the-data-engineers-guide-to-file-formats-parquet-vs-orc-vs-avro-470e1d7f7643)
>
> **Citation**: [Benchmarking File Formats: Parquet vs Iceberg vs Delta in 2025 -- Medium (Manik Hossain)](https://medium.com/@manik.ruet08/benchmarking-file-formats-parquet-vs-iceberg-vs-delta-in-2025-8324e4355859)

---

## Level 5: Shuffle -- The Most Expensive Operation

If your Spark job is slow, it is **almost always because of shuffle**. Shuffle is the mechanism by which Spark redistributes data across partitions -- and it involves disk I/O, network I/O, and serialization. Understanding shuffle deeply is essential for optimization.

### 5.1 What Triggers a Shuffle

Any operation that requires data from **multiple input partitions** to produce a single output partition triggers a shuffle:
- `groupBy()` / `groupByKey()` / `reduceByKey()`
- `join()` (except broadcast joins)
- `distinct()` / `dropDuplicates()`
- `orderBy()` / `sort()`
- `repartition()`
- Window functions with `partitionBy`

### 5.2 Shuffle File Architecture

When a shuffle happens, Spark divides the operation into two phases:

**Map side (Shuffle Write)**:
1. Each task in the upstream stage processes its partition.
2. Each task partitions its output rows by the shuffle key (e.g., the `groupBy` column) using a hash function.
3. Rows are sorted within each partition, spilled to disk if memory is insufficient, and written as **`.data`** and **`.index`** files.
4. The `.index` file contains byte offsets so downstream tasks can fetch exactly the data they need.

**Reduce side (Shuffle Read)**:
1. Each task in the downstream stage fetches its relevant partition from **all** upstream tasks across the cluster.
2. This is the expensive part -- it is an all-to-all network communication pattern.
3. Fetched data is merged and processed.

```
Stage 0 (Map side)              Stage 1 (Reduce side)
┌──────────┐                    ┌──────────┐
│ Task 0   │──write──►.data/.idx──fetch──►│ Task 0'  │
│ Part 0   │──write──►.data/.idx──fetch──►│ Part 0'  │
└──────────┘                    └──────────┘
┌──────────┐                    ┌──────────┐
│ Task 1   │──write──►.data/.idx──fetch──►│ Task 1'  │
│ Part 1   │──write──►.data/.idx──fetch──►│ Part 1'  │
└──────────┘                    └──────────┘
     ...        network I/O          ...
```

### 5.3 `spark.sql.shuffle.partitions`

This is arguably the **single most impactful configuration** in Spark. It controls how many partitions are created after a shuffle operation (join, groupBy, etc.). The default is **200**.

**Why the default is often wrong**:
- For small datasets (< 1GB), 200 partitions means tiny partitions with more scheduling overhead than useful work.
- For large datasets (100GB+), 200 partitions means huge partitions that can OOM individual tasks.

**Rule of thumb**: Target **100-200 MB of uncompressed data per task** after shuffle.

```python
# For a 50GB dataset after shuffle:
# 50GB / 200MB = 250 partitions (reasonable)
# 50GB / 200 default partitions = 250MB each (might be OK)

# For a 500GB dataset after shuffle:
# 500GB / 200MB = 2500 partitions
spark.conf.set("spark.sql.shuffle.partitions", "2500")

# For a 1GB dataset:
# 1GB / 200 = 5MB each (too small, too much overhead)
spark.conf.set("spark.sql.shuffle.partitions", "10")
```

**With AQE enabled** (Spark 3.0+, default in 3.2+), you can set a high initial value and let AQE automatically coalesce small partitions:

```python
spark.conf.set("spark.sql.shuffle.partitions", "2000")  # start high
spark.conf.set("spark.sql.adaptive.enabled", "true")     # let AQE coalesce
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

> **Citation**: [Mastering spark.sql.shuffle.partitions -- SparkCodeHub](https://www.sparkcodehub.com/spark/configurations/sql-shuffle-partitions)
>
> **Citation**: [Performance Tuning -- Spark 4.1.1 Documentation](https://spark.apache.org/docs/latest/sql-performance-tuning.html)

### 5.4 Partitioning Strategies

When Spark shuffles data, it uses a **partitioning strategy** to determine which row goes to which partition:

**Hash Partitioning** (default for joins and groupBy):
- `partition_id = hash(key) % num_partitions`
- Ensures all rows with the same key end up in the same partition.
- Used implicitly by `groupBy()`, `join()`, and explicitly by `repartition(n, "col")`.

**Range Partitioning**:
- Divides data into partitions based on sorted ranges of a column.
- Used by `orderBy()` / `sort()` and explicitly by `repartitionByRange()`.
- Requires sampling the data first to determine range boundaries.

**Round-Robin Partitioning**:
- Distributes rows evenly across partitions without regard to content.
- Used by `repartition(n)` (without specifying columns).

```python
# Hash partition by user_id into 100 partitions
df.repartition(100, "user_id")

# Range partition by order_date
df.repartitionByRange(100, "order_date")

# Round-robin into 50 partitions
df.repartition(50)
```

### 5.5 `repartition()` vs `coalesce()`

Both change the number of partitions, but they work very differently.

**`repartition(n, *cols)`**:
- **Always triggers a full shuffle** (data is redistributed across the cluster).
- Can increase or decrease the number of partitions.
- If columns are specified, uses hash partitioning on those columns.
- If only `n` is specified, uses round-robin partitioning.
- Results in roughly **equal-sized** partitions.

**`coalesce(n)`**:
- **Does NOT shuffle** data. It merges adjacent partitions on the same executor.
- Can only **decrease** the number of partitions (if you pass a larger number, nothing happens).
- Produces **uneven** partition sizes because it simply combines existing partitions.
- Much cheaper than `repartition()` because it avoids network I/O.

**When to use which**:

| Scenario | Use |
|---|---|
| Reducing partitions before writing (avoid small files) | `coalesce()` |
| Increasing partitions for more parallelism | `repartition()` |
| Ensuring data is co-partitioned for a join | `repartition(n, "join_key")` |
| Balanced output files of similar size | `repartition()` |
| After a heavy filter that emptied many partitions | `coalesce()` to drop empty partitions |

```python
# Writing to S3 -- reduce 200 shuffle partitions to 10 output files
df.coalesce(10).write.parquet("s3://bucket/output/")

# Need to increase parallelism
df.repartition(500).filter(...).groupBy(...)

# Co-partition two DataFrames before joining
df1 = df1.repartition(200, "user_id")
df2 = df2.repartition(200, "user_id")
result = df1.join(df2, "user_id")
```

> **Citation**: [Coalesce vs. Repartition in Apache Spark -- SparkCodeHub](https://www.sparkcodehub.com/spark/performance/coalesce-vs-repartition)
>
> **Citation**: [Repartition and RepartitionByExpression -- The Internals of Spark SQL (Jacek Laskowski)](https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-LogicalPlan-Repartition-RepartitionByExpression.html)

### 5.6 Bucketing: Pre-Organizing Data to Avoid Shuffles

Bucketing is a storage optimization that **pre-hashes** data into a fixed number of buckets when writing. When two tables are bucketed on the same columns with the same number of buckets, Spark can perform a **shuffle-free join**.

```python
# Write bucketed tables
(
    orders_df
    .write
    .bucketBy(256, "user_id")
    .sortBy("user_id")
    .saveAsTable("orders_bucketed")
)

(
    users_df
    .write
    .bucketBy(256, "user_id")
    .sortBy("user_id")
    .saveAsTable("users_bucketed")
)

# Join without shuffle!
orders = spark.table("orders_bucketed")
users = spark.table("users_bucketed")
result = orders.join(users, "user_id")  # No Exchange node in the plan
```

**Limitations of bucketing**:
- Only works with `saveAsTable()` (writes to the Hive metastore), not `save()` or `insertInto()`.
- Both tables must have the **same number of buckets** and be bucketed on the **same columns**.
- Only supported for file-based data sources (Parquet, ORC, JSON, CSV).
- The initial write is expensive (it is a full shuffle + sort).
- Schema changes require re-bucketing.

> **Redshift analogy**: Bucketing is closest to Redshift's `DISTKEY`. A `DISTKEY` on `user_id` ensures that rows with the same `user_id` live on the same node, enabling co-located joins. Bucketing does the same thing at the file level.

> **Citation**: [Bucketing -- The Internals of Spark SQL (Jacek Laskowski)](https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/spark-sql-bucketing.html)
>
> **Citation**: [Best Practices for Bucketing in Spark SQL -- Medium (David Vrba)](https://medium.com/data-science/best-practices-for-bucketing-in-spark-sql-ea9f23f7dd53)

---

## Level 6: Joins -- Strategies and Optimization

Joins are where most Spark performance problems live. Unlike Redshift, where the optimizer picks a join strategy opaquely, Spark exposes its join strategies and lets you influence them.

### 6.1 The Five Join Strategies

Spark supports five physical join strategies. The optimizer picks one based on table sizes, join type, and configuration:

#### 1. Broadcast Hash Join (BHJ) -- The Fastest

**How it works**:
1. The **smaller** DataFrame is collected to the Driver.
2. The Driver broadcasts it to **every executor** in the cluster.
3. Each executor builds an in-memory hash table from the broadcast data.
4. Each executor joins its local partition of the larger DataFrame against the hash table.

**No shuffle required.** This is the fastest join strategy because it eliminates all-to-all network communication.

**When Spark uses it**:
- One side is smaller than `spark.sql.autoBroadcastJoinThreshold` (default: **10 MB**).
- The join type supports it (all types except FULL OUTER JOIN).
- The join condition is an equi-join (`==`).

```python
from pyspark.sql.functions import broadcast

# Force a broadcast join (even if the table is > 10MB)
result = large_df.join(broadcast(small_df), "join_key")

# Adjust the threshold (set to 100MB)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(100 * 1024 * 1024))

# Disable broadcast joins entirely
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
```

**Practical limit**: The broadcast data must fit in the Driver's memory and each executor's memory. In practice, you can broadcast tables up to **1-2 GB**, though the theoretical limit per executor is ~8 GB. Beyond that, use a different strategy.

> **Redshift analogy**: In Redshift, this is analogous to a `DS_DIST_NONE` or `DS_BCAST_INNER` strategy, where the inner table is broadcast to all nodes.

#### 2. Sort Merge Join (SMJ) -- The Default

**How it works**:
1. **Shuffle**: Both DataFrames are hash-partitioned by the join key. This ensures all rows with the same key end up on the same executor.
2. **Sort**: Within each partition, both sides are sorted by the join key.
3. **Merge**: A merge-join algorithm walks through both sorted sequences simultaneously, matching rows with the same key.

**When Spark uses it**: This is the default for equi-joins when neither side is small enough to broadcast.

```
In the physical plan:
Exchange hashpartitioning(join_key, 200)    ← shuffle left side
  +- Sort [join_key ASC]                    ← sort left side
Exchange hashpartitioning(join_key, 200)    ← shuffle right side
  +- Sort [join_key ASC]                    ← sort right side
SortMergeJoin [join_key]                    ← merge
```

**Performance characteristics**:
- Complexity: O(n log n) due to sorting.
- Handles very large datasets -- both sides can be arbitrarily large.
- Requires two full shuffles + sorts.
- Sensitive to **data skew**: if one key has vastly more rows than others, the task handling that key becomes a bottleneck.

#### 3. Shuffle Hash Join (SHJ)

**How it works**:
1. **Shuffle**: Both DataFrames are hash-partitioned by the join key.
2. **Hash**: For each partition, the **smaller** side is loaded into an in-memory hash table.
3. **Probe**: The larger side probes the hash table for matches.

**When Spark uses it**: Only when `spark.sql.join.preferSortMergeJoin` is set to `false` AND one side is significantly smaller than the other (but too large to broadcast).

```python
# Enable shuffle hash join (not recommended as default)
spark.conf.set("spark.sql.join.preferSortMergeJoin", "false")
```

**Performance**: Avoids the sort step, making it faster when one side fits in memory per-partition. But if the smaller side is too large for memory, it spills to disk and becomes slower than SMJ.

#### 4. Broadcast Nested Loop Join (BNLJ)

**How it works**: The smaller side is broadcast, and for each row in the larger side, every row in the smaller side is checked. This is the O(n×m) brute-force join.

**When Spark uses it**: Non-equi joins (e.g., `a.value > b.threshold`) where one side is small enough to broadcast.

```python
# Cross join with a condition
result = large_df.join(broadcast(thresholds_df), large_df.value > thresholds_df.threshold)
```

#### 5. Cartesian Product (Nested Loop Join)

The same O(n×m) algorithm, but without broadcasting. Both sides are fully shuffled. This is the **most expensive** join strategy and should be avoided at all costs.

Spark uses this for cross joins (`CROSS JOIN`) or non-equi joins where neither side is small enough to broadcast.

### 6.2 Reading Join Strategies from `explain()`

```python
result = orders.join(users, "user_id")
result.explain()
```

Look for these keywords in the physical plan:

| Keyword in Plan | Join Strategy |
|---|---|
| `BroadcastHashJoin` | Broadcast Hash Join |
| `SortMergeJoin` | Sort Merge Join |
| `ShuffledHashJoin` | Shuffle Hash Join |
| `BroadcastNestedLoopJoin` | Broadcast Nested Loop Join |
| `CartesianProduct` | Cartesian Product / Nested Loop |

Also look for `Exchange` nodes -- they indicate shuffles. A `BroadcastExchange` node means data is being broadcast. An `Exchange hashpartitioning(...)` means a shuffle.

### 6.3 Handling Data Skew in Joins

Data skew is when one or a few join keys have drastically more rows than others. The classic symptom: 99 out of 100 tasks finish in 30 seconds, but task #47 runs for 45 minutes because it received 80% of the data.

#### Technique 1: AQE Skew Join Handling (Spark 3.0+)

The easiest solution. AQE automatically detects and splits skewed partitions.

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
# Partition is considered skewed if it's 5x bigger than median and > 256MB
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256m")
```

#### Technique 2: Key Salting (Manual)

When AQE is not available or insufficient, salting is the go-to technique. It works by splitting hot keys across multiple partitions:

```python
from pyspark.sql.functions import floor, rand, lit, explode, array, col

SALT_BUCKETS = 10

# Step 1: Add random salt to the skewed (large) side
skewed_df = large_df.withColumn("salt", floor(rand() * SALT_BUCKETS).cast("int"))

# Step 2: Explode the small side to have one row per salt bucket
small_expanded = small_df.withColumn(
    "salt",
    explode(array(*[lit(i) for i in range(SALT_BUCKETS)]))
)

# Step 3: Join on composite key (original key + salt)
result = skewed_df.join(small_expanded, ["join_key", "salt"]).drop("salt")
```

**What this does**: If `join_key = "ACME_CORP"` has 10 million rows on the left side, salting distributes them across 10 partitions (1 million each) instead of stuffing all 10 million into one task. The right side is replicated 10x (one copy per salt value), which is acceptable because it is the smaller table.

#### Technique 3: Broadcast the Small Side

If one side is small enough, broadcast it to eliminate the shuffle entirely. Skew becomes irrelevant because every executor has the full small table.

```python
result = large_df.join(broadcast(small_df), "join_key")
```

> **Citation**: [Demystifying Spark Join Strategies -- Medium (CoderRaj)](https://coderraj07.medium.com/demystifying-spark-join-strategies-shuffle-sort-merge-shuffle-hash-and-broadcast-joins-with-5d8011772b13)
>
> **Citation**: [Handling Data Skew in Spark: The Power of Salting -- CloudThat](https://www.cloudthat.com/resources/blog/handling-data-skew-in-spark-the-power-of-salting/)
>
> **Citation**: [Demystifying Spark Shuffle Internals -- Medium (Lokesh Gupta)](https://medium.com/@lokeshgupta2206/demystifying-spark-shuffle-internals-advanced-1bc3a4c99151)

---

## Level 7: Memory Management & Caching

### 7.1 Executor Memory Layout

Understanding how Spark uses memory is crucial for sizing clusters and debugging OOM errors. Each executor's memory is divided into several regions:

```
┌──────────────────────────────────────────────────┐
│             Total Container Memory               │
│  (spark.executor.memory + memoryOverhead +       │
│   offHeap + pyspark.memory)                      │
│                                                  │
│  ┌──────────────────────────────────────────┐    │
│  │        spark.executor.memory (heap)      │    │
│  │                                          │    │
│  │  ┌─────────────────────────────────┐     │    │
│  │  │    Reserved Memory (300 MB)     │     │    │
│  │  └─────────────────────────────────┘     │    │
│  │  ┌─────────────────────────────────┐     │    │
│  │  │    User Memory (~40%)           │     │    │
│  │  │    (your data structures,       │     │    │
│  │  │     variables, UDF state)       │     │    │
│  │  └─────────────────────────────────┘     │    │
│  │  ┌─────────────────────────────────┐     │    │
│  │  │    Unified Memory (~60%)        │     │    │
│  │  │  ┌────────────┬──────────────┐  │     │    │
│  │  │  │ Execution  │   Storage    │  │     │    │
│  │  │  │ (shuffle,  │   (cache,    │  │     │    │
│  │  │  │  joins,    │    persist)  │  │     │    │
│  │  │  │  sorts,    │              │  │     │    │
│  │  │  │  aggs)     │              │  │     │    │
│  │  │  └────────────┴──────────────┘  │     │    │
│  │  └─────────────────────────────────┘     │    │
│  └──────────────────────────────────────────┘    │
│  ┌──────────────────────────────────────────┐    │
│  │  Memory Overhead                         │    │
│  │  max(384MB, 0.10 * spark.executor.memory)│    │
│  │  (JVM overhead, thread stacks, native    │    │
│  │   libs, Python process for PySpark)      │    │
│  └──────────────────────────────────────────┘    │
│  ┌──────────────────────────────────────────┐    │
│  │  PySpark Memory (spark.executor           │    │
│  │  .pyspark.memory, default: 0)            │    │
│  └──────────────────────────────────────────┘    │
└──────────────────────────────────────────────────┘
```

**Unified Memory** (controlled by `spark.memory.fraction`, default 0.6):
- Split between **Execution** and **Storage**.
- They can **borrow from each other**. If storage is using only 20% and execution needs more, execution can use the rest. If execution demands its memory back, cached data in storage can be evicted.
- This dynamic sharing is called the **Unified Memory Manager** (since Spark 1.6).

**Execution Memory**: Used for shuffles, joins, sorts, and aggregations. Intermediate data that must be kept in memory during computation.

**Storage Memory**: Used for caching (`cache()` / `persist()`) and broadcast variables.

**User Memory**: Your Python/JVM objects, UDF state, custom data structures. If you create large dictionaries or lists in a UDF, they come from here.

**Memory Overhead**: JVM internals (thread stacks, class metadata, JIT-compiled code, native libraries). For PySpark, the Python worker processes also draw from this pool. This is **outside** the Java heap.

**Common OOM causes**:
- Execution memory exhaustion: too-large shuffle partitions. Fix: increase `spark.sql.shuffle.partitions`.
- Storage memory pressure: caching too much data. Fix: unpersist unneeded DataFrames, or use `MEMORY_AND_DISK`.
- Driver OOM: calling `collect()` or `toPandas()` on large datasets. Fix: don't.
- Python memory overhead: Large PySpark UDFs. Fix: use Pandas UDFs or built-in functions.

### 7.2 Project Tungsten

Tungsten is a suite of low-level optimizations that make Spark significantly faster. Three pillars:

**1. Off-Heap Memory Management**: Spark can allocate memory outside the JVM heap using `sun.misc.Unsafe`, bypassing garbage collection. Data is stored in the **UnsafeRow** format -- a compact binary representation where each row is a contiguous byte array with fixed-width fields at known offsets. This eliminates Java object overhead (each Java object typically has 16 bytes of header).

**2. Cache-Aware Computation**: Data structures are designed to exploit CPU cache hierarchy (L1/L2/L3). Pointer-free data structures with sequential memory access patterns maximize cache hit rates.

**3. Whole-Stage Code Generation**: (Covered in Section 3.4) Fuses operators into tight loops, keeping intermediate values in CPU registers.

> **Why this matters for PySpark engineers**: Even though you write Python, all DataFrame operations execute in the JVM using Tungsten's optimized memory format. Your Python code just builds the plan -- the actual data processing happens in optimized JVM code. This is why DataFrames are fast even from Python, while RDD operations in Python are slow (data must cross the JVM-Python boundary).

> **Citation**: [Project Tungsten: Bringing Apache Spark Closer to Bare Metal -- Databricks Blog](https://www.databricks.com/blog/2015/04/28/project-tungsten-bringing-spark-closer-to-bare-metal.html)
>
> **Citation**: [Spark Memory Fundamentals: How Executors Really Allocate Memory -- Raul San Martin](https://raulsanmartin.me/big/data/spark-memory-fundamentals-executor-allocation/)

### 7.3 `cache()` vs `persist()` and Storage Levels

Caching keeps a DataFrame's computed results in memory (or disk) so that subsequent actions on the same DataFrame do not recompute it from scratch.

```python
# cache() is shorthand for persist(StorageLevel.MEMORY_AND_DISK)
df_cached = df.cache()

# persist() lets you choose a specific storage level
from pyspark import StorageLevel
df_persisted = df.persist(StorageLevel.MEMORY_ONLY)
```

**Storage Levels**:

| Level | Where | Serialized? | Behavior |
|---|---|---|---|
| `MEMORY_ONLY` | Heap only | No (Java objects) | Fastest reads; recomputes if evicted |
| `MEMORY_AND_DISK` | Heap + disk spillover | No | Default for `cache()`; spills to disk when memory is full |
| `MEMORY_ONLY_SER` | Heap only | Yes (compact binary) | Less memory, slower reads due to deserialization |
| `MEMORY_AND_DISK_SER` | Heap + disk | Yes | Compact + reliable |
| `DISK_ONLY` | Disk only | Yes | Slowest reads; use when memory is scarce |
| `OFF_HEAP` | Off-heap memory | Yes | Avoids GC; requires off-heap memory configuration |

**When to cache**:
- A DataFrame is used in **multiple actions** (e.g., writing to two different outputs, or a join followed by a separate aggregation).
- An expensive computation (large join, complex aggregation) is reused.
- Iterative algorithms (ML training loops) where the same data is scanned multiple times.

**When NOT to cache**:
- A DataFrame is used only once. Caching adds overhead (storage, eviction management) for zero benefit.
- The DataFrame is very large and caching it would evict other useful data from memory.
- The source data is already in a fast format (e.g., Parquet on local SSD or Delta Lake with data skipping).

**Critical detail**: `cache()` is **lazy**. The data is not actually cached until an action triggers computation.

```python
df = spark.read.parquet("s3://bucket/data/")
processed = df.filter(...).groupBy(...).agg(...)

# This only marks the DataFrame for caching
processed.cache()

# THIS triggers the computation and caching
processed.count()

# Now this action reads from cache instead of re-reading S3 + recomputing
processed.write.parquet("s3://bucket/output/")

# IMPORTANT: release the cache when done
processed.unpersist()
```

### 7.4 Broadcast Variables and Accumulators

These are **shared variables** -- mechanisms for efficiently sharing data between the Driver and Executors.

**Broadcast Variables**: Read-only, immutable data distributed to all executors **once** (not per-task). Spark uses efficient broadcast protocols (BitTorrent-like peer-to-peer distribution) to minimize network usage.

```python
# Example: lookup table for enrichment
country_codes = {"US": "United States", "GB": "United Kingdom", "DE": "Germany"}
broadcast_codes = spark.sparkContext.broadcast(country_codes)

# In a UDF or RDD operation, access via .value
@udf("string")
def get_country_name(code):
    return broadcast_codes.value.get(code, "Unknown")

df.withColumn("country_name", get_country_name(df.country_code))
```

Without broadcasting, `country_codes` would be serialized and sent with **every task** -- 200 shuffle partitions means 200 copies sent over the network. With broadcasting, it is sent **once per executor**.

**Accumulators**: Write-only (from executors), read-only (from Driver). Used for side-channel aggregations like counters and error tracking.

```python
bad_records = spark.sparkContext.accumulator(0)

def process_row(row):
    if row.amount is None or row.amount < 0:
        bad_records.add(1)
        return None
    return row.amount * 1.1

rdd = df.rdd.map(process_row)
rdd.count()  # triggers execution
print(f"Bad records: {bad_records.value}")
```

> **Warning**: Accumulators are only guaranteed to be accurate inside **actions**, not transformations. If a task is retried (due to failure or speculation), the accumulator may be incremented multiple times. Use them for approximate monitoring, not for business logic.

> **Citation**: [RDD Programming Guide: Shared Variables -- Spark Documentation](https://spark.apache.org/docs/latest/rdd-programming-guide.html#shared-variables)
>
> **Citation**: [Introduction to Broadcast and Accumulator Variables -- Medium (Gaurav)](https://gaurav98095.medium.com/introduction-to-broadcast-and-accumulator-variables-in-spark-4fc1efd80c35)

---

## Level 8: Adaptive Query Execution (AQE)

AQE is one of the most impactful features introduced in Spark 3.0. It **re-optimizes the query plan at runtime** based on actual data statistics collected after each shuffle stage completes. This is a game-changer because compile-time estimates are often wildly wrong.

**Enabled by default since Spark 3.2.** If you are on an older version:

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

### 8.1 How AQE Works

Traditional Spark compiles the entire physical plan before execution begins. AQE changes this:

1. Spark executes the query plan up to the first **shuffle** or **broadcast exchange**.
2. At the exchange boundary, it **materializes** the shuffle output.
3. With actual partition sizes now known, AQE **re-optimizes** the remaining plan.
4. Steps 1-3 repeat for each subsequent stage.

This means the plan **evolves during execution**. In `explain()` output, you will see `AdaptiveSparkPlan` with `isFinalPlan=false` initially, changing to `true` after execution completes.

### 8.2 AQE Feature 1: Dynamic Partition Coalescing

**Problem**: You set `spark.sql.shuffle.partitions = 2000` to handle a large dataset, but after filtering, most partitions are nearly empty. You end up with 2000 tasks where 1800 of them process < 1 MB each -- massive scheduling overhead.

**AQE Solution**: After the shuffle, AQE checks the actual partition sizes and merges adjacent small partitions into larger ones, targeting the configured advisory size.

```python
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")  # default: true
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128m")  # target size
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "1m")
```

**Practical effect**: You can set `shuffle.partitions` high (2000-5000) as a safety net for large data, and AQE will automatically reduce it when the actual data is small. No more manual tuning per query.

### 8.3 AQE Feature 2: Dynamic Join Strategy Switching

**Problem**: At compile time, Spark estimates that a table is 500 MB (too large to broadcast). After applying filters, the actual size is only 8 MB.

**AQE Solution**: After the filter stage executes and the actual post-filter size is known, AQE switches the join strategy from SortMergeJoin to BroadcastHashJoin.

```python
spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))
```

This is separate from `spark.sql.autoBroadcastJoinThreshold` (the compile-time threshold). AQE can apply broadcasting even if the compile-time estimate was too high, because it uses **actual** post-shuffle sizes.

### 8.4 AQE Feature 3: Skew Join Handling

**Problem**: In a SortMergeJoin, one partition has 10 GB of data while all others have ~100 MB. That one partition becomes a straggler, holding up the entire stage.

**AQE Solution**: AQE detects the skewed partition at runtime and automatically splits it into multiple smaller sub-partitions. For the other side of the join, the corresponding partition is replicated to match each sub-partition.

```python
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")  # default: true in 3.2+

# A partition is considered skewed if BOTH conditions are true:
# 1. It is larger than skewedPartitionFactor * median partition size
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
# 2. It is larger than this absolute threshold
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256m")
```

**How it looks in the plan**: You will see `SkewJoin` annotations in the `AdaptiveSparkPlan` output after execution.

### 8.5 AQE Feature 4: Dynamic Empty Relation Propagation

If AQE detects that one side of a join has zero rows after execution, it can short-circuit the entire join operation and return an empty result immediately.

### 8.6 AQE Configuration Reference

| Config | Default | Description |
|---|---|---|
| `spark.sql.adaptive.enabled` | `true` (3.2+) | Master switch for AQE |
| `spark.sql.adaptive.coalescePartitions.enabled` | `true` | Auto-coalesce small partitions |
| `spark.sql.adaptive.coalescePartitions.initialPartitionNum` | (none) | Initial partition count; defaults to `spark.sql.shuffle.partitions` |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes` | `64m` | Target size for coalesced partitions |
| `spark.sql.adaptive.skewJoin.enabled` | `true` | Auto-handle skewed joins |
| `spark.sql.adaptive.skewJoin.skewedPartitionFactor` | `5` | Skew factor relative to median |
| `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` | `256m` | Absolute skew threshold |
| `spark.sql.adaptive.autoBroadcastJoinThreshold` | (same as compile-time) | Runtime broadcast threshold |

> **Citation**: [Adaptive Query Execution -- Databricks Documentation](https://docs.databricks.com/aws/en/optimizations/aqe)
>
> **Citation**: [Optimizing Spark Performance with AQE: Mastering Shuffle Partition Coalescing -- Medium (Vincent Daniel)](https://medium.com/@vincent_daniel/optimizing-spark-performance-with-aqe-mastering-shuffle-partition-coalescing-e9d3eef8579f)

---

## Level 9: Performance Tuning Playbook

### 9.1 Reading Explain Plans Like a Pro

The explain plan is your primary debugging tool. Here is how to read it systematically:

```python
result = (
    spark.read.parquet("s3://bucket/orders/")
    .filter("order_date >= '2025-01-01'")
    .join(spark.read.parquet("s3://bucket/users/"), "user_id")
    .groupBy("region")
    .agg({"amount": "sum"})
)

result.explain(mode="formatted")
```

**Read the plan bottom-up** (leaf nodes are data sources, the root is the final output):

```
== Physical Plan ==
*(3) HashAggregate(keys=[region], functions=[sum(amount)])
+- Exchange hashpartitioning(region, 200)                   ← SHUFFLE for groupBy
   +- *(2) HashAggregate(keys=[region], functions=[partial_sum(amount)])
      +- *(2) Project [region, amount]
         +- *(2) BroadcastHashJoin [user_id], [user_id]     ← JOIN strategy
            :- *(2) Project [user_id, amount]
            :  +- *(2) Filter (order_date >= 2025-01-01)
            :     +- *(2) FileScan parquet [user_id,amount,order_date]
            :        PushedFilters: [GTE(order_date,2025-01-01)]  ← pushed to source
            +- BroadcastExchange                             ← broadcast
               +- *(1) FileScan parquet [user_id,region]
```

**What to look for**:

| Element | What it Means | Action |
|---|---|---|
| `Exchange hashpartitioning(...)` | A shuffle is happening | Can you eliminate it? (broadcast, bucketing) |
| `SortMergeJoin` | Both sides are shuffled and sorted | Is one side small enough to broadcast? |
| `BroadcastHashJoin` | Small side is broadcast | Good! Check that the broadcast side is actually small. |
| `PushedFilters: [...]` | Filters pushed to the data source | Good! Check that your filters are being pushed down. |
| `*(n)` | Whole-stage codegen ID | Operators within the same `*()` are fused into one function. |
| `FileScan ... PartitionFilters: [...]` | Partition pruning | Good! Spark is skipping irrelevant partitions. |

### 9.2 The Spark UI

The Spark UI (default port 4040) is your runtime debugging tool. Key tabs:

**Jobs Tab**: Shows all jobs with status, duration, and DAG visualization. Start here to find the slowest job.

**Stages Tab**: Drill into stages. Look for:
- **Skew**: In task duration distribution, if the max is 100x the median, you have skew.
- **Spill**: If "Shuffle Spill (Memory)" and "Shuffle Spill (Disk)" are large, tasks are running out of execution memory. Increase executor memory or reduce partition size.
- **GC Time**: If tasks spend > 10% of time in GC, you have memory pressure. Consider using serialized caching, reducing cache size, or increasing executor memory.

**SQL Tab**: Shows the executed query plan with runtime metrics (rows read, bytes shuffled, time per operator). This is the richest view for debugging DataFrame/SQL queries.

**Executors Tab**: Shows resource utilization per executor. Check for uneven task distribution and memory usage.

**For post-mortem analysis**, enable the Spark History Server:

```python
spark.conf.set("spark.eventLog.enabled", "true")
spark.conf.set("spark.eventLog.dir", "s3://bucket/spark-events/")
```

> **Citation**: [Web UI -- Spark Documentation](https://spark.apache.org/docs/latest/web-ui.html)
>
> **Citation**: [Diagnose cost and performance issues using the Spark UI -- Databricks](https://docs.databricks.com/en/optimizations/spark-ui-guide/index.html)

### 9.3 Speculative Execution

**Problem**: One task takes 10x longer than the others in the same stage, but it is not due to data skew (the partition sizes are similar). The cause might be a slow disk, a noisy neighbor on the same node, or a transient network issue.

**Solution**: Speculative execution launches a **duplicate copy** of slow tasks on different nodes. Whichever copy finishes first wins; the other is killed.

```python
spark.conf.set("spark.speculation", "true")
spark.conf.set("spark.speculation.multiplier", "1.5")   # 1.5x slower than median
spark.conf.set("spark.speculation.quantile", "0.75")     # wait until 75% of tasks complete
spark.conf.set("spark.speculation.minTaskRuntime", "60s") # don't speculate on fast tasks
```

**When to use**: Good for clusters with heterogeneous hardware or cloud environments with noisy neighbors. **Do not** enable for skewed data (speculation will just launch more copies of the expensive partition, wasting resources).

> **Citation**: [Understanding Speculative Execution -- Databricks KB](https://kb.databricks.com/scala/understanding-speculative-execution)

### 9.4 Serialization: Kryo vs Java

Spark serializes data when shuffling (between executors) and when sending tasks to executors. The default Java serializer is slow and produces large output.

**Kryo** serializer is ~10x faster and more compact:

```python
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
```

Kryo is particularly impactful for RDD operations and shuffle-heavy workloads. For DataFrame operations, the impact is smaller because Tungsten uses its own binary format (UnsafeRow) for internal data, but shuffle data still benefits from Kryo.

### 9.5 Key Configuration Cheat Sheet

| Config | Default | Recommendation |
|---|---|---|
| `spark.sql.shuffle.partitions` | 200 | Set based on data size: target 100-200 MB/partition. Use AQE for auto-tuning. |
| `spark.sql.autoBroadcastJoinThreshold` | 10 MB | Increase to 100-500 MB if you have memory. Set to `-1` to disable. |
| `spark.sql.adaptive.enabled` | `true` (3.2+) | Always enable. |
| `spark.sql.adaptive.skewJoin.enabled` | `true` | Always enable. |
| `spark.serializer` | Java | Use `KryoSerializer`. |
| `spark.sql.parquet.compression.codec` | `snappy` | Use `zstd` for better compression. |
| `spark.sql.files.maxPartitionBytes` | 128 MB | Controls max partition size when reading files. |
| `spark.sql.files.openCostInBytes` | 4 MB | Overhead of opening a file; helps merge small files. |
| `spark.default.parallelism` | Total cores | For RDD ops. Not used for DataFrame ops. |
| `spark.executor.memory` | 1g | Set based on workload: 4-16g typical. |
| `spark.executor.cores` | 1 | 4-5 cores per executor is a good starting point. |
| `spark.driver.memory` | 1g | Increase if Driver OOMs (collect, broadcast). |
| `spark.memory.fraction` | 0.6 | Fraction of heap for unified memory. Rarely needs changing. |
| `spark.sql.codegen.wholeStage` | `true` | Keep enabled. |

> **Citation**: [Configuration -- Spark 4.1.1 Documentation](https://spark.apache.org/docs/latest/configuration.html)
>
> **Citation**: [Tuning -- Spark 4.1.1 Documentation](https://spark.apache.org/docs/latest/tuning.html)

---

## Level 10: Cluster Management & Deployment

### 10.1 Cluster Managers

Spark itself does not manage resources. It delegates to an external cluster manager.

**YARN (Hadoop)**:
- The traditional choice for Hadoop-based deployments.
- Mature, battle-tested, supports resource queues (capacity/fair scheduler).
- Higher batch throughput in stable, large, dedicated clusters (28% faster in benchmarks for 50-200 node deployments).
- Requires the Hadoop ecosystem to be installed and maintained.

**Kubernetes**:
- The modern, cloud-native choice. Fully supported since Spark 3.1.
- Runs Spark in pods, enabling fine-grained resource allocation.
- Superior dynamic scaling: can spin up/down pods in under 30 seconds (vs. 90+ seconds for YARN).
- 40% higher cluster utilization for dynamic, ephemeral workloads.
- No external shuffle service needed (shuffle tracking via `spark.dynamicAllocation.shuffleTracking.enabled`).
- Native support for containerized dependencies (Docker images with your libraries baked in).

**Standalone**:
- Spark's built-in cluster manager. Simple to set up (no Hadoop or K8s needed).
- Good for dev/test, small clusters, or single-tenant deployments.
- Lacks advanced features like resource queues and multi-tenancy.

**Managed Services** (the most common in production):
- **AWS EMR**: Managed Hadoop/Spark on EC2. Supports both YARN and EMR on EKS (Kubernetes).
- **Databricks**: Fully managed Spark platform with additional features (Delta Lake, Unity Catalog, auto-scaling, photon engine). The easiest way to run production Spark.
- **Google Dataproc**: Managed Hadoop/Spark on GCP.
- **Azure HDInsight / Synapse**: Managed Spark on Azure.

> **Redshift analogy**: In Redshift, the cluster is always running and managed by AWS. In Spark, you choose your cluster manager and can run ephemeral clusters that start, execute your pipeline, and terminate -- paying only for the compute time used.

### 10.2 Client Mode vs Cluster Mode

**Client Mode**: The Driver runs on the machine that submits the job (e.g., your laptop or an edge node). Use for interactive development, notebooks, and debugging.

```bash
spark-submit --master yarn --deploy-mode client my_job.py
```

**Cluster Mode**: The Driver runs on one of the cluster's worker nodes. Use for production jobs -- if the submitting machine dies, the job continues.

```bash
spark-submit --master yarn --deploy-mode cluster my_job.py
```

| Aspect | Client Mode | Cluster Mode |
|---|---|---|
| Driver location | Submitting machine | Cluster worker node |
| Use case | Interactive, debugging | Production pipelines |
| Fault tolerance | Job dies if submitter dies | Job survives submitter death |
| Logs | Visible on your terminal | Stored on cluster; access via YARN/K8s |

### 10.3 Dynamic Allocation

Dynamic allocation lets Spark **add and remove executors** at runtime based on workload demands, rather than reserving a fixed number upfront.

```python
spark.conf.set("spark.dynamicAllocation.enabled", "true")
spark.conf.set("spark.dynamicAllocation.minExecutors", "2")
spark.conf.set("spark.dynamicAllocation.maxExecutors", "100")
spark.conf.set("spark.dynamicAllocation.initialExecutors", "5")

# How quickly to add executors when there are pending tasks
spark.conf.set("spark.dynamicAllocation.schedulerBacklogTimeout", "1s")

# How quickly to remove idle executors
spark.conf.set("spark.dynamicAllocation.executorIdleTimeout", "60s")

# Required for YARN (not needed for K8s with Spark 3+)
spark.conf.set("spark.dynamicAllocation.shuffleTracking.enabled", "true")
```

**How it works**:
1. When tasks are queued and no executor slots are available, Spark requests new executors from the cluster manager.
2. When an executor has been idle for `executorIdleTimeout`, Spark releases it.
3. The **External Shuffle Service** (YARN) or **Shuffle Tracking** (K8s/Spark 3+) preserves shuffle files when executors are removed, so downstream stages can still read them.

**Why this matters**: Without dynamic allocation, you must estimate peak resource needs upfront. Over-provision and you waste money. Under-provision and your jobs are slow or fail. Dynamic allocation solves this.

> **Citation**: [Job Scheduling -- Spark 4.1.1 Documentation](https://spark.apache.org/docs/latest/job-scheduling.html)
>
> **Citation**: [Dynamic Resource Allocation -- AWS EMR Best Practices](https://aws.github.io/aws-emr-containers-best-practices/performance/docs/dra/)
>
> **Citation**: [Exploring Advanced Resource Management: YARN vs Kubernetes -- MoldStud](https://moldstud.com/articles/p-exploring-advanced-resource-management-apache-spark-yarn-vs-kubernetes)

### 10.4 Resource Sizing Rules of Thumb

**Executor Cores** (`spark.executor.cores`):
- 4-5 cores per executor is ideal.
- Too few cores: underutilized executors, too many small tasks.
- Too many cores (>5): excessive GC overhead, HDFS throughput bottlenecks (each core opens concurrent connections).

**Executor Memory** (`spark.executor.memory`):
- Leave ~10% of node memory for the OS and cluster manager daemons.
- Account for memory overhead (~10% of executor memory, min 384 MB).
- Example: a node with 64 GB RAM and 16 cores → 3 executors × 5 cores × 18 GB each (leaving ~10 GB for overhead and OS).

**Number of Executors** (for static allocation):
- `num_executors = (total_cluster_cores / cores_per_executor) - 1` (reserve one core for the cluster manager daemon per node).

**Driver Memory** (`spark.driver.memory`):
- 2-4 GB for most jobs.
- Increase if you `collect()`, broadcast large tables, or process many small files (the Driver tracks metadata for all files).

---

## Level 11: Structured Streaming

While the focus of this guide is batch processing, a Senior Data Engineer should understand Spark's streaming model at a conceptual level.

### 11.1 The Streaming Model

Structured Streaming treats a **live data stream as an unbounded table** that is continuously appended. You write your query the same way you would for a batch DataFrame, and Spark takes care of incrementally executing it as new data arrives.

```python
# Batch read
df = spark.read.parquet("s3://bucket/events/")

# Streaming read -- same API, different source
stream_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("subscribe", "events")
    .load()
)

# Transformations are identical
processed = (
    stream_df
    .selectExpr("CAST(value AS STRING) as json")
    .select(from_json("json", schema).alias("data"))
    .select("data.*")
    .groupBy(window("event_time", "5 minutes"), "event_type")
    .count()
)

# Write stream
query = (
    processed.writeStream
    .outputMode("update")
    .format("delta")
    .option("checkpointLocation", "s3://bucket/checkpoints/events/")
    .start("s3://bucket/output/event_counts/")
)
```

### 11.2 Processing Modes

**Micro-Batch** (default): Processes data in small batches (e.g., every 100ms to several seconds). Provides **exactly-once** processing guarantees. Each micro-batch is a small batch job using the same Catalyst optimizer and Tungsten execution engine as batch queries. This is what you will use 95% of the time.

**Continuous Processing** (experimental): Processes each record as it arrives, achieving ~1ms end-to-end latency. Provides **at-least-once** guarantees. Use this only when sub-second latency is critical.

### 11.3 Watermarks and Late Data

In event-time processing, data can arrive late. Watermarks tell Spark how long to wait for late data before finalizing a window:

```python
# Accept data up to 10 minutes late
stream_df = stream_df.withWatermark("event_time", "10 minutes")

# Windows are kept in state until the watermark passes them
result = (
    stream_df
    .groupBy(window("event_time", "5 minutes"))
    .count()
)
```

**How watermarks work internally**:
- Spark tracks the maximum observed event time across all partitions.
- The watermark is set to `max_event_time - threshold` (e.g., 10 minutes).
- Any window that is entirely before the watermark is finalized and its state is purged.
- Any data with an event time older than the watermark is **dropped**.

### 11.4 State Management

Stateful operations (aggregations, joins, deduplication) maintain state across micro-batches. Spark stores this state in a **state store**:

- **Default**: HDFS-backed state store (in-memory hash map checkpointed to HDFS/S3).
- **RocksDB** (recommended for production): Reduces memory pressure and GC pauses by storing state in an embedded RocksDB database with disk spillover.

```python
spark.conf.set("spark.sql.streaming.stateStore.providerClass",
               "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
```

> **Redshift comparison**: Redshift has no native streaming capability. The common pattern is Lambda architecture: Kinesis → Firehose → S3 → COPY INTO Redshift. With Spark Structured Streaming, you can process data continuously without this multi-hop architecture.

> **Citation**: [Structured Streaming Programming Guide -- Spark Documentation](https://spark.apache.org/docs/latest/streaming/apis-on-dataframes-and-datasets.html)
>
> **Citation**: [Feature Deep Dive: Watermarking in Apache Spark Structured Streaming -- Databricks Blog](https://www.databricks.com/blog/feature-deep-dive-watermarking-apache-spark-structured-streaming)

---

## Appendix: Quick Reference -- Redshift Concept → Spark Equivalent

| Redshift Concept | Spark Equivalent |
|---|---|
| `CREATE TABLE ... DISTKEY(col)` | `df.repartition(n, "col")` or bucketing |
| `CREATE TABLE ... SORTKEY(col)` | Bucketing with `sortBy()`, or Delta Lake Z-Ordering |
| `VACUUM` (reclaim space, re-sort) | `OPTIMIZE` (Delta Lake), rewrite + compact Parquet files |
| `ANALYZE` (compute statistics) | `ANALYZE TABLE ... COMPUTE STATISTICS` |
| `EXPLAIN` | `df.explain(mode="formatted")` |
| Distribution styles (KEY, ALL, EVEN) | Hash partitioning, broadcast, round-robin |
| WLM queues | Fair Scheduler pools, cluster namespaces (K8s) |
| Concurrency scaling | Dynamic allocation of executors |
| Zone maps (min/max per block) | Parquet row group statistics, Delta Lake file-level stats |
| Spectrum (query S3 directly) | This IS what Spark does natively -- it reads S3/HDFS/etc. |
| Materialized views | `cache()` / `persist()`, or Delta Lake materialized views |
| `UNLOAD` (export to S3) | `df.write.parquet("s3://...")` |
| `COPY` (load from S3) | `spark.read.parquet("s3://...")` |
| Leader node | Driver |
| Compute nodes | Executors |
| Node slices | Executor cores (task slots) |

---

## Further Reading

- [Spark: The Definitive Guide (O'Reilly)](https://www.oreilly.com/library/view/spark-the-definitive/9781491912201/) -- The canonical book on Spark.
- [The Internals of Spark SQL (Jacek Laskowski)](https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/) -- The deepest open-source reference on Spark SQL internals.
- [Spark SQL, DataFrames and Datasets Guide -- Official](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [Databricks Blog -- Engineering](https://www.databricks.com/blog/category/engineering) -- Excellent deep-dive articles on Spark internals from the creators.
- [Learning Spark, 2nd Edition (O'Reilly)](https://www.oreilly.com/library/view/learning-spark-2nd/9781492050032/) -- Updated for Spark 3.x.
