Notebook cells for replacing MDFE.jar in Azure Synapse

Instructions: Paste each code block into a separate cell in a Synapse Spark Python notebook and run in order. Replace placeholder values (storage paths, JDBC URL, secrets) with your environment's values. Use `mssparkutils` on Synapse or `dbutils` on Databricks as appropriate.

---

# Cell 1 — Imports and configuration helpers
```python
# Imports and small helpers
from pyspark.sql import functions as F
from pyspark.sql import types as T
import json
import os

# Use mssparkutils on Synapse. If not present, fall back to dbutils where available.
try:
    from mssparkutils import fs as mssparkfs
    read_file = lambda path: mssparkfs.head(path, 10_000_000)
except Exception:
    try:
        read_file = lambda path: dbutils.fs.head(path)
    except Exception:
        read_file = None

# Generic normalization helper
def normalize_df(df, float_precision=6, drop_columns=None, column_casts=None, trim_strings=True):
    drop_columns = drop_columns or []
    column_casts = column_casts or {}
    cols = []
    for c, t in df.dtypes:
        if c in drop_columns:
            continue
        col = F.col(c)
        if c in column_casts:
            col = col.cast(column_casts[c])
        elif t.startswith('decimal') or t.startswith('double') or t.startswith('float'):
            # round floats to given precision
            col = F.round(col, float_precision)
        elif t.startswith('int') or t.startswith('bigint'):
            col = col.cast('long')
        elif t.startswith('timestamp') or t.startswith('date'):
            col = F.col(c).cast('string')
        else:
            # leave string-like types
            pass
        if trim_strings and t == 'string':
            col = F.trim(F.coalesce(col, F.lit('')))
        else:
            col = F.coalesce(col.cast('string'), F.lit(''))
        cols.append(col.alias(c))
    return df.select(*cols)

# Deterministic row-hash
def add_row_hash(df, cols):
    return df.withColumn('_hash', F.sha2(F.concat_ws('||', *cols), 256))
```

---

# Cell 1.5 — Set MDFE-observed Spark configs (match runtime behavior)
```python
# These are observed in MDFE execution logs. Setting them helps reproduce MDFE behavior.
configs_to_set = {
    'spark.sql.parquet.fieldId.read.enabled': 'true',
    'spark.sql.parquet.fieldId.write.enabled': 'true',
    'spark.synapse.vegas.useCache': 'false'
}

for k, v in configs_to_set.items():
    try:
        spark.conf.set(k, v)
        print(f'Set {k} = {v}')
    except Exception as e:
        print(f'Could not set {k}:', e)

# Caution: enabling `fieldId.write` changes Parquet metadata; avoid enabling on production outputs
# until validated. For validation runs, set both read/write to match MDFE's observed behavior.
```

---

# Cell 2 — Read properties file (SQL) from blob
```python
# Replace with your properties path
properties_path = 'abfss://<container>@<account>.dfs.core.windows.net/properties/my_properties.sql'

if read_file is None:
    raise Exception('No filesystem helper available. Use Synapse mssparkutils or Databricks dbutils in the notebook.')

raw = read_file(properties_path)
# If the file contains only SQL, take it as-is; if key=value style, detect and extract `sql=` key
sql = raw.strip()
if '=' in sql and '\n' in sql and sql.lower().startswith('sql'):
    # naive parse for simple properties files
    lines = [l.strip() for l in sql.splitlines() if l.strip()]
    props = {}
    for l in lines:
        if '=' in l:
            k, v = l.split('=', 1)
            props[k.strip()] = v.strip()
    # common key names
    sql = props.get('sql') or props.get('query') or sql

print('SQL snippet (first 500 chars):')
print(sql[:500])
```

---

# Cell 3 — Execute the SQL to produce a DataFrame (default: Spark SQL)
```python
# If the SQL references catalog tables available to Spark, use spark.sql
df_my = spark.sql(sql)

# Optionally, if the SQL is meant to run on an external RDBMS and return results via JDBC, use this pattern instead:
# jdbc_url = 'jdbc:sqlserver://<server>.database.windows.net:1433;database=<db>'
# df_my = spark.read.format('jdbc').option('url', jdbc_url).option('dbtable', f'({sql}) as src').option('user', '<user>').option('password', '<pw>').load()

print('Produced DataFrame rows:', df_my.count())
```

---

# Cell 4 — Capture Spark configs and plan (diagnostics)
```python
conf = dict(spark.sparkContext.getConf().getAll())
diag_path = 'abfss://<container>@<account>.dfs.core.windows.net/diagnostics'

# Save some key config values to a small JSON file
interesting = {k: conf.get(k) for k in ['spark.sql.shuffle.partitions', 'spark.sql.autoBroadcastJoinThreshold', 'spark.default.parallelism', 'spark.sql.adaptive.enabled']}
print('Interesting spark configs:', interesting)

# Save to diagnostics path (best-effort - replace with your preferred write method)
try:
    spark.createDataFrame([(json.dumps(interesting),)], ['json']).write.mode('overwrite').text(f'{diag_path}/spark_configs.json')
except Exception as e:
    print('Could not write spark config file:', e)

# Save explain plan to a file
plan = []
try:
    buf = []
    df_my._jdf.queryExecution().toString()  # might throw in some runtimes, but attempt
    plan_text = df_my._sc._jvm.org.apache.spark.sql.execution.debug.package$.apply(df_my._jdf.queryExecution()).toString()
    open('/tmp/df_plan.txt','w').write(plan_text)
except Exception:
    # fallback: print explain to output
    print('Could not capture full programmatic plan; please run df_my.explain(extended=True) interactively')

print('Run df_my.explain(extended=True) to inspect physical plan.')
```

---

# Cell 5 — Optional: Disable broadcast joins (to test determinism)
```python
# Temporarily disable broadcast joins and re-run the SQL (if you want to test join behavior)
spark.conf.set('spark.sql.autoBroadcastJoinThreshold', -1)
# Recompute the DF (re-run query or recreate df_my)
df_no_broadcast = spark.sql(sql)
print('Rows with broadcast disabled:', df_no_broadcast.count())
# Reset later as needed
# spark.conf.unset('spark.sql.autoBroadcastJoinThreshold')
```

---

# Cell 6 — Normalization & deterministic hashing
```python
# Define normalization parameters
drop_columns = ['processing_ts','loaded_at']  # adjust per your metadata columns
float_precision = 6

# Choose the intersecting columns between MDFe and yours during validation; for now normalize df_my only.
cols = sorted(df_my.columns)

norm_my = normalize_df(df_my, float_precision=float_precision, drop_columns=drop_columns)
norm_my = add_row_hash(norm_my, cols)

# Persist normalized set (optional)
norm_my.write.mode('overwrite').parquet(f'{diag_path}/norm_my')
print('Normalized rows:', norm_my.count())
```

---

# Cell 7 — Load MDFE's output for comparison (two input options)
```python
# Option 1: If MDFE wrote a table accessible to Spark catalog
mdfe_table_name = '<mdfe_output_table_name>'
try:
    df_mdfe = spark.table(mdfe_table_name)
except Exception:
    # Option 2: load MDFE output from a known path (parquet/csv)
    mdfe_path = 'abfss://<container>@<account>.dfs.core.windows.net/mdfe_outputs/<file>.parquet'
    df_mdfe = spark.read.parquet(mdfe_path)

print('MDFe rows:', df_mdfe.count())

# Normalize MDFE df similarly to ensure consistent comparison
cols = sorted(list(set(df_mdfe.columns).intersection(set(df_my.columns))))
norm_mdfe = normalize_df(df_mdfe.select(*cols), float_precision=float_precision, drop_columns=drop_columns)
norm_mdfe = add_row_hash(norm_mdfe, cols)
```

---

# Cell 8 — Hash-based multiset comparison and diagnostics
```python
mdfe_hash_counts = norm_mdfe.groupBy('_hash').count().withColumnRenamed('count', 'cnt_mdfe')
my_hash_counts = norm_my.groupBy('_hash').count().withColumnRenamed('count', 'cnt_my')

join_hash = mdfe_hash_counts.join(my_hash_counts, on='_hash', how='fullouter').na.fill(0)
diffs = join_hash.where(F.col('cnt_mdfe') != F.col('cnt_my'))

print('Distinct differing hashes:', diffs.count())

diffs_sample = diffs.limit(200)
diffs_sample.write.mode('overwrite').parquet(f'{diag_path}/hash_diffs')

# If small, collect sample hashes and show sample rows
sample_hashes = [r['_hash'] for r in diffs_sample.select('_hash').collect()]
if sample_hashes:
    mdfe_samples = norm_mdfe.where(F.col('_hash').isin(sample_hashes)).limit(200).toPandas()
    my_samples = norm_my.where(F.col('_hash').isin(sample_hashes)).limit(200).toPandas()
    display(mdfe_samples.head(50))
    display(my_samples.head(50))
    # Write CSV diagnostics for offline inspection
    pd_mdfe = mdfe_samples
    pd_my = my_samples
    pd_mdfe.to_csv('/tmp/mdfe_samples.csv', index=False)
    pd_my.to_csv('/tmp/my_samples.csv', index=False)
    try:
        spark.createDataFrame(pd_mdfe).write.mode('overwrite').csv(f'{diag_path}/mdfe_samples.csv')
        spark.createDataFrame(pd_my).write.mode('overwrite').csv(f'{diag_path}/my_samples.csv')
    except Exception as e:
        print('Could not write Pandas diagnostics back to storage:', e)
```

---

# Cell 9 — Deterministic write to Azure SQL (overwrite or upsert)
```python
# JDBC write example (overwrite). Replace placeholders with secrets or use managed identity/token approach.
jdbc_url = 'jdbc:sqlserver://<server>.database.windows.net:1433;database=<db>'
properties = {
    'user': '<username>',
    'password': '<password>',
    'driver': 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
}

target_table = '<schema>.<table>'

# Overwrite table (careful; validate before running in prod)
norm_my.drop('_hash').write.mode('overwrite').jdbc(url=jdbc_url, table=target_table, properties=properties)

# For upserts, implement staging table + MERGE in Azure SQL using JDBC to execute the MERGE statement, or use pyodbc on driver node.
```

---

# Cell 10 — Final validation report and exit code
```python
report = {
    'rows_my': df_my.count(),
    'rows_mdfe': df_mdfe.count(),
    'distinct_hash_diffs': diffs.count(),
}
print(report)
# Write a small JSON report
try:
    spark.createDataFrame([(json.dumps(report),)], ['json']).write.mode('overwrite').text(f'{diag_path}/validation_report.json')
except Exception as e:
    print('Could not write validation report:', e)

if report['distinct_hash_diffs'] == 0:
    print('VALIDATION PASS')
else:
    print('VALIDATION FAIL — see diagnostics at', diag_path)
    # Optionally raise exception to fail pipeline
    # raise Exception('Validation failed: differences found')
```
