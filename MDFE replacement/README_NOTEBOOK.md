How to use the Synapse notebook scaffold

Files created:
- `notebook_cells.md`: ordered cells to paste into a Synapse Spark Python notebook.

Quick steps:
1. Open Azure Synapse Studio and create a new Spark Python notebook.
2. Copy each code block from `notebook_cells.md` into a separate notebook cell and run in order.
3. Replace placeholders:
   - Storage paths: `abfss://<container>@<account>.dfs.core.windows.net/...`
   - JDBC connection details: `jdbc_url`, `user`, `password` (or configure managed identity usage)
   - `mdfe_table_name` or `mdfe_path` where MDFE writes its outputs
   - `diag_path` for diagnostic outputs and validation reports
4. If your environment uses `dbutils` instead of `mssparkutils`, adjust the FS helper.
5. Run Cell 4 (`df_my.explain(extended=True)`) interactively if you need to inspect physical plans.
6. If validation fails, retrieve diagnostics from `diag_path` for triage.

Security notes:
- Do not commit production credentials into notebooks. Use Synapse secrets or Key Vault (linked to Synapse) and access secrets inside the notebook.
- Prefer managed identity or service principal for JDBC authentication where possible.

Next steps I can help with:
- Tailor the scaffold to an example properties file you paste (sanitized).
- Add a cell implementing an upsert/merge to Azure SQL using a staging table and MERGE statement.
- Add a small unit-test harness to run the notebook end-to-end on a subset of data.
