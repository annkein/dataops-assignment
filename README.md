# DataOps assignment

## Execution flow
1. Activate Python environment:
   ```bash
   source venv/Scripts/activate
2. Ingest batches incrementally
   ```bash
   dvc add data/bronze/batch_1.csv
   dvc add data/bronze/batch_2.csv
   ...
   dvc add data/bronze/batch_5.csv
3. Transform bronze -> silver:
   ```bash
   python scripts/bronze_to_silver.py
   dvc add data/silver/climate_silver.csv
4. Transform silver -> gold
   ```bash
   python scripts/silver_to_gold.py
   dvc add data/gold/climate_gold.csv
   git commit -m "Create gold dataset from silver dataset"

## Assumptions
- Data batches are time-ordered and incremental.
- Bronze to silver transformations include data quality checks (continuity, duplicates, expected ranges).
