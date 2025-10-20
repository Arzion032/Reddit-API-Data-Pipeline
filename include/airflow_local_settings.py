# include/airflow_local_settings.py
from airflow.configuration import conf
import os

# Force CeleryExecutor at runtime
os.environ["AIRFLOW__CORE__EXECUTOR"] = "CeleryExecutor"

# Optional: print to confirm
print(f"⚙️  Executor forced to {os.environ['AIRFLOW__CORE__EXECUTOR']}")
