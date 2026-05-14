FROM apache/airflow:2.9.3

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
        "dbt-core==1.8.7" \
        "dbt-snowflake==1.8.4" \
        "pandera==0.20.4" \
        "astronomer-cosmos==1.8.0 "\
        "scikit-learn>=1.4.0 "\
        "joblib>=1.4.0 "
