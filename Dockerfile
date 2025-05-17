FROM quay.io/astronomer/astro-runtime:8.0.0

RUN pip install --no-cache-dir \
    dbt-duckdb \
    duckdb \
    pandas \
    openpyxl \
    confluent-kafka

COPY include/.dbt /home/astro/.dbt
ENV PATH="/home/astro/.local/bin:$PATH"

