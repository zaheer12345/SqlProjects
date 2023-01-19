FROM quay.io/astronomer/astro-runtime:5.0.5

ENV AIRFLOW_VAR_MY_DAG_PARTNER='{"name":"partner_a","api_secret":"mysecret","path":"/tmp/partner_a"}'