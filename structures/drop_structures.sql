
DROP TABLE dim_data_type_tab;
DROP TABLE dim_owner_tab;
DROP TABLE src_fact_tab;
DROP TABLE fact_tab;
DROP TABLE fact_tab_bad;
DROP SEQUENCE fact_id_seq;
DROP TABLE fact_tab_stage;

CONN SYSTEM/oracle;
DROP USER etl_perf CASCADE;
