CREATE OR REPLACE PACKAGE BODY etl_perf.load_classic
IS
  PROCEDURE load_bad( p_parallel_degree INTEGER := 1 )
  IS
  BEGIN
    EXECUTE IMMEDIATE 'INSERT /*+ PARALLEL(fact_tab_bad '||p_parallel_degree||') */
    INTO fact_tab_bad
      (fact_id, batch_number, table_owner_id, table_name, column_name, data_type_id, blocks, partitioned,
       src_owner, src_data_type, src_partitioned)
      SELECT /*+ PARALLEL(fact_tab_stage '||p_parallel_degree||') */ fact_id, batch_number, table_owner_id, table_name, column_name, data_type_id, blocks, partitioned,
             src_owner, src_data_type, src_partitioned
        FROM fact_tab_stage s
      where s.is_bad=''Y''';
  END load_bad;

  PROCEDURE load_stage( p_parallel_degree INTEGER := 1 )
  IS
    BEGIN
      EXECUTE IMMEDIATE 'TRUNCATE TABLE FACT_TAB_STAGE';
      EXECUTE IMMEDIATE 'INSERT /*+ PARALLEL(fact_tab_stage '||p_parallel_degree||') */
      INTO fact_tab_stage
      (fact_id, batch_number, table_owner_id, table_name, column_name, data_type_id, blocks, partitioned,
       src_owner, src_data_type, src_partitioned, is_bad, operation_type)
        SELECT  /*+ PARALLEL(src_fact_tab '||p_parallel_degree||') */fact_id_seq.nextval, batch_no, o.owner_id, a.table_name, a.column_name, d.data_type_id, a.blocks, DECODE(a.partitioned, ''YES'', ''Y'', ''NO'', ''N'' ),
          a.owner, a.data_type, a.partitioned,
          CASE
          WHEN o.owner_id IS NULL OR d.data_type_id IS NULL OR DECODE(a.partitioned, ''YES'', ''Y'', ''NO'', ''N'' ) IS NULL OR a.blocks IS NULL
          THEN ''Y''
          ELSE ''N''
          END,
          ''I''
        FROM src_fact_tab a
          LEFT JOIN dim_owner_tab o ON o.owner = a.owner
          LEFT JOIN dim_data_type_tab d ON d.data_type = a.data_type';
    END load_stage;

  PROCEDURE load_fact( p_parallel_degree INTEGER := 1 )
  IS
    BEGIN
      EXECUTE IMMEDIATE 'INSERT  /*+ PARALLEL(fact_tab '||p_parallel_degree||') */ INTO fact_tab
      (fact_id, batch_number, table_owner_id, table_name, column_name, data_type_id, blocks, partitioned)
      SELECT  /*+ PARALLEL(fact_tab_stage '||p_parallel_degree||') */fact_id, batch_number, table_owner_id, table_name, column_name, data_type_id, blocks, partitioned
        FROM fact_tab_stage
          WHERE is_bad = ''N''';
    END load_fact;

END load_classic;
/
