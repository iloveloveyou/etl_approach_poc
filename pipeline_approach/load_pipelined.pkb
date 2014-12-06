CREATE OR REPLACE PACKAGE BODY etl_perf.load_pipelined
IS
  FUNCTION get_source_data_sql( p_parallel_degree INTEGER := 1 ) RETURN VARCHAR2 IS
    BEGIN
      RETURN '
        SELECT /*+ PARALLEL(a '||p_parallel_degree||')*/
              TO_NUMBER( NULL ) fact_id,
              batch_no,
              TO_NUMBER( NULL ) owner_id,
              a.table_name,
              a.column_name,
              TO_NUMBER( NULL ) data_type_id,
              a.blocks, DECODE( a.partitioned, ''YES'', ''Y'', ''NO'', ''N'' ),
              a.owner, a.data_type, a.partitioned, ''N'' is_bad,
              ''I'' operation_type
         FROM src_fact_tab a';
    END get_source_data_sql;

  FUNCTION get_source_data( p_parallel_degree INTEGER := 1 ) RETURN SYS_REFCURSOR IS
    result SYS_REFCURSOR;
    BEGIN
      OPEN result FOR get_source_data_sql( p_parallel_degree );
      RETURN result;
    END get_source_data;

  FUNCTION get_transformed_data( p_cursor SYS_REFCURSOR ) RETURN fact_data_t PIPELINED PARALLEL_ENABLE(PARTITION p_cursor BY ANY) IS
    result_list fact_data_t;
    TYPE varchar_t IS TABLE OF INTEGER INDEX BY VARCHAR2(128);
    TYPE dim_owner_t IS TABLE OF dim_owner_tab%ROWTYPE;
    TYPE dim_data_type_t IS TABLE OF dim_data_type_tab%ROWTYPE;
    v_dim_owner_tab     dim_owner_t;
    v_dim_data_type_tab dim_data_type_t;
    v_owner_tab         varchar_t;
    v_data_type_tab     varchar_t;
    BEGIN
      SELECT * BULK COLLECT INTO v_dim_owner_tab FROM dim_owner_tab;
      FOR i IN 1 .. v_dim_owner_tab.COUNT LOOP
        v_owner_tab( v_dim_owner_tab(i).owner ) := v_dim_owner_tab(i).owner_id;
      END LOOP;
      SELECT * BULK COLLECT INTO v_dim_data_type_tab FROM dim_data_type_tab d;
      FOR i IN 1 .. v_dim_data_type_tab.COUNT LOOP
        v_data_type_tab( v_dim_data_type_tab(i).data_type ) := v_dim_data_type_tab(i).data_type_id;
      END LOOP;
      LOOP
        FETCH p_cursor BULK COLLECT INTO result_list LIMIT C_BULK_LIMIT;
        EXIT WHEN result_list.COUNT = 0;
        FOR i IN 1 .. result_list.COUNT LOOP
          BEGIN result_list(i).table_owner_id := v_owner_tab( result_list(i).src_owner ); EXCEPTION WHEN NO_DATA_FOUND THEN result_list(i).is_bad := 'Y'; END;
          BEGIN result_list(i).data_type_id   := v_data_type_tab( result_list(i).src_data_type ); EXCEPTION WHEN NO_DATA_FOUND THEN result_list(i).is_bad := 'Y'; END;
          IF result_list(i).partitioned IS NULL OR result_list(i).blocks IS NULL THEN
            result_list(i).is_bad := 'Y';
          END IF;
          PIPE ROW( result_list(i) );
        END LOOP;
        EXIT WHEN p_cursor%NOTFOUND;
      END LOOP;
      RETURN;
    END get_transformed_data;

  PROCEDURE load( p_parallel_degree INTEGER := 1 ) IS
    BEGIN
      EXECUTE IMMEDIATE
      'INSERT /*+ PARALLEL(fact_tab '||p_parallel_degree||') PARALLEL(fact_tab_bad '||p_parallel_degree||')*/ FIRST
        WHEN is_bad = ''N'' THEN
          INTO fact_tab (fact_id, batch_number, table_owner_id, table_name, column_name, data_type_id, blocks, partitioned)
          VALUES (fact_id_seq.NEXTVAL, batch_number, table_owner_id, table_name, column_name, data_type_id, blocks, partitioned)
        WHEN is_bad = ''Y'' THEN
          INTO fact_tab_bad (fact_id, batch_number, table_owner_id, table_name, column_name, data_type_id, blocks, partitioned, src_owner, src_data_type, src_partitioned)
          VALUES (fact_id_seq.NEXTVAL, batch_number, table_owner_id, table_name, column_name, data_type_id, blocks, partitioned, src_owner, src_data_type, src_partitioned)
      SELECT /*+ PARALLEL(T '||p_parallel_degree||') */
             *
        FROM TABLE( load_pipelined.get_transformed_data( CURSOR( '||load_pipelined.get_source_data_sql( p_parallel_degree) ||') ) ) T';
    END load;

END load_pipelined;
/
