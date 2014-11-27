CREATE OR REPLACE PACKAGE BODY etl_perf.load_pipelined
IS
  FUNCTION get_source_data(parallel_degree_p INTEGER := 1) RETURN SYS_REFCURSOR IS
    result SYS_REFCURSOR;
    BEGIN

      OPEN result FOR '
        SELECT /*+ PARALLEL(a '||parallel_degree_p||')*/ FACT_ID_SEQ.nextval , BATCH_NO,
              TO_NUMBER(NULL) OWNER_ID, a.TABLE_NAME, a.COLUMN_NAME,
              TO_NUMBER(NULL) DATA_TYPE_ID,
              a.BLOCKS, DECODE(a.PARTITIONED, ''YES'', ''Y'', ''NO'', ''N'' ),
              a.OWNER, a.DATA_TYPE, a.PARTITIONED, ''N'' IS_BAD,
              ''I'' OPERATION_TYPE
         FROM SRC_FACT_TAB a';
      RETURN result;
    END get_source_data;

  FUNCTION get_transformed_data(cursor_p SYS_REFCURSOR ) RETURN fact_data_t PIPELINED IS
    result_row fact_data_row_t;
    TYPE varchar_t IS TABLE OF INTEGER INDEX BY VARCHAR2(128);
    owner_tab varchar_t;
    data_type_tab varchar_t;
    BEGIN
      FOR i IN (SELECT OWNER_ID, OWNER FROM DIM_OWNER_TAB) LOOP
        owner_tab(I.OWNER) := I.OWNER_ID;
      END LOOP;
      FOR i IN (SELECT d.DATA_TYPE_ID, DATA_TYPE FROM DIM_DATA_TYPE_TAB d) LOOP
        data_type_tab(I.DATA_TYPE) := I.DATA_TYPE_ID;
      END LOOP;
      LOOP
        FETCH cursor_p INTO result_row;
        EXIT WHEN cursor_p%NOTFOUND;
        BEGIN result_row.table_owner_id := owner_tab( result_row.src_owner ); EXCEPTION WHEN NO_DATA_FOUND THEN NULL; END;
        BEGIN result_row.data_type_id   := data_type_tab( result_row.src_data_type ); EXCEPTION WHEN NO_DATA_FOUND THEN NULL; END;
        IF result_row.TABLE_OWNER_ID IS NULL OR result_row.DATA_TYPE_ID IS NULL OR result_row.PARTITIONED IS NULL OR result_row.BLOCKS IS NULL THEN
          result_row.is_bad := 'Y';
        END IF;
        PIPE ROW (result_row);
      END LOOP;
      return;
    END get_transformed_data;

  PROCEDURE load IS
    BEGIN
      INSERT ALL
        WHEN is_bad = 'N' THEN
          INTO FACT_TAB (FACT_ID, BATCH_NUMBER, TABLE_OWNER_ID, TABLE_NAME, COLUMN_NAME, DATA_TYPE_ID, BLOCKS, PARTITIONED)
          VALUES (FACT_ID, BATCH_NUMBER, TABLE_OWNER_ID, TABLE_NAME, COLUMN_NAME, DATA_TYPE_ID, BLOCKS, PARTITIONED)
        WHEN is_bad = 'Y' THEN
          INTO fact_tab_bad (FACT_ID, BATCH_NUMBER, TABLE_OWNER_ID, TABLE_NAME, COLUMN_NAME, DATA_TYPE_ID, BLOCKS, PARTITIONED, SRC_OWNER, SRC_DATA_TYPE, SRC_PARTITIONED)
          VALUES (FACT_ID, BATCH_NUMBER, TABLE_OWNER_ID, TABLE_NAME, COLUMN_NAME, DATA_TYPE_ID, BLOCKS, PARTITIONED, SRC_OWNER, SRC_DATA_TYPE, SRC_PARTITIONED)
      SELECT * FROM TABLE( load_pipelined.get_transformed_data( load_pipelined.get_source_data ) );
    END load;

END load_pipelined;
/
