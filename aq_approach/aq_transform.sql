CREATE OR REPLACE FUNCTION aq_transform( buffer ANYDATA ) RETURN ANYDATA IS
  result FACT_COMMON_ELEMENT_ARRAY_TYPE;
  x      PLS_INTEGER;
  TYPE varchar_t IS TABLE OF INTEGER INDEX BY VARCHAR2(128);
  owner_tab varchar_t;
  data_type_tab varchar_t;
  BEGIN
    FOR i IN (SELECT /*+ result_cache */ OWNER_ID, OWNER FROM DIM_OWNER_TAB) LOOP
      owner_tab(I.OWNER) := I.OWNER_ID;
    END LOOP;
    FOR i IN (SELECT /*+ result_cache */ d.DATA_TYPE_ID, DATA_TYPE FROM DIM_DATA_TYPE_TAB d) LOOP
      data_type_tab(I.DATA_TYPE) := I.DATA_TYPE_ID;
    END LOOP;
    x := buffer.GetCollection( result );
    FOR i IN 1 .. result.COUNT LOOP
      result(i).is_bad := 'N';
      BEGIN result(i).table_owner_id := owner_tab( result(i).src_owner ); EXCEPTION WHEN NO_DATA_FOUND THEN NULL; END;
      BEGIN result(i).data_type_id   := data_type_tab( result(i).src_data_type ); EXCEPTION WHEN NO_DATA_FOUND THEN NULL; END;
      result(i).partitioned := CASE result(i).src_partitioned WHEN 'YES' THEN 'Y' WHEN 'NO' THEN 'N' END;
      IF result(i).table_owner_id IS NULL OR result(i).DATA_TYPE_ID IS NULL OR result(i).PARTITIONED IS NULL OR result(i).BLOCKS IS NULL THEN
        result(i).is_bad := 'Y';
      END IF;
    END LOOP;
    RETURN ANYDATA.ConvertCollection( result );
  END aq_transform;
/

