CREATE OR REPLACE FUNCTION aq_transform( buffer ANYDATA ) RETURN ANYDATA IS
  result FACT_COMMON_ELEMENT_ARRAY_TYPE;
  x      PLS_INTEGER;
  BEGIN
    x := buffer.GetCollection( result );
    FOR i IN 1 .. result.COUNT LOOP
      result(i).is_bad := 'N';
      result(i).table_owner_id := get_owner_id( result(i).src_owner );
      result(i).data_type_id   := get_data_type_id( result(i).src_data_type );
      result(i).partitioned := CASE result(i).src_partitioned WHEN 'YES' THEN 'Y' WHEN 'NO' THEN 'N' END;
      IF result(i).table_owner_id IS NULL OR result(i).DATA_TYPE_ID IS NULL OR result(i).PARTITIONED IS NULL OR result(i).BLOCKS IS NULL THEN
        result(i).is_bad := 'Y';
      END IF;
    END LOOP;
    RETURN ANYDATA.ConvertCollection( result );
  END aq_transform;
/

