CREATE OR REPLACE FUNCTION get_owner_id(p_owner VARCHAR2) RETURN INTEGER RESULT_CACHE IS
  result INTEGER;
  BEGIN
    SELECT OWNER_ID
      INTO result
      FROM DIM_OWNER_TAB T WHERE T.OWNER = p_owner;
      RETURN result;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        RETURN NULL;
  END;
/
