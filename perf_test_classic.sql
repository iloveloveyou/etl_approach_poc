DECLARE
  total_t TIMESTAMP := systimestamp;
  PROCEDURE exec_code(code VARCHAR2) IS
    t TIMESTAMP := systimestamp;
    BEGIN
      EXECUTE IMMEDIATE 'BEGIN '||code||'; END;';
      DBMS_OUTPUT.PUT_LINE('Took: ' || (systimestamp-t) || ' to execute ' || code);
      COMMIT;
    END exec_code;
BEGIN
  exec_code('load_classic.load_stage');

  exec_code('load_classic.load_bad');

  exec_code('load_classic.load_fact');

  DBMS_OUTPUT.PUT_LINE('Total took:'||(systimestamp-total_t));
END;
/
