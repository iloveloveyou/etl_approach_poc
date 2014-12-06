SET TIMING ON
SET SERVEROUTPUT ON SIZE UNLIMITED
SPOOL perf_test_aq.log
TRUNCATE TABLE fact_tab_bad DROP STORAGE;
TRUNCATE TABLE fact_tab_stage DROP STORAGE;
TRUNCATE TABLE fact_tab DROP STORAGE;
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
  exec_code('aq_read_source');

  DBMS_OUTPUT.PUT_LINE('Total took:'||(systimestamp-total_t));
  COMMIT;
END;
/
SPOOL OFF

