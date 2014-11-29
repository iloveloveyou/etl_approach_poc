-- Inspired by
-- http://www.oracle-developer.net/display.php?id=411

DROP PROCEDURE aq_read_source;
DROP PROCEDURE aq_load_fact;
DROP PROCEDURE aq_load_bad;
DROP FUNCTION aq_transform;

DROP TYPE fact_common_type FORCE ;
DROP TYPE fact_common_element_array_type FORCE ;
DROP TYPE fact_common_element_type FORCE ;

--CONN system/oracle;
--REVOKE EXECUTE ON DBMS_AQ FROM ETL_PERF;
--REVOKE EXECUTE ON DBMS_TRANSFORM FROM ETL_PERF;
