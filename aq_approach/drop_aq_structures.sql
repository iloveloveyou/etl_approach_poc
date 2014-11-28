-- Inspired by
-- http://www.oracle-developer.net/display.php?id=411
-- EXEC DBMS_AQADM.UNSCHEDULE_PROPAGATION('etl_transform_queue');
-- EXEC DBMS_AQADM.UNSCHEDULE_PROPAGATION('etl_load_fact_queue');
-- EXEC DBMS_AQADM.UNSCHEDULE_PROPAGATION('etl_load_bad_queue');
BEGIN
  DBMS_AQ.UNREGISTER (
      SYS.AQ$_REG_INFO_LIST(
          SYS.AQ$_REG_INFO(
              'ETL_TRANSFORM_QUEUE:ETL_TRANSFORM_SUBSCRIBER',
              DBMS_AQ.NAMESPACE_AQ,
              'plsql://ETL_PERF.AQ_TRANSFORM',
              HEXTORAW('FF')
          )
      ),
      1
  );

  DBMS_AQ.UNREGISTER (
      SYS.AQ$_REG_INFO_LIST(
          SYS.AQ$_REG_INFO(
              'ETL_LOAD_BAD_QUEUE:ETL_LOAD_BAD_SUBSCRIBER',
              DBMS_AQ.NAMESPACE_AQ,
              'plsql://ETL_PERF.AQ_LOAD_BAD',
              HEXTORAW('FF')
          )
      ),
      1
  );

  DBMS_AQ.UNREGISTER (
      SYS.AQ$_REG_INFO_LIST(
          SYS.AQ$_REG_INFO(
              'ETL_LOAD_FACT_QUEUE:ETL_LOAD_FACT_SUBSCRIBER',
              DBMS_AQ.NAMESPACE_AQ,
              'plsql://ETL_PERF.AQ_LOAD_FACT',
              HEXTORAW('FF')
          )
      ),
      1
  );
END;
/

BEGIN
  DBMS_AQADM.REMOVE_SUBSCRIBER (
      queue_name => 'etl_transform_queue',
      subscriber => SYS.AQ$_AGENT(
          'etl_transform_subscriber',
          NULL,
          NULL )
  );

  DBMS_AQADM.REMOVE_SUBSCRIBER (
      queue_name => 'etl_load_bad_queue',
      subscriber => SYS.AQ$_AGENT(
          'etl_load_bad_subscriber',
          NULL,
          NULL )
  );

  DBMS_AQADM.REMOVE_SUBSCRIBER (
      queue_name => 'etl_load_fact_queue',
      subscriber => SYS.AQ$_AGENT(
          'etl_load_fact_subscriber',
          NULL,
          NULL )
  );
END;
/

BEGIN
  DBMS_AQADM.STOP_QUEUE(queue_name=>'etl_transform_queue');
  DBMS_AQADM.DROP_QUEUE(queue_name=>'etl_transform_queue');
  DBMS_AQADM.DROP_QUEUE_TABLE(queue_table=> 'etl_transform_table');
  DBMS_AQADM.STOP_QUEUE(queue_name=>'etl_load_fact_queue');
  DBMS_AQADM.DROP_QUEUE(queue_name=>'etl_load_fact_queue');
  DBMS_AQADM.DROP_QUEUE_TABLE(queue_table=> 'etl_load_fact_table');
  DBMS_AQADM.STOP_QUEUE(queue_name=>'etl_load_bad_queue');
  DBMS_AQADM.DROP_QUEUE(queue_name=>'etl_load_bad_queue');
  DBMS_AQADM.DROP_QUEUE_TABLE(queue_table=> 'etl_load_bad_table');

END;
/

DROP TYPE fact_common_type;

DROP PACKAGE load_aq;


--CONN system/oracle;
--REVOKE EXECUTE ON DBMS_AQ FROM ETL_PERF;

