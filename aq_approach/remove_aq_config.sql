-- Inspired by
-- http://www.oracle-developer.net/display.php?id=411
BEGIN
  DBMS_AQ.UNREGISTER (
      SYS.AQ$_REG_INFO_LIST(
          SYS.AQ$_REG_INFO(
              'ETL_LOAD_QUEUE:ETL_LOAD_FACT_SUBSCRIBER',
              DBMS_AQ.NAMESPACE_AQ,
              'plsql://ETL_PERF.AQ_LOAD_FACT',
              HEXTORAW('FF')
          ),
          SYS.AQ$_REG_INFO(
              'ETL_LOAD_QUEUE:ETL_LOAD_BAD_SUBSCRIBER',
              DBMS_AQ.NAMESPACE_AQ,
              'plsql://ETL_PERF.AQ_LOAD_BAD',
              HEXTORAW('FF')
          )
      )
      ,2
  );
END;
/

BEGIN
  DBMS_AQADM.REMOVE_SUBSCRIBER (
      queue_name => 'etl_load_queue',
      subscriber => SYS.AQ$_AGENT(
          'etl_load_bad_subscriber',
          NULL,
          NULL )
  );

  DBMS_AQADM.REMOVE_SUBSCRIBER (
      queue_name => 'etl_load_queue',
      subscriber => SYS.AQ$_AGENT(
          'etl_load_fact_subscriber',
          NULL,
          NULL )
  );
END;
/

BEGIN
  DBMS_AQADM.STOP_QUEUE(queue_name=>'etl_load_queue');
  DBMS_AQADM.DROP_QUEUE(queue_name=>'etl_load_queue');
  DBMS_AQADM.DROP_QUEUE_TABLE(queue_table=> 'etl_load_table');
END;
/

BEGIN
  DBMS_TRANSFORM.DROP_TRANSFORMATION(
      schema =>         'ETL_PERF',
      name   =>         'AQ_TRANSFORM');
END;
/
