-- Inspired by
-- http://www.oracle-developer.net/display.php?id=411

CONN system/oracle;
GRANT EXECUTE ON DBMS_AQ TO ETL_PERF;

CONN etl_perf/etl_perf;

CREATE TYPE fact_common_type AS OBJECT(
  FACT_ID            NUMBER,
  BATCH_NUMBER       NUMBER,
  TABLE_OWNER_ID     NUMBER,
  TABLE_NAME         VARCHAR2(128 BYTE),
  COLUMN_NAME        VARCHAR2(128 BYTE),
  DATA_TYPE_ID       NUMBER,
  BLOCKS             NUMBER,
  PARTITIONED        VARCHAR2(1 CHAR),
  src_owner          VARCHAR2(128 BYTE),
  src_data_type      VARCHAR2(128 BYTE),
  src_partitioned    VARCHAR2(3 BYTE)
);
/

BEGIN
    DBMS_AQADM.CREATE_QUEUE_TABLE (
       queue_table        => 'etl_transform_table',
       queue_payload_type => 'fact_common_type',
       multiple_consumers => TRUE
       );

  DBMS_AQADM.CREATE_QUEUE_TABLE (
      queue_table        => 'etl_load_bad_table',
      queue_payload_type => 'fact_common_type',
      multiple_consumers => TRUE
  );

  DBMS_AQADM.CREATE_QUEUE_TABLE (
      queue_table        => 'etl_load_fact_table',
      queue_payload_type => 'fact_common_type',
      multiple_consumers => TRUE
  );
  DBMS_AQADM.CREATE_QUEUE (
      queue_name  => 'etl_transform_queue',
      queue_table => 'etl_transform_table'
  );
  DBMS_AQADM.START_QUEUE (
      queue_name => 'etl_transform_queue'
  );
  DBMS_AQADM.CREATE_QUEUE (
      queue_name  => 'etl_load_bad_queue',
      queue_table => 'etl_load_bad_table'
  );
  DBMS_AQADM.START_QUEUE (
      queue_name => 'etl_load_bad_queue'
  );
  DBMS_AQADM.CREATE_QUEUE (
      queue_name  => 'etl_load_fact_queue',
      queue_table => 'etl_load_fact_table'
  );
  DBMS_AQADM.START_QUEUE (
      queue_name => 'etl_load_fact_queue'
  );

END;
/


@@load_aq.pks
@@load_aq.pkb


BEGIN
  DBMS_AQADM.ADD_SUBSCRIBER (
     queue_name => 'etl_transform_queue',
     subscriber => SYS.AQ$_AGENT(
                      'etl_transform_subscriber',
                      NULL,
                      NULL ),
     delivery_mode => DBMS_AQ.BUFFERED
     );

  DBMS_AQADM.ADD_SUBSCRIBER (
      queue_name => 'etl_load_bad_queue',
      subscriber => SYS.AQ$_AGENT(
          'etl_load_bad_subscriber',
          NULL,
          NULL ),
      delivery_mode => DBMS_AQ.BUFFERED
  );

  DBMS_AQADM.ADD_SUBSCRIBER (
      queue_name => 'etl_load_fact_queue',
      subscriber => SYS.AQ$_AGENT(
          'etl_load_fact_subscriber',
          NULL,
          NULL ),
      delivery_mode => DBMS_AQ.BUFFERED
  );

END;
/

BEGIN
  DBMS_AQ.REGISTER (
      SYS.AQ$_REG_INFO_LIST(
          SYS.AQ$_REG_INFO(
              'ETL_TRANSFORM_QUEUE:ETL_TRANSFORM_SUBSCRIBER',
              DBMS_AQ.NAMESPACE_AQ,
              'plsql://ETL_PERF.LOAD_AQ.TRANSFORM',
              HEXTORAW('FF')
          )
      ),
      1
  );

  DBMS_AQ.REGISTER (
      SYS.AQ$_REG_INFO_LIST(
          SYS.AQ$_REG_INFO(
              'ETL_LOAD_BAD_QUEUE:ETL_LOAD_BAD_SUBSCRIBER',
              DBMS_AQ.NAMESPACE_AQ,
              'plsql://ETL_PERF.LOAD_AQ.LOAD_BAD',
              HEXTORAW('FF')
          )
      ),
      1
  );

  DBMS_AQ.REGISTER (
      SYS.AQ$_REG_INFO_LIST(
          SYS.AQ$_REG_INFO(
              'ETL_LOAD_FACT_QUEUE:ETL_LOAD_FACT_SUBSCRIBER',
              DBMS_AQ.NAMESPACE_AQ,
              'plsql://ETL_PERF.LOAD_AQ.LOAD_FACT',
              HEXTORAW('FF')
          )
      ),
      1
  );
END;
/

EXEC DBMS_AQADM.SCHEDULE_PROPAGATION('etl_transform_queue', LATENCY=>1);
EXEC DBMS_AQADM.SCHEDULE_PROPAGATION('etl_load_fact_queue', LATENCY=>1);
EXEC DBMS_AQADM.SCHEDULE_PROPAGATION('etl_load_bad_queue', LATENCY=>1);