-- Inspired by
-- http://www.oracle-developer.net/display.php?id=411

-- CONN system/oracle as sysdba;
-- GRANT EXECUTE ON DBMS_AQ TO ETL_PERF;
-- GRANT EXECUTE ON DBMS_TRANSFORM TO ETL_PERF;
--
-- CONN etl_perf/etl_perf;

BEGIN
  DBMS_AQADM.CREATE_QUEUE_TABLE( queue_table => 'etl_load_table', queue_payload_type => 'SYS.ANYDATA', multiple_consumers => TRUE );
  DBMS_AQADM.CREATE_QUEUE ( queue_name  => 'etl_load_queue', queue_table => 'etl_load_table' );
  DBMS_AQADM.START_QUEUE ( queue_name => 'etl_load_queue' );
END;
/

BEGIN
  DBMS_AQADM.ADD_SUBSCRIBER (
      queue_name => 'etl_load_queue',
      subscriber => SYS.AQ$_AGENT(
          'etl_load_bad_subscriber',
          NULL,
          NULL )
--      , rule => 'tab.user_data.is_bad = ''Y'''
      , delivery_mode => DBMS_AQADM.PERSISTENT_OR_BUFFERED
  );

  DBMS_AQADM.ADD_SUBSCRIBER (
      queue_name => 'etl_load_queue',
      subscriber => SYS.AQ$_AGENT(
          'etl_load_fact_subscriber',
          NULL,
          NULL )
--      , rule => ' tab.user_data.is_bad = ''N'''
      , delivery_mode => DBMS_AQADM.PERSISTENT_OR_BUFFERED
  );

END;
/

BEGIN
  DBMS_TRANSFORM.CREATE_TRANSFORMATION(
      schema =>         'ETL_PERF',
      name   =>         'AQ_TRANSFORM',
      from_schema =>    'SYS',
      from_type =>      'ANYDATA',
      to_schema =>      'SYS',
      to_type =>        'ANYDATA',
      transformation => 'aq_transform( source.user_data )');
END;
/

BEGIN
  DBMS_AQ.REGISTER (
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
