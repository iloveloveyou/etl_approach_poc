CREATE OR REPLACE PROCEDURE aq_load_fact(
  context  RAW,
  reginfo  SYS.AQ$_REG_INFO,
  descr    SYS.AQ$_DESCRIPTOR,
  payload  RAW,
  payloadl NUMBER
)
IS
  r_dequeue_options    DBMS_AQ.DEQUEUE_OPTIONS_T;
  r_message_properties DBMS_AQ.MESSAGE_PROPERTIES_T;

  v_message_handle     RAW(16);
  o_fact_common        FACT_COMMON_ELEMENT_ARRAY_TYPE;
  buff                 ANYDATA;
  x                    PLS_INTEGER;
  BEGIN
    r_dequeue_options.msgid := descr.msg_id;
    r_dequeue_options.consumer_name := descr.consumer_name;

    DBMS_AQ.DEQUEUE(
        queue_name         => descr.queue_name,
        dequeue_options    => r_dequeue_options,
        message_properties => r_message_properties,
        payload            => buff,
        msgid              => v_message_handle
    );
    x := buff.getCollection( o_fact_common );
    INSERT INTO FACT_TAB (FACT_ID, BATCH_NUMBER, TABLE_OWNER_ID, TABLE_NAME, COLUMN_NAME, DATA_TYPE_ID, BLOCKS, PARTITIONED)
      SELECT o_fact_common.FACT_ID, o_fact_common.BATCH_NUMBER, o_fact_common.TABLE_OWNER_ID, o_fact_common.TABLE_NAME, o_fact_common.COLUMN_NAME, o_fact_common.DATA_TYPE_ID, o_fact_common.BLOCKS, o_fact_common.PARTITIONED
      FROM TABLE(o_fact_common) o_fact_common
      where o_fact_common.is_bad = 'N';
    COMMIT;
  END aq_load_fact;
/

