CREATE OR REPLACE PACKAGE BODY etl_perf.load_aq
AS
  PROCEDURE read_source
  IS
    r_enqueue_options    DBMS_AQ.ENQUEUE_OPTIONS_T;
    r_message_properties DBMS_AQ.MESSAGE_PROPERTIES_T;

    v_message_handle     RAW(16);
    TYPE fact_common_type_lst IS TABLE OF fact_common_type;
    o_fact_common_lst fact_common_type_lst;
    crsr SYS_REFCURSOR;
    BEGIN
      r_enqueue_options.visibility := DBMS_AQ.IMMEDIATE;
      r_enqueue_options.delivery_mode := DBMS_AQ.BUFFERED;
      OPEN crsr FOR
        SELECT fact_common_type(
                fact_id_seq.nextval,
                batch_no,
                null,
                table_name,
                column_name,
                null,
                blocks,
                null,
                owner,
                data_type,
                partitioned)
          FROM src_fact_tab;
      LOOP
        FETCH crsr BULK COLLECT INTO o_fact_common_lst limit 10000;

        FOR i IN 1 .. o_fact_common_lst.COUNT LOOP
          DBMS_AQ.ENQUEUE(
              queue_name         => 'etl_transform_queue',
              enqueue_options    => r_enqueue_options,
              message_properties => r_message_properties,
              payload            => o_fact_common_lst(i),
              msgid              => v_message_handle
          );
        END LOOP;

        EXIT WHEN crsr%NOTFOUND;
      END LOOP;
      CLOSE crsr;
      COMMIT;
    END read_source;

  PROCEDURE transform(
    context  RAW,
    reginfo  SYS.AQ$_REG_INFO,
    descr    SYS.AQ$_DESCRIPTOR,
    payload  RAW,
    payloadl NUMBER
  )
  IS
    r_dequeue_options    DBMS_AQ.DEQUEUE_OPTIONS_T;
    r_enqueue_options    DBMS_AQ.ENQUEUE_OPTIONS_T;

    ---TODO check if it can be common for dequeue and enqueue
    r_message_properties DBMS_AQ.MESSAGE_PROPERTIES_T;

    v_message_handle     RAW(16);
    o_fact_common        fact_common_type;

    dest_queue_name VARCHAR2(100);
  BEGIN
    r_dequeue_options.msgid := descr.msg_id;
    r_dequeue_options.consumer_name := descr.consumer_name;
    r_dequeue_options.visibility    := DBMS_AQ.IMMEDIATE;
    r_dequeue_options.delivery_mode := DBMS_AQ.BUFFERED;
    r_enqueue_options.visibility    := DBMS_AQ.IMMEDIATE;
    r_enqueue_options.delivery_mode := DBMS_AQ.BUFFERED;

    DBMS_AQ.DEQUEUE(
       queue_name         => descr.queue_name,
       dequeue_options    => r_dequeue_options,
       message_properties => r_message_properties,
       payload            => o_fact_common,
       msgid              => v_message_handle
       );

    SELECT /*+ RESULT_CACHE */ a.owner_id
    INTO o_fact_common.table_owner_id
    FROM dim_owner_tab a
    WHERE a.owner = o_fact_common.src_owner;

    SELECT /*+ RESULT_CACHE */ data_type_id
    INTO o_fact_common.data_type_id
    FROM dim_data_type_tab
    WHERE data_type = o_fact_common.src_data_type;

    IF o_fact_common.table_owner_id IS NULL OR o_fact_common.DATA_TYPE_ID IS NULL OR o_fact_common.PARTITIONED IS NULL OR o_fact_common.BLOCKS IS NULL THEN
      dest_queue_name := 'etl_load_bad_queue';
    ELSE
      dest_queue_name := 'etl_load_fact_queue';
    END IF;
      DBMS_AQ.ENQUEUE(
          queue_name         => dest_queue_name,
          enqueue_options    => r_enqueue_options,
          message_properties => r_message_properties,
          payload            => o_fact_common,
          msgid              => v_message_handle
      );
    COMMIT;
  END transform;

  PROCEDURE load_bad(
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
    o_fact_common        fact_common_type;
    BEGIN
      r_dequeue_options.msgid := descr.msg_id;
      r_dequeue_options.consumer_name := descr.consumer_name;
      r_dequeue_options.visibility := DBMS_AQ.IMMEDIATE;
      r_dequeue_options.delivery_mode := DBMS_AQ.BUFFERED;

      DBMS_AQ.DEQUEUE(
          queue_name         => descr.queue_name,
          dequeue_options    => r_dequeue_options,
          message_properties => r_message_properties,
          payload            => o_fact_common,
          msgid              => v_message_handle
      );

      INSERT INTO FACT_TAB_BAD (FACT_ID, BATCH_NUMBER, TABLE_OWNER_ID, TABLE_NAME, COLUMN_NAME, DATA_TYPE_ID, BLOCKS, PARTITIONED, SRC_OWNER, SRC_DATA_TYPE, SRC_PARTITIONED)
        VALUES
          (o_fact_common.FACT_ID, o_fact_common.BATCH_NUMBER, o_fact_common.TABLE_OWNER_ID, o_fact_common.TABLE_NAME, o_fact_common.COLUMN_NAME, o_fact_common.DATA_TYPE_ID, o_fact_common.BLOCKS, o_fact_common.PARTITIONED, o_fact_common.SRC_OWNER, o_fact_common.SRC_DATA_TYPE, o_fact_common.SRC_PARTITIONED);
      COMMIT;
  END load_bad;

  PROCEDURE load_fact(
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
    o_fact_common        fact_common_type;
    BEGIN
      r_dequeue_options.msgid := descr.msg_id;
      r_dequeue_options.consumer_name := descr.consumer_name;
      r_dequeue_options.visibility := DBMS_AQ.IMMEDIATE;
      r_dequeue_options.delivery_mode := DBMS_AQ.BUFFERED;

      DBMS_AQ.DEQUEUE(
          queue_name         => descr.queue_name,
          dequeue_options    => r_dequeue_options,
          message_properties => r_message_properties,
          payload            => o_fact_common,
          msgid              => v_message_handle
      );

      INSERT INTO FACT_TAB (FACT_ID, BATCH_NUMBER, TABLE_OWNER_ID, TABLE_NAME, COLUMN_NAME, DATA_TYPE_ID, BLOCKS, PARTITIONED)
      VALUES
        (o_fact_common.FACT_ID, o_fact_common.BATCH_NUMBER, o_fact_common.TABLE_OWNER_ID, o_fact_common.TABLE_NAME, o_fact_common.COLUMN_NAME, o_fact_common.DATA_TYPE_ID, o_fact_common.BLOCKS, o_fact_common.PARTITIONED);
      COMMIT;
  END load_fact;
END LOAD_AQ;