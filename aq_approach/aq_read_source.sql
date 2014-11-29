CREATE OR REPLACE PROCEDURE aq_read_source
IS
  r_enqueue_options    DBMS_AQ.ENQUEUE_OPTIONS_T;
  r_message_properties DBMS_AQ.MESSAGE_PROPERTIES_T;

  v_message_handle     RAW(16);
  o_fact_common_lst    FACT_COMMON_ELEMENT_ARRAY_TYPE;
  crsr                 SYS_REFCURSOR;
  buffer_size          INTEGER := 5000;
  BEGIN
    r_enqueue_options.transformation := ('ETL_PERF.AQ_TRANSFORM');
    OPEN crsr FOR
    SELECT fact_common_element_type( fact_id_seq.nextval, batch_no, /*o.owner_id*/NULL, table_name, column_name, /*d.data_type_id*/NULL,
                                     blocks, null, a.owner, a.data_type, a.partitioned, null)
    FROM src_fact_tab a
/*LEFT JOIN DIM_OWNER_TAB o ON o.OWNER = a.OWNER
LEFT JOIN DIM_DATA_TYPE_TAB d ON d.DATA_TYPE = a.DATA_TYPE*/;
    LOOP
      FETCH crsr BULK COLLECT INTO o_fact_common_lst limit 1000;
      EXIT WHEN o_fact_common_lst.count = 0;
      DBMS_AQ.ENQUEUE(
          queue_name         => 'etl_load_queue',
          enqueue_options    => r_enqueue_options,
          message_properties => r_message_properties,
          payload            => ANYDATA.ConvertCollection( o_fact_common_lst ),
          msgid              => v_message_handle
      );
      COMMIT;
      EXIT WHEN crsr%NOTFOUND;
    END LOOP;
    CLOSE crsr;
  END aq_read_source;
/

