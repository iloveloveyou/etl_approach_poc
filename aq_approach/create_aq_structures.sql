-- Inspired by
-- http://www.oracle-developer.net/display.php?id=411

CREATE TYPE fact_common_element_type AS OBJECT(
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
  src_partitioned    VARCHAR2(3 BYTE),
  is_bad             VARCHAR2(1 BYTE)
);
/

CREATE TYPE fact_common_element_array_type AS TABLE OF fact_common_element_type;

-- @@aq_read_source.sql
-- @@aq_load_fact.sql
-- @@aq_load_bad.sql
-- @@aq_transform.sql

