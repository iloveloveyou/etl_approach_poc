CREATE OR REPLACE PACKAGE etl_perf.load_pipelined
IS
  SUBTYPE fact_data_row_t IS fact_tab_stage%ROWTYPE;
  TYPE fact_data_t IS TABLE OF fact_data_row_t;

  C_BULK_LIMIT CONSTANT INTEGER := 1000;

  FUNCTION get_source_data_sql( p_parallel_degree INTEGER := 1 ) RETURN VARCHAR2;
  FUNCTION get_source_data( p_parallel_degree INTEGER := 1 ) RETURN SYS_REFCURSOR;

  FUNCTION get_transformed_data( p_cursor SYS_REFCURSOR ) RETURN fact_data_t PIPELINED PARALLEL_ENABLE (PARTITION p_cursor BY ANY);

  PROCEDURE load( p_parallel_degree INTEGER := 1 );
END load_pipelined;
/
