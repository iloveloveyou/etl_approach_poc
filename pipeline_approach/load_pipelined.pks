CREATE OR REPLACE PACKAGE etl_perf.load_pipelined
IS
  SUBTYPE fact_data_row_t IS fact_tab_stage%ROWTYPE;
  TYPE fact_data_t IS TABLE OF fact_data_row_t;

  FUNCTION get_source_data(parallel_degree_p INTEGER := 1) RETURN SYS_REFCURSOR;

  FUNCTION get_transformed_data( cursor_p SYS_REFCURSOR ) RETURN fact_data_t PIPELINED;

  PROCEDURE load;
END load_pipelined;
/
