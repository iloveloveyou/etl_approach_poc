CREATE OR REPLACE PACKAGE etl_perf.load_classic
IS
  PROCEDURE load_bad( p_parallel_degree INTEGER := 1 );
  PROCEDURE load_stage( p_parallel_degree INTEGER := 1 );
  PROCEDURE load_fact( p_parallel_degree INTEGER := 1 );
END load_classic;
/
