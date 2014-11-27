CREATE OR REPLACE PACKAGE etl_perf.load_classic
IS
  PROCEDURE load_bad;
  PROCEDURE load_stage;
END load_classic;
/
