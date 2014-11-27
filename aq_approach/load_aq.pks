CREATE OR REPLACE PACKAGE etl_perf.load_aq
IS
  PROCEDURE read_source;
  PROCEDURE transform(
    context  RAW,
    reginfo  SYS.AQ$_REG_INFO,
    descr    SYS.AQ$_DESCRIPTOR,
    payload  RAW,
    payloadl NUMBER
  );

  PROCEDURE load_bad(
    context  RAW,
    reginfo  SYS.AQ$_REG_INFO,
    descr    SYS.AQ$_DESCRIPTOR,
    payload  RAW,
    payloadl NUMBER
  );

  PROCEDURE load_fact(
    context  RAW,
    reginfo  SYS.AQ$_REG_INFO,
    descr    SYS.AQ$_DESCRIPTOR,
    payload  RAW,
    payloadl NUMBER
  );
END LOAD_AQ;