CONN system/oracle;

CREATE USER etl_perf IDENTIFIED BY etl_perf TEMPORARY TABLESPACE TEMP DEFAULT TABLESPACE USERS;
GRANT DBA TO etl_perf;

CONN etl_perf/etl_perf;

create table src_fact_tab as
select batch_no, owner, table_name, column_name, data_type, BLOCKS, PARTITIONED
  from dba_tab_columns c
  join dba_tables t using(owner, table_name)
  cross join (select rownum batch_no from dual connect by level <=100);
  
create table dim_data_type_tab as
select rownum data_type_id, data_type, data_type_name
  from (select count(distinct data_type) over() dt_cnt, count(1) cnt,
               data_type, initcap(replace(regexp_replace(data_type,'\([[:digit:]]*\)'),'_',' ')) data_type_name from src_fact_tab group by data_type order by count(1) desc)
where rownum < dt_cnt;

alter table dim_data_type_tab add constraint dim_data_type_tab_pk primary key(data_type_id) using index;
CREATE UNIQUE INDEX dim_data_type_tab_u1 ON  dim_data_type_tab (data_type);

create table dim_owner_tab as
select rownum owner_id, owner, owner_name
  from (select count(distinct owner) over() own_cnt, count(1) cnt,
               owner, initcap(replace(regexp_replace(owner,'\([[:digit:]]*\)'),'_',' ')) owner_name from src_fact_tab group by owner order by count(1) desc)
where rownum < own_cnt;

alter table dim_owner_tab add constraint dim_owner_tab_pk primary key(owner_id) using index;
CREATE UNIQUE INDEX dim_owner_tab_u1 ON  dim_owner_tab (owner);

create sequence fact_id_seq;

CREATE TABLE fact_tab
(
  FACT_ID                  NUMBER NOT NULL,
  BATCH_NUMBER       NUMBER NOT NULL,
  TABLE_OWNER_ID    NUMBER NOT NULL,
  TABLE_NAME        VARCHAR2(128 BYTE)               NOT NULL,
  COLUMN_NAME    VARCHAR2(128 BYTE)               NOT NULL,
  DATA_TYPE_ID    NUMBER NOT NULL,
  BLOCKS              NUMBER NOT NULL,
  PARTITIONED     VARCHAR2(1 char) NOT NULL,
  CONSTRAINT partitioned_chk CHECK (partitioned IN ('Y', 'N'))
  );

ALTER TABLE fact_tab ADD CONSTRAINT fact_tab_pk PRIMARY KEY( fact_id ) USING INDEX;


CREATE TABLE fact_tab_bad
(
  FACT_ID                  NUMBER not null,
  BATCH_NUMBER       NUMBER,
  TABLE_OWNER_ID    NUMBER,
  TABLE_NAME        VARCHAR2(128 BYTE),
  COLUMN_NAME    VARCHAR2(128 BYTE),
  DATA_TYPE_ID    NUMBER,
  BLOCKS              NUMBER,
  PARTITIONED     VARCHAR2(1 char),
  src_owner          VARCHAR2(128 BYTE),
  src_data_type      VARCHAR2(128 BYTE),
  src_partitioned    VARCHAR2(3 BYTE)
  );

ALTER TABLE fact_tab_bad ADD CONSTRAINT fact_tab_bad_pk PRIMARY KEY( fact_id ) USING INDEX;


CREATE TABLE fact_tab_stage
(
  FACT_ID                  NUMBER not null,
  BATCH_NUMBER       NUMBER,
  TABLE_OWNER_ID    NUMBER,
  TABLE_NAME        VARCHAR2(128 BYTE),
  COLUMN_NAME    VARCHAR2(128 BYTE),
  DATA_TYPE_ID    NUMBER,
  BLOCKS              NUMBER,
  PARTITIONED     VARCHAR2(1 char),
  src_owner          VARCHAR2(128 BYTE),
  src_data_type      VARCHAR2(128 BYTE),
  src_partitioned    VARCHAR2(3 BYTE),
  is_bad                VARCHAR2(1 BYTE) not null,
  OPERATION_TYPE VARCHAR2(1 BYTE) not null,
  CONSTRAINT is_bad_chk CHECK (is_bad IN ('Y', 'N')),
  CONSTRAINT operation_type_chk CHECK (operation_type IN ('I', 'U', 'D'))
  );

ALTER TABLE fact_tab_stage ADD CONSTRAINT fact_tab_stage_pk PRIMARY KEY( fact_id ) USING INDEX;

