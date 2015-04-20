drop table osr_source.src_test;

create table osr_source.src_test (
  test_id varchar2(50) null,
  test_value varchar2(50) null,
  source_id number null,
  file_id number null,
  record_id number(28, 0) null
);