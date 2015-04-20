drop table if exists source.src_test;
create table source.src_test
(
  test_id character varying(50),
  test_value character varying(50),
  source_id bigint,
  file_id bigint,
  record_id numeric(28,0)
);
alter table source.src_test owner to source;