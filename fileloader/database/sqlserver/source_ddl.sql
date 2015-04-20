set ansi_nulls on;
set quoted_identifier on;
set ansi_padding on;

if object_id('src_test', 'u') is not null
	drop table src_test;

create table src_test (
  test_id varchar(50) null,
  test_value varchar(50) null,
  source_id bigint null,
  file_id bigint null,
  record_id numeric(28, 0) null
);