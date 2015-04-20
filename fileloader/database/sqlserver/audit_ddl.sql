set ansi_nulls on;
set quoted_identifier on;
set ansi_padding on;

if object_id('seq_audit') is not null
	drop sequence seq_audit;

create sequence seq_audit as int start with 1 increment by 1 cache;

if object_id('audit_file', 'u') is not null
	drop table audit_file;

create table audit_file (
  file_id bigint not null,
  source_id bigint not null,
	file_name varchar(50) not null,
	table_name varchar(50) not null,
	etl_type char(1) not null,
	etl_date datetime not null,
	processed_flag char(1) not null
	constraint audit_file_pk primary key clustered (file_id asc)
);
