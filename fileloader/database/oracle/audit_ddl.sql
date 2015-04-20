drop sequence osr_audit.seq_audit;
create sequence osr_audit.seq_audit;


drop table osr_audit.audit_file;
create table osr_audit.audit_file (
  file_id number not null,
  source_id number not null,
	file_name varchar2(50) not null,
	table_name varchar2(50) not null,
	etl_type char(1) not null,
	etl_date date not null,
	processed_flag char(1) not null,
	constraint audit_file_pk primary key (file_id)
);
