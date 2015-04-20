drop sequence if exists audit.seq_audit;
create sequence audit.seq_audit;
alter sequence audit.seq_audit owner to audit;

drop table if exists audit.audit_file;
create table audit.audit_file
(
  file_id bigint not null,
  source_id bigint not null,
  file_name character varying(50) not null,
  table_name character varying(50) not null,
  etl_type character(1) not null,
  etl_date timestamp without time zone not null,
  processed_flag character(1) not null,
  constraint audit_file_pk primary key (file_id)
);
alter table audit.audit_file owner to audit;