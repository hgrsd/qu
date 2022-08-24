create table if not exists qu
(
    internal_id            bigserial,
		job_id					 		   uuid           not null,
    scheduled_for          timestamptz,
    status 				         text 					not null,
    payload                jsonb          not null,
    primary key (internal_id)
);

create index if not exists idx_status_scheduled_for ON qu (status, scheduled_for);
