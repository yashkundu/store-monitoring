create table store_status(
	store_id varchar(25),
	status boolean,
	timestamp_utc timestamp
);

create table menu_hours(
	store_id varchar(25),
	day smallint,
	start_time_local timestamp,
	end_time_local timestamp
);


create table timezones(
	store_id varchar(25),
	timezone_str varchar(30)
);