

CREATE TABLE public.emr_applications_execution_log_lz (
	app_id varchar NULL,
	app_name varchar NULL,
	queue varchar NULL,
	job_state varchar NULL,
	job_status varchar NULL,
	starttime varchar NULL,
	endtime varchar NULL,
	runtime_seconds float8 NULL,
	vcore_seconds numeric NULL,
	memory_seconds numeric NULL,
	running_containers int4 NULL,
	rm_clusterid varchar NULL
);

CREATE TABLE public.emr_applications_execution_log (
	appdatecollect date NULL,
	app_id varchar(200) NULL,
	app_name varchar NULL,
	queue varchar(100) NULL,
	job_state varchar(100) NULL,
	job_status varchar(100) NULL,
	starttime timestamp NULL,
	endtime timestamp NULL,
	runtime_seconds float8 NULL,
	vcore_seconds numeric NULL,
	memory_seconds numeric NULL,
	running_containers int4 NULL,
	rm_clusterid varchar(100) NULL,
	request_id varchar(100) NULL,
	owldq_job_uuid varchar(200) NULL,
	loadtime timestamp NULL
);



CREATE TABLE public.emr_cluster_usage_cost (
	costdatecollect date NULL,
	startdate date NULL,
	enddate date NULL,
	emr_unique_tag varchar(200) NULL,
	net_unblendedcost float8 NULL,
	unblendedcost float8 NULL,
	cost_type varchar(20) NULL,
	service_name varchar(200) NULL,
	emr_clusterid varchar(30) NULL,
	emr_clustername varchar(100) NULL,
	loadtime timestamp NULL
);


CREATE TABLE public.emr_cluster_instances_usage (
	instancedatecollect date NULL,
	emr_instance_day_run_seconds numeric NULL,
	emr_region varchar(30) NULL,
	emr_clusterid varchar(30) NULL,
	emr_clustername varchar(100) NULL,
	emr_cluster_fleet_type varchar(100) NULL,
	emr_node_type varchar(100) NULL,
	emr_market varchar(100) NULL,
	emr_instance_type varchar(100) NULL,
	emr_ec2_instance_id varchar(100) NULL,
	emr_ec2_status varchar(100) NULL,
	emr_ec2_default_vcpus int4 NULL,
	emr_ec2_memory int4 NULL,
	emr_ec2_creation_datetime timestamp NULL,
	emr_ec2_end_datetime timestamp NULL,
	emr_ec2_ready_datetime timestamp NULL,
	loadtime timestamp NULL
);