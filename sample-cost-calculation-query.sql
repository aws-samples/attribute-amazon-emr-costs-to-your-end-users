select cal_cost_date,case WHEN app_name like 'fin%' then 'Finance' 
when  app_name like 'hr%'then 'HR'
when app_name like 'trs%'then 'Treasury'
when app_name like 'marketing%'then 'Marketing'
else 'Others' end Application_Name, 
queue, sum(vcore_seconds) ConsumedCPUSeconds, sum(distinct percentage_share) prctConsumed
, sum(emr_cost) AS ComsumedAmount, sum(distinct emr_total_day_cost) AS TotalEMRClusterCostforOwlDQ
 
from(
select lower(app_name) as app_name, AppDateCollect as cal_cost_date,
queue, vcore_seconds,(shareper.vcore_seconds/shareper.emr_app_consumed_day_vcore_seconds)*100 as percentage_share,
shareper.net_unblendedcost*((shareper.vcore_seconds/shareper.emr_app_consumed_day_vcore_seconds)*100)/100 as emr_cost,net_unblendedcost as emr_total_day_cost,
shareper.runtime_seconds, memory_seconds , emr_allocated_vcore_seconds, emr_app_consumed_day_vcore_seconds as  emr_allapps_consumed_day_vcore_seconds,
(
emr_allocated_vcore_seconds-emr_app_consumed_day_vcore_seconds) emr_idle_vcore_seconds_day
from
(
select  apps.AppDateCollect, apps.app_name,apps.queue,total_vcore.emr_app_consumed_day_vcore_seconds, day_emr_cost.net_unblendedcost,
sum(runtime_seconds) as runtime_seconds, sum(vcore_seconds) as vcore_seconds, sum(memory_seconds) as memory_seconds, instance_usage.emr_instance_day_vcore_seconds as emr_allocated_vcore_seconds
from
             (select AppDateCollect, app_name, queue,runtime_seconds,vcore_seconds,memory_seconds,
            
             request_id,
             owldq_job_uuid            
             from public.emr_applications_execution_log
             --where queue like '%owldq%'
             --and AppDateCollect = '2023-01-18'
             ) apps,
                          (
                          select sum(vcore_seconds)  emr_app_consumed_day_vcore_seconds, AppDateCollect
                          from public.emr_applications_execution_log --where cast(starttime as date) ='2022-10-28'
                          --where queue like '%owldq%'
                          --and AppDateCollect = '2023-01-18'
                          group by AppDateCollect) total_vcore,
                          (select 'EMR' as cluster,CostDateCollect,sum(net_unblendedcost) as net_unblendedcost--, sum(net_unblendedcost)*.25 as net_unblendedcost
                          from public.emr_cluster_usage_cost
                          --where enddate='2022-10-28'
                          group by cluster,CostDateCollect
                          ) day_emr_cost,
                          (select iu.instancedatecollect, sum(iu.emr_instance_day_run_seconds*iu.emr_ec2_default_vcpus) as emr_instance_day_vcore_seconds --,sum(iu.emr_instance_day_run_seconds*iu.emr_ec2_default_vcpus)*.25 as emr_instance_day_vcore_seconds
                          from public.emr_cluster_instances_usage iu group by iu.instancedatecollect
                          ) instance_usage
 
where --cast(starttime as date) ='2022-10-28' and
apps.AppDateCollect=day_emr_cost.CostDateCollect
and apps.AppDateCollect=total_vcore.AppDateCollect
and apps.AppDateCollect=instance_usage.instancedatecollect
and total_vcore.emr_app_consumed_day_vcore_seconds <>0
group by  apps.AppDateCollect, apps.app_name, apps.queue, total_vcore.emr_app_consumed_day_vcore_seconds, day_emr_cost.net_unblendedcost, emr_allocated_vcore_seconds
) shareper
)A
group by 1,2,3 order by 1 desc