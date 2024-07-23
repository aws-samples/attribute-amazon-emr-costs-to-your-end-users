
import requests
import json
import time
import boto3
from pprint import pprint
from datetime import datetime, date, timedelta
from botocore.errorfactory import ClientError
import psycopg2

ce_client = boto3.client('ce')
ssm=boto3.client('ssm')
emr_client = boto3.client('emr','us-east-1')
ec2 = boto3.client('ec2','us-east-1')
secrets_manager = boto3.client('secretsmanager')

url = ''
postgres_host = ''
postgres_db = ''
postgres_user = ''
table_emrlogs_lz = ''
table_emrlogs = ''
emr_cost_table_name= ''
emr_table_name= ''
emr_cluster_id= ''
emr_clustername= ''
emrcluster_role= ''
emrcluster_linkedaccount= ''
cur = ''

region='us-east-1'
loaddatetime = str(datetime.today())
CostType="Daily"
Difference_Sec=0
InstanceDayRunSeconds=0

def lambda_handler(event, context):

    #====Read SSM parameter store to fetch environment vairables====#
    print("Reading SSM Parameter Store ")
    ssm_response = ssm.get_parameters(Names=['cost_metering_parameters'])
    print("SSM Parameter Store is read")
    ssm_param = ssm_response['Parameters'][0]['Value']
    #print(ssm_param)
    ssm_param_json =json.loads(ssm_param)    
    #print(ssm_param_json)
    global url
    global postgres_host
    global postgres_db
    global postgres_user
    global table_emrlogs_lz
    global table_emrlogs
    global emr_cost_table_name
    global emr_table_name
    global emr_cluster_id
    global emr_clustername
    global emrcluster_role
    global emrcluster_linkedaccount
    global cur
    
##==================================================================================================
# SSM Parameter is used here to store relevant parameters. Please create corresponding entries else
# else pass as per your conveneince 
##==================================================================================================
    print(ssm_param_json)
    url = ssm_param_json["yarn_url"] #Yarn url for rest API calls
    print(url)
    postgres_host = ssm_param_json["postgres_rds"]["host"] #Postgres host to store usage/cost data
    postgres_db = ssm_param_json["postgres_rds"]["dbname"] #database name
    rds_secretid = ssm_param_json["postgres_rds"]["secretid"] #RDS Postgres login password secret id
    table_emrlogs_lz = ssm_param_json["tbl_applicationlogs_lz"] #Landing zone table to capture daily application jobs usage data from yarn Resource manager
    table_emrlogs = ssm_param_json["tbl_applicationlogs"] #Main table to capture daily application jobs usage data from yarn Resource manager
    emr_cost_table_name= ssm_param_json["tbl_emrcost"] #Store total daily cost incurred for complete EMR cluster
    emr_table_name=ssm_param_json["tbl_emrinstance_usage"] #Daily EMR cluster usage data
    emr_cluster_id=ssm_param_json["emrcluster_id"] # EMR clsuterid for which cost report need to be generated
    emr_clustername=ssm_param_json["emrcluster_name"] #EMR clsuter name for which cost report need to be generated
    emrcluster_role=ssm_param_json["emrcluster_role"] #EMR cluster access role 
    emrcluster_linkedaccount=ssm_param_json["emrcluster_linkedaccount"] #EMR cluster AWS account id
    
    #====Read postgres credential from secrets manager====#
    print("reading rds secrets")
    secrets_resp = secrets_manager.get_secret_value(SecretId=rds_secretid)
    secret = secrets_resp['SecretString']
    secret = json.loads(secret)
    postgres_user = secret["username"]#RDS Postgres UserName
    postgres_password = secret["password"]# RDS Postgres Password
    
    print('before db connect')
    conn = psycopg2.connect("host="+postgres_host+" dbname="+postgres_db+" user="+postgres_user+" password="+postgres_password)
    cur = conn.cursor()

    
##==================================================================================================
# Logic to initiate variables to avoid duplicate data load on same day
##==================================================================================================   
    initial_datetime = datetime.now() - timedelta(90)
    initial_date = datetime.strftime(initial_datetime, '%Y-%m-%d')
    today_date = datetime.today().strftime('%Y-%m-%d')
    yesterday = datetime.now() - timedelta(1)
    yesterday_date = datetime.strftime(yesterday, '%Y-%m-%d')

    cur.execute('select max(AppDateCollect)::varchar from '+table_emrlogs)
    max_AppDateCollect = cur.fetchone()[0]
    if max_AppDateCollect == None:
        max_AppDateCollect = initial_date

    if yesterday_date == max_AppDateCollect:
        yesterday_minusone = datetime.now() - timedelta(2)
        max_AppDateCollect = datetime.strftime(yesterday_minusone, '%Y-%m-%d')
        
    
    cur.execute('select max(InstanceDateCollect) from '+emr_table_name)
    max_InstanceDateCollect = cur.fetchone()[0]
    
    if max_InstanceDateCollect == None:
        max_InstanceDateCollect = initial_date
    
    if str(yesterday_date) == str(max_InstanceDateCollect):
        print('-----------Yesterdays data is already there for instance table')
        yesterday_minusone = datetime.now() - timedelta(2)
        max_InstanceDateCollect = datetime.strftime(yesterday_minusone, '%Y-%m-%d')
    
    cur.execute('select max(CostDateCollect) from '+emr_cost_table_name)
    max_CostDateCollect = cur.fetchone()[0]
    
    if max_CostDateCollect == None:
        max_CostDateCollect = initial_date
    
    print("max_AppDateCollect -->",max_AppDateCollect)
    print("max_CostDateCollect -->",max_CostDateCollect)
    print("max_InstanceDateCollect -->",max_InstanceDateCollect)
    #print("yesterday_date -->",yesterday_date)
    
    if str(yesterday_date) == str(max_CostDateCollect):
        #print('-----------Yesterdays data is already there for cost table')
        yesterday_minusone = datetime.now() - timedelta(2)
        max_CostDateCollect = datetime.strftime(yesterday_minusone, '%Y-%m-%d')
        #print('max_CostDateCollect-->',max_CostDateCollect)

    ###Load Application usage data
    print("Loading application usage data for the range - ["+str(max_AppDateCollect)+","+str(today_date)+"]")
    emr_applications_execution(str(max_AppDateCollect),str(today_date))
    #exit()
    
    # # ###Load EMR/EC2 cost usage data
    print("Loading EMR/EC2 cost usage data")
    emr_cluster_cost_usage(str(max_CostDateCollect),str(today_date))

    ###Load EMR/EC2 instance usage data
    print("Loading  EMR/EC2 instance usage data")
    emr_cluster_instances_usage(str(max_InstanceDateCollect),str(yesterday_date))

##==================================================================================================
# Module to read application execution logs from yarn RM URL via rst api call
# 1. Read application jobs execution data from Resource Manger
# 2. Load this into a RDS landing zone table
# 3. FInal insert into main rds table
##==================================================================================================
def emr_applications_execution(startdate,enddate):
    
    startdate_ts = startdate + ' 23:59:59'
    enddate_ts = enddate + ' 00:00:00'
    
    startdate_epoc = int(time.mktime(time.strptime(str(startdate_ts), '%Y-%m-%d %H:%M:%S')))
    enddate_epoc = int(time.mktime(time.strptime(str(enddate_ts), '%Y-%m-%d %H:%M:%S')))
    
    startdate_epoc = str(startdate_epoc) + '000'
    enddate_epoc = str(enddate_epoc) + '000'
    
    #print("startdate -->",startdate)
    #print("enddate -->",enddate)
    
    #print("startdate_epoc -->",startdate_epoc)
    #print("enddate_epoc -->",enddate_epoc)
    #exit()
    
    url_filtered = url + "?finishedTimeBegin="+startdate_epoc + "&finishedTimeEnd=" + enddate_epoc
    print(url_filtered)
    response = requests.get(url_filtered)
    print("Application usage data from yarn url")
    jsondata=json.loads(response.text)
    print("json response- ",jsondata)
    #exit()

    ##----Delete landing zone table 
    print("Deleting data from landing zone table: "+table_emrlogs_lz)
    cur.execute("DELETE FROM "+table_emrlogs_lz)
    cur.execute("commit")
    print("Landing zone table deletion complete")
    print("Deletion complete")
        
        
    starttime_ts = ""
    endtime_ts = ""
    
    
        ##----Populate current day data into landing zone table 
    
    if "app" in jsondata["apps"]:
            try:
            
                for data in jsondata["apps"]["app"]:
                    collect_date = time.strftime('%m/%d/%Y %H:%M:%S',  time.gmtime(data['startedTime']/1000.))
                    #print(collect_date)
                    starttime_ts = time.strftime('%m/%d/%Y %H:%M:%S',  time.gmtime(data['startedTime']/1000.))
                    endtime_ts = time.strftime('%m/%d/%Y %H:%M:%S',  time.gmtime(data['finishedTime']/1000.))
                    insert_query = "INSERT INTO "+table_emrlogs_lz+"(app_id,app_name,queue,job_state,job_status,starttime,endtime,runtime_seconds,vcore_seconds,memory_seconds,running_containers,rm_clusterid)"+\
                            "VALUES('"+str(data['id'])+"','"+data['name']+"','"+data['queue']+"','"+data['state']+"','"+data['finalStatus']+"','"+str(starttime_ts)+"','"+str(endtime_ts)+"','"+str(data['elapsedTime']/1000.)+\
                            "','"+str(data['vcoreSeconds'])+"','"+str(data['memorySeconds'])+"','"+str(data['runningContainers'])+"','"+str(data['clusterId'])+"')"
                    #print("Populated current day data into landing zone table--")
                    print("insert_query-->",insert_query)
                    cur.execute(insert_query)
                    cur.execute("commit")
            #exit()
            
        
            except ClientError:
                print("error catch")
    
            print("Landing zone data load completd, fetching rowcount...")
            cur.execute("select count(*) from public.emr_applications_execution_log_lz "+ " where endtime::date > '"+startdate+"' and endtime::date < '"+enddate+"'")
            row = cur.fetchone()
            print('lz row count-->',row)
    
            cur.execute("select count(*) from public.emr_applications_execution_log ")
            row = cur.fetchone()
            print('final table row count-->',row)
    
            ##----Delete data from main application log  table 
            print("Deleting data from main application log table: "+table_emrlogs)
            final_table_delete = "delete from "+table_emrlogs+" where AppDateCollect > '"+startdate+"' and AppDateCollect < '"+enddate+"'"
            print('final_table_delete-->',final_table_delete)
            cur.execute(final_table_delete)
            cur.execute("commit")
            print("Main Table deletion complete")
            # except ClientError:
    
            try:
                print("loading final table now...")    
                final_insert_query = "insert into "+table_emrlogs+"(appdatecollect,app_id,app_name,queue,job_state,job_status,starttime,endtime,runtime_seconds,vcore_seconds,memory_seconds,running_containers,rm_clusterid,loadtime)"\
                            " select  endtime::date ,app_id, app_name,queue,\
                            job_state, job_status, starttime::timestamp,endtime::timestamp,runtime_seconds, vcore_seconds,memory_seconds,running_containers,rm_clusterid,\
                            NOW()::timestamp as loaddatetime\
                            from "+table_emrlogs_lz+\
                            " where endtime::date > '"+startdate+"' and endtime::date < '"+enddate+"'"
        
                print('final_insert_query-->',final_insert_query)
        
                cur.execute(final_insert_query)
                cur.execute("commit")
                print("Values Inserted in final table: "+table_emrlogs)
        
            except ClientError:
                print("catch error")
    else:
        print("No Application logs found for the timeframe - ['"+startdate+"', '"+enddate+"']")
        
##==================================================================================================
# Module to read EMR cluster usage details via boto3 api
# 1. Read EMR cluster usage data via boto3 describe_cluster api call
# 2. Load this into the RDS table
##================================================================================================== 
def emr_cluster_instances_usage(startdate,enddate):

    print("Deleting data from"+emr_table_name+" between "+startdate+" and "+enddate)
    delete_query = "DELETE FROM "+emr_table_name + " WHERE InstanceDateCollect > '"+startdate+"' AND InstanceDateCollect <= '"+enddate+"'"
    cur.execute(delete_query)
    cur.execute("commit")
    print("Deletion complete")

        
    try:
        print("Fetching Cluster instance info via descibe_cluster")
        emrdescribe=emr_client.describe_cluster(ClusterId=emr_cluster_id)
        print(emrdescribe)
        emrClusterStatus=emrdescribe['Cluster']['Status']['State']
        emrClusterName=emrdescribe['Cluster']['Name']
        emrClusterTags=emrdescribe['Cluster']['Tags']
        for tag in emrClusterTags:
            if tag['Key']=='CostCenter' and tag['Value']=='GEHC':
                print(tag['Key'],"<==>", tag['Value'])
        if emrClusterStatus=='RUNNING' or emrClusterStatus=='WAITING':
            emrInstanceCollectionType=emrdescribe['Cluster']['InstanceCollectionType'] ##INSTANCE_FLEET ##INSTANCE_GROUP
            
            fullist=[]
            
            if emrInstanceCollectionType=='INSTANCE_FLEET':
                emrmasterinstances = emr_client.list_instances(ClusterId=emr_cluster_id, InstanceFleetType='MASTER')
                newemrmasterinstances={'InstanceFleetType':'MASTER'}
                newemrmasterinstances.update(emrmasterinstances)
                emrcoreinstances = emr_client.list_instances(ClusterId=emr_cluster_id, InstanceFleetType='CORE')
                newemrcoreinstances={'InstanceFleetType':'CORE'}
                newemrcoreinstances.update(emrcoreinstances)
                emrtaskinstances = emr_client.list_instances(ClusterId=emr_cluster_id, InstanceFleetType='TASK')
                newemrtaskinstances={'InstanceFleetType':'TASK'}
                newemrtaskinstances.update(emrtaskinstances)
            else:
                emrmasterinstances = emr_client.list_instances(ClusterId=emr_cluster_id, InstanceGroupTypes=['MASTER'])
                newemrmasterinstances={'InstanceFleetType':'MASTER'}
                newemrmasterinstances.update(emrmasterinstances)
                emrcoreinstances = emr_client.list_instances(ClusterId=emr_cluster_id, InstanceGroupTypes=['CORE'])
                newemrcoreinstances={'InstanceFleetType':'CORE'}
                newemrcoreinstances.update(emrcoreinstances)
                emrtaskinstances = emr_client.list_instances(ClusterId=emr_cluster_id, InstanceGroupTypes=['TASK'])
                newemrtaskinstances={'InstanceFleetType':'TASK'}
                newemrtaskinstances.update(emrtaskinstances)
            
            fullist.append(newemrmasterinstances)
            fullist.append(newemrcoreinstances)
            
            fullist.append(newemrtaskinstances)
            InstanceEndDateTime=datetime.today().replace(tzinfo=None)
            
            print("fullist--fullist",fullist)
            for emrinstances in fullist:
                    InstanceInstanceFleetType=emrinstances['InstanceFleetType']
                    for emrinstance in emrinstances['Instances']:
                        InstanceId=emrinstance['Ec2InstanceId']
                        InstanceState=emrinstance['Status']['State']
                        InstanceCreationDateTime=emrinstance['Status']['Timeline']['CreationDateTime'].replace(tzinfo=None)
                        InstanceCreationDateTime_Day= InstanceCreationDateTime.date()
                        InstanceReadyDateTime=emrinstance['Status']['Timeline']['ReadyDateTime'].replace(tzinfo=None)
                        InstanceInstanceType=emrinstance['InstanceType']
                        InstanceMarket=emrinstance['Market']
                        s_date=datetime.strptime(startdate,"%Y-%m-%d")#Date which already into the table
                        e_date=datetime.strptime(enddate,"%Y-%m-%d")#Till this date data needs to be loaded
                        print("Ec2InstanceId-->",InstanceId)
                        #print("Running for:",InstanceId, "Start:",startdate,"Date collect is:",enddate)
                        #print("Running for: "+InstanceId+" with State: "+ InstanceState+" and Fleet Type "+InstanceInstanceFleetType+" Created On:"+str(InstanceCreationDateTime))
                        delta = timedelta(days=1)
                        s_date += delta#as data is already loaded for start date value
                        while s_date<=e_date:
                            v_load_flag = False
                            InstanceDateCollect=s_date
                            InstanceDateCollect_Day= InstanceDateCollect.date()
                            print("InstanceId:"+InstanceId+" Data collection: "+ str(InstanceDateCollect)+" InstanceCreationDateTime:"+str(InstanceCreationDateTime_Day))
                            if InstanceState=='TERMINATED' and InstanceCreationDateTime.date() <= InstanceDateCollect.date():
                                InstanceEndDateTime=emrinstance['Status']['Timeline']['EndDateTime'].replace(tzinfo=None)
                                print("Instance was created on or before collectiodate and terminated on collectiondate")
                                print("value of InstanceEndDateTime is:",InstanceEndDateTime)
                                if InstanceEndDateTime.date() == InstanceDateCollect.date():
                                    Difference_Sec1=InstanceEndDateTime-InstanceCreationDateTime
                                    InstanceDayRunSeconds=Difference_Sec1.total_seconds()
                                    v_load_flag = True
                                #print("when created on or before collection date and terminated on collection date : end - start") 
                                #print("Instance created on or before "+str(InstanceDateCollect)+" and terminated on "+str(InstanceDateCollect)+" with running seconds as: "+str(InstanceDayRunSeconds))
                            elif (InstanceState=='RUNNING' or InstanceState=='BOOTSTRAPPING') and InstanceCreationDateTime.date() < InstanceDateCollect.date():
                                print("Instance was created before the collectiondate and still running")
                                InstanceDayRunSeconds=86400 ##ran for whole day
                                # print("fleet type is:",InstanceInstanceFleetType)
                                InstanceEndDateTime='9999-12-31 00:00:00'
                                #print("Instance still running with running seconds as: "+str(InstanceDayRunSeconds))
                                #print("when running and creation < collection day")
                                v_load_flag = True
                            elif (InstanceState=='RUNNING' or InstanceState=='BOOTSTRAPPING') and InstanceCreationDateTime_Day == InstanceDateCollect_Day:
                                print("Instance was created on the collectiondate and still running")
                                Difference_Sec1=InstanceCreationDateTime-InstanceDateCollect
                                InstanceDayRunSeconds=86400-Difference_Sec1.total_seconds()
                                InstanceEndDateTime='9999-12-31 00:00:00'
                                #print("created on collection date and still running")
                                #print("Instance created on "+str(InstanceDateCollect)+" and with running seconds as: "+str(InstanceDayRunSeconds))
                                v_load_flag = True
                            #elif (InstanceState=='RUNNING' or InstanceState=='BOOTSTRAPPING') and InstanceCreationDateTime_Day > InstanceDateCollect_Day:
                            elif InstanceCreationDateTime_Day > InstanceDateCollect_Day:
                                print("Instance was created after the collectiondate, not be loaded for the collectiondate")
                                InstanceDayRunSeconds=0
                                
                                #print("Instance created after "+str(InstanceDateCollect))
                        
                            
                            else:
                                #print("State of instance is:",InstanceState,"instance Endtime is:",InstanceEndDateTime)
                                Difference_Sec1=InstanceEndDateTime.replace(tzinfo=None)-InstanceCreationDateTime.replace(tzinfo=None)                                
                                InstanceDayRunSeconds=Difference_Sec1.total_seconds()
                                #print("default")
                                print("Instance at default state")
                            ec2nstancetype = ec2.describe_instance_types(InstanceTypes=[InstanceInstanceType])
                            InstanceVcpuCount = ec2nstancetype['InstanceTypes'][0]['VCpuInfo']['DefaultVCpus']
                            InstanceSizeInMiB = ec2nstancetype['InstanceTypes'][0]['MemoryInfo']['SizeInMiB']
                            #print(InstanceDateCollect,"<==>",region,"<==>",emr_cluster_id,"<==>",InstanceInstanceFleetType,"<==>",InstanceId,"<==>",InstanceInstanceType,"<==>",InstanceMarket,"<==>",InstanceCreationDateTime,"<==>",InstanceEndDateTime,"<==>",InstanceVcpuCount,"<==>",InstanceSizeInMiB,"<==>",InstanceState,"<==>",InstanceDayRunSeconds)
                            
                            try:
                                if InstanceDayRunSeconds!=0 and v_load_flag == True:
                                    print("value of InstanceEndDateTime just before insert is:",InstanceEndDateTime)
                                    insert_query = "INSERT INTO "+emr_table_name+" (InstanceDateCollect, emr_instance_day_run_seconds, emr_region, emr_clusterid, emr_clustername, emr_cluster_fleet_type,emr_node_type, emr_market, emr_instance_type, emr_ec2_instance_id, emr_ec2_status, emr_ec2_default_vcpus,emr_ec2_memory, emr_ec2_creation_datetime, emr_ec2_end_datetime,  emr_ec2_ready_datetime, loadtime)"+\
                                        "VALUES('"+str(InstanceDateCollect)+"','"+str(InstanceDayRunSeconds)+"','"+region+"','"+emr_cluster_id+"','"+emr_clustername+"','"+emrInstanceCollectionType+"','"+InstanceInstanceFleetType+"','"+InstanceMarket+"','"+InstanceInstanceType+"','"+InstanceId+"','"+InstanceState+"','"+str(InstanceVcpuCount)+"','"+str(InstanceSizeInMiB)+"', '"+str(InstanceCreationDateTime)+"','"+str(InstanceEndDateTime)+"','"+str(InstanceReadyDateTime)+"','"+loaddatetime+"')"
                                    cur.execute(insert_query)
                                    cur.execute("commit")
                                    #print("row inserted to postgres emr instance table")
                                    #print("Value inserted in table: "+emr_table_name)
                                    #print(str(InstanceDateCollect)+"<==>"+region+"<==>"+emr_cluster_id+"<==>"+InstanceInstanceFleetType+"<==>"+InstanceId+"<==>"+InstanceInstanceType+"<==>"+InstanceMarket+"<==>"+str(InstanceCreationDateTime)+"<==>"+str(InstanceEndDateTime)+"<==>"+str(InstanceVcpuCount)+"<==>"+str(InstanceSizeInMiB)+"<==>"+InstanceState+"<==>"+str(InstanceDayRunSeconds))
                            
                            except ClientError:
                                print("catch-error")
                            s_date += delta
                            
        else:
            print("Cluster is not in RUNNING or WAITING state")
        
    except ClientError as e:
        print(str(e))

##==================================================================================================
# Module to read EMR cluster $cost data via boto3 cost explorer api
# 1. Read EMR cluster usage data via boto3 cost explorer(ce) api call
# 2. Load this into the RDS table
##================================================================================================== 
def emr_cluster_cost_usage(startdate,enddate):  
    
    print("Fetching count data from",emr_cost_table_name," where CostDateCollect is > ",startdate+" and <= ",enddate)
    count_query = "SELECT COUNT(*) FROM "+emr_cost_table_name + " WHERE CostDateCollect >= '"+startdate+"' AND CostDateCollect <= '"+enddate+"'"
    cur.execute(count_query) 
    row_count = cur.fetchone()
    print("row_count -->",row_count)
    
    print("Deleting data from",emr_cost_table_name," where CostDateCollect is > ",startdate+" and <= ",enddate)
    delete_query = "DELETE FROM "+emr_cost_table_name + " WHERE CostDateCollect >= '"+startdate+"' AND CostDateCollect <= '"+enddate+"'"
    cur.execute(delete_query)
    cur.execute("commit")
    print("Deletion complete")
    
        
    try:
        print("Reading CostExplorer data")
        response = ce_client.get_cost_and_usage(
            TimePeriod={
                'Start': startdate,
                'End': enddate
                      },
            Metrics=['UnblendedCost','NetUnblendedCost'],
            Granularity='DAILY',
            Filter={
                    "And": [ 
                            {"Dimensions": { "Key": "SERVICE", "Values": [ "Amazon Elastic MapReduce","Amazon Elastic Compute Cloud - Compute" ] }}
                            , 
                            
                            {"Tags": { "Key": "EMR_Objective", "Values": ["Cost"] } }
                            ,
                            {"Dimensions": { "Key": "LINKED_ACCOUNT", "Values": [emrcluster_linkedaccount] }}
                                                ]
                  },
            GroupBy=[ {"Type":"DIMENSION","Key":"SERVICE"},
                      {"Type":"TAG","Key":"EMR_Objective"} ]
        )
            
        print(response['ResultsByTime'])
        #exit()
        for timeline in response['ResultsByTime']:
            EndDate=timeline['TimePeriod']['End']
            StartDate=timeline['TimePeriod']['Start']
            if len(timeline['Groups']) != 0:
                for group in timeline['Groups']:
                        ServiceNameTag=group['Keys']
                        serviceName=ServiceNameTag[0]
                        serviceTag=ServiceNameTag[1]
                        NetUnblendedCost=group['Metrics']['NetUnblendedCost']['Amount']
                        NetUnblendedCostUnit=group['Metrics']['NetUnblendedCost']['Unit']
                        print(StartDate,"<==>",EndDate,"<==>",serviceName,"<==>",serviceTag,"<==>",NetUnblendedCost,"<==>",NetUnblendedCostUnit)
                        try:
                            insert_query = "INSERT INTO "+emr_cost_table_name+" (CostDateCollect,startdate,enddate,emr_unique_tag,net_unblendedcost,unblendedcost,cost_type,service_name,emr_clusterid,emr_clustername,loadtime)"+\
                                    "VALUES('"+StartDate+"','"+StartDate+"','"+EndDate+"','"+serviceTag+"','"+NetUnblendedCost+"','"+NetUnblendedCost+"','"+CostType+"','"+serviceName+"','"+emr_cluster_id+\
                                    "','"+emr_clustername+"','"+loaddatetime+"')"
                            cur.execute(insert_query)
                            cur.execute("commit")
                            print("row inserted to postgres cost table")
                            print("Values inserted into "+emr_cost_table_name)
                            print("--"+StartDate+"<==>"+EndDate+"<==>"+serviceName+"<==>"+serviceTag+"<==>"+NetUnblendedCost+"<==>"+NetUnblendedCostUnit+"<==>"+emr_cluster_id+"<==>"+emr_clustername+"<==>"+loaddatetime)
                            
                        except ClientError:
                            print("catch-error")
            else:
                
                NetUnblendedCost=timeline['Total']['NetUnblendedCost']['Amount']
                NetUnblendedCostUnit=timeline['Total']['NetUnblendedCost']['Unit']
                print("Data not inserted into ",emr_cost_table_name)
                print("--",StartDate,"<==>",EndDate,"<==>",NetUnblendedCost,"<==>",NetUnblendedCostUnit)
                
    except ClientError as e:
        print(str(e))