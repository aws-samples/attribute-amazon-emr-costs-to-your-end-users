
# How to attribute Amazon EMR costs to your end-users

This project is about a chargeback model that can be used to track and allocate the costs of Spark workloads running on Amazon EMR on EC2 clusters. We describe an approach that assigns Amazon EMR costs to different jobs, teams, or lines of business. 

This approach can help you evaluate and improve the cost efficiency of your EMR clusters. It can also be used to allocate costs to different lines of business, which can help you track the return on investment for your Spark workloads.

The solution is designed to help you track the cost of your Spark applications running on EMR on EC2. It can help you identify cost optimizations and improve the cost efficiency of your EMR clusters.

In this solution,  we use CDK to deploy a Lambda function, RDS Postgres 3 data model tables and a QuickSight dashboard to track EMR cluster cost at the job, team, or business unit level.

The Lambda function uses YARN resource manager APIs to extract the vCore-seconds, memory MB-seconds, and storage GB-seconds consumed by each Spark application that ran on your EMR cluster. It also extracts the daily cost of EMR, including EC2, using the AWS Cost Explorer Boto3 APIs. Lastly, it extracts the EC2 usage details using the EMR and EC2 Boto3 APIs.

The process runs daily extracting the previous day’s data and stores it in a Postgres RDS table. The historical data in the Postgres tables need to be purged based on the need. 

# How to Deploy the Solution

# 1. Create RDS Tables

Please create following tables by loging into postgres rds manually into public schema. -  emr-cost-rds-tables-ddl.sql .  
Use dbeaver /SQL clients to connect to RDS instance and validate tables have been created.

# 2. Deploy AWS CDK stacks

Complete the following steps to deploy your resources using the AWS CDK:

1. Clone the GitHub repo:
```
git clone git@github.com:aws-samples/attribute-amazon-emr-costs-to-your-end-users.git
```

1. Update the following the environment parameters in cdk.context.json (this file can be found in the main directory):
    1. yarn_url – yarn url to read job execution logs and metrics. This url should be accessible within the vpc where lambda would be deployed.
    2. tbl_applicationlogs_lz - RDS temp table to store EMR application execution logs
    3. tbl_applicationlogs - RDS table to store EMR application execution logs
    4. tbl_emrcost - RDS table to capture daily EMR cluster usage cost
    5. tbl_emrinstance_usage - RDS table to store EMR cluster instances usage info
    6. emrcluster_id - EMR cluster instance id
    7. emrcluster_name - EMR cluster name
    8. emrcluster_tag – Tag key assigned to EMR cluster
    9. emrcluster_tag_value – Unique value for EMR cluster tag
    10.emrcluster_role - IAM role assigned to EMR cluster
    11. emrcluster_linkedaccount - Account id under which EMR cluster is running
    12. postgres_rds - Postgres RDS connection detail
    13. athenapostgressecret_id – The name of the Secrets Manager key that stores the RDS Postgres database credentials
    14. vpc_id – VPC id in which EMR cluster is configured and in which cost metering lambda would be deployed.
    15. vpc_subnets – Subnets associated with the VPC
    16. sg_id – Corresponding Security group id.

The following is a sample cdk.context.json file after being populated with the parameters
```
{
  "yarn_url": "<EMR Cluster Yarn URL>",
  "tbl_applicationlogs_lz": "public.emr_applications_execution_log_lz",
  "tbl_applicationlogs": "public.emr_applications_execution_log",
  "tbl_emrcost": "public.emr_cluster_usage_cost",
  "tbl_emrinstance_usage": "public.emr_cluster_instances_usage",
  "emrcluster_id": "<EMR Cluster ID>",
  "emrcluster_name": "<EMR Cluster Name>",
  "emrcluster_tag": "<EMR Cluster Tag>",
  "emrcluster_tag_value": "<EMR Cluster Unique Tag Value>",
  "emrcluster_role": "<EMR Cluster Service Role>",
  "emrcluster_linkedaccount": "<EMR Cluster Linked Account>",
  "postgres_rds": {
    "host": "<RDS Postgres host name>",
    "dbname": "<RDS Postgres Database Name>",
    "user": "<RDS Postgres Login UserName>",
    "secretid": "<AWS secret id for RDS Postgres Login password>"
  }
}

```
#   Instructions needed to setup cloud 9 at least some pointers to create Enviornment

1. Go to AWS Cloud9 and upload the project folder via File→Upload Local Files.
2. Deploy the AWS CDK stack with the following code:
```
cd attribute-amazon-emr-costs-to-your-end-users/
pip install -r requirements.txt
cdk deploy --all
```
