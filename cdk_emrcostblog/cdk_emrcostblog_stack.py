from aws_cdk import (
    Duration,
    Stack,
    aws_lambda as _lambda,
    aws_iam as _iam,
    aws_sns as _sns,
    aws_ssm as _ssm,
    aws_ec2 as _ec2
)
import os

from constructs import Construct

class CdkEMRCostStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope,construct_id, **kwargs)
        
        # Form ssm parameter by reading the user input from cdk.context.json file
        ssm_parameter = '{ "yarn_url": "'+ self.node.try_get_context("yarn_url") +'","tbl_applicationlogs_lz": "'+ self.node.try_get_context("tbl_applicationlogs_lz") +'",\
        "tbl_applicationlogs": "'+self.node.try_get_context("tbl_applicationlogs")+'", \
        "tbl_emrcost": "'+self.node.try_get_context("tbl_emrcost")+'", \
        "tbl_emrinstance_usage": "'+self.node.try_get_context("tbl_emrinstance_usage")+'", \
        "emrcluster_id": "'+self.node.try_get_context("emrcluster_id")+'", \
        "emrcluster_name": "'+self.node.try_get_context("emrcluster_name")+'", \
        "emrcluster_role": "'+self.node.try_get_context("emrcluster_role")+'", \
        "emrcluster_linkedaccount": "'+self.node.try_get_context("emrcluster_linkedaccount")+'", \
        "postgres_rds": '+str(self.node.try_get_context("postgres_rds"))+'}'
        
        ssm_parameter = ssm_parameter.replace("'",'"')
        
        # Create SSM parameter
        ssm_parameter = _ssm.StringParameter(
            self, 'MySSMParameter',
            parameter_name='cost_metering_parameters',
            string_value=ssm_parameter,
            description='API Key for my application'
            )
            
        # Create a new IAM role to be used by lambda
        EMRCostMeasureCaptureRole = _iam.Role(
            self,
            "EMRCostMeasureRole",
            assumed_by=_iam.ServicePrincipal("lambda.amazonaws.com"),
            role_name="EMRCostMeasureCaptureRole"
            )

        # Create policy to allow access to Cost Explorer
        cost_explorer_policy = _iam.PolicyStatement(
            actions=[
                "ce:*",
            ],
            resources=["*"],
            )

        EMRCostMeasureCaptureRole.add_to_policy(cost_explorer_policy)
        
        #Create policy to allow access on SSM parameter store
        ssm_parameter_policy = _iam.PolicyStatement(
            actions=[
                "ssm:Describe*",
                "ssm:Get*",
                "ssm:List*"
            ],
            resources=["*"],
            )
        EMRCostMeasureCaptureRole.add_to_policy(ssm_parameter_policy)
        
        #Create policy to allow access on EMR Clsuter
        emr_policy = _iam.PolicyStatement(
            actions=[
                "elasticmapreduce:Describe*",
                "elasticmapreduce:List*",

            ],
            resources=["*"],
            )
        EMRCostMeasureCaptureRole.add_to_policy(emr_policy)
        
        #Create policy to allow vpc access
        vpc_policy = _iam.PolicyStatement(
            actions=[
                "ec2:DescribeNetworkInterfaces",
                "ec2:CreateNetworkInterface",
                "ec2:DeleteNetworkInterface",
                "ec2:DescribeInstances",
                "ec2:AttachNetworkInterface"
            ],
            resources=["*"],
            )
        EMRCostMeasureCaptureRole.add_to_policy(vpc_policy)

        #Create policy for cloudwatch loggroup
        cw_policy = _iam.PolicyStatement(
            actions=[
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents",
                "logs:DescribeLogStreams"
            ],
            resources=["arn:aws:logs:*:*:*"],
            )
        EMRCostMeasureCaptureRole.add_to_policy(cw_policy)

        #Create policy for secretsmanager access
        secretsmanager_policy = _iam.PolicyStatement(
            actions=[
                "secretsmanager:GetSecretValue"
            ],
            resources=["arn:aws:secretsmanager:*:*:*"],
            )
        EMRCostMeasureCaptureRole.add_to_policy(secretsmanager_policy)
        
        # Here define a Lambda Layer 
        '''requests_layers = _alp.PythonLayerVersion(
               self, 'RequestsLambdaLayer',
               entry='Lambda/layers',
               compatible_runtimes=[_lambda.Runtime.PYTHON_3_8],
               description='requests Library',
               layer_version_name='1.0'
           )'''
        
        # Create a layer version for requests
        #requests_layers = _lambda.LayerVersion(
        #    self, 'RequestsLambdaLayer',
        #    code=_lambda.Code.from_asset('Lambda/layers/requests'),
        #    compatible_runtimes=[_lambda.Runtime.PYTHON_3_8],
        #    description='requests Library'
        #)
        
        # Create a layer version
        #psycopg2_layers = _lambda.LayerVersion(
        #    self, 'Psycopg2LambdaLayer',
        #    code=_lambda.Code.from_asset('Lambda/layers/psycopg2'),
        #    compatible_runtimes=[_lambda.Runtime.PYTHON_3_8],
        #    description='psycopg2 Library'
        #)
        
        #VPC details for lambda
        vpc_id = self.node.try_get_context("vpc_id") #"vpc-0ff0f63e4bf34a784"
        #subnet_ids = ["subnet-14c6437f"] #self.node.try_get_context("vpc_subnets")
        subnet_ids = {self.node.try_get_context("vpc_subnets")}
        print('========subnet_ids=====',subnet_ids)
        
        sg_id = self.node.try_get_context("sg_id") #"sg-00040d84b09508cae"
        
        subnets = list()
        for subnet_id in subnet_ids:
            subnets.append(_ec2.Subnet.from_subnet_attributes(self, subnet_id.replace("-", "").replace("_", "").replace(" ", ""), subnet_id=subnet_id))
        
        vpc_subnets = _ec2.SubnetSelection(subnets=subnets)
        sg = _ec2.SecurityGroup.from_security_group_id(self, "MySG", sg_id)
        
        
        my_lambda = _lambda.Function(
            self, 'EMRCostMeasure',
            runtime=_lambda.Runtime.PYTHON_3_8,
            #layers=[requests_layers,psycopg2_layers],
            code=_lambda.Code.from_asset('Lambda'),
            handler='lambda_function.lambda_handler',
            role=EMRCostMeasureCaptureRole,
            vpc=_ec2.Vpc.from_lookup(self, "MyVpc", vpc_id=vpc_id),
            vpc_subnets=vpc_subnets,
            timeout=Duration.minutes(5),
            #security_groups=[sg]
            #name="l_emr_cost_measure_capture"
            allow_public_subnet=True
            )
