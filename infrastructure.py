import boto3 
import os
import json
import zipfile
from dotenv import load_dotenv
from botocore.client import BaseClient



# creating a s3 bucket 

def createBucket(client_name:BaseClient, bucket_name:str):
    try:
        client_name.create_bucket(ACL ='private', # access of the bucket 
                                Bucket=bucket_name, # bucket name 
                                CreateBucketConfiguration ={'LocationConstraint': 'eu-north-1'}, #region where bucket is created
                                ObjectOwnership='BucketOwnerEnforced' #ownership of bucket
                                 ) 
        print(f'The bucket {bucket_name} is created')
    except Exception as e:
        print(f'error occured while creating the bucket:{e}')


# creation of glue database

def createGlueDatabase(client_name:BaseClient, database_name:str):
    try:
        client_name.create_database(DatabaseInput={
            'Name':database_name # database name
        })
        print(f'database {database_name} is created successfully')
    except Exception as e:
        print(f'Error occured in creation of database:{e}')

# creation of a role

def createIamRole(client_name:BaseClient, role_name:str, role_policy_document:dict): # creating a role 
    try:
        client_name.create_role(
            RoleName = role_name, #role name
            AssumeRolePolicyDocument=json.dumps(role_policy_document) #policy attached to role (only take json)
        )
        print(f'role {role_name} is created successfully')
    except Exception as e:
        print(f'error occured when creating a role: {e}')



# attaching a policy to the role

def attachRolePolicies(client_name:BaseClient, role_name:str, policy_arn: str): # attachng policies 
    try:
        client_name.attach_role_policy(
            RoleName = role_name, # role name
            PolicyArn = policy_arn # policy need to attach to role
        )
        print(f'policy is successfully attached to {role_name}')
    except Exception as e:
        print(f'error occured when attaching the policy: {e}')



# creation of a glue crawler


def createGlueCrawler(client_name:BaseClient, crawler_name:str, role_name:str, database_name:str, s3_path:str):
    try:
        client_name.create_crawler(
                                    Name=crawler_name, # crawler name
                                    Role = role_name, # role attach to crawler
                                    DatabaseName = database_name, # database to store catalog
                                    Targets={
                                        'S3Targets':[{
                                            'Path': s3_path, # target s3 bucket path to crawl
                                    }]
                                    },
                                     RecrawlPolicy={
                                                    'RecrawlBehavior': 'CRAWL_EVERYTHING' # behavior of crawler
                                                    }
                                    )
        print(f'{crawler_name} is succussfully created')
    except Exception as e:
        print(f'error occured in creating a crawler: {e}')


# execution of a glue crawler 

def startCrawler(client_name:BaseClient, crawler_name:str):
    try:
        client_name.start_crawler(Name= crawler_name) #crawler name 
        print(f'{crawler_name} is successfully started')
    except Exception as e:
        print(f'Error occured while running the crawler: {e}')


# status of glue crawler 

def statusCrawler(client_name:BaseClient, crawler_name:str):
    try:
        response=client_name.get_crawler(Name= crawler_name) # crealer name 
        print(f'The glue crawler status :{response['Crawler']['LastCrawl']['Status']}')
        print(f'The glue crawler catalog are store in :{response['Crawler']['LastCrawl']['LogStream']}')
    except Exception as e:
        print(f'The following error occured when creating the glue catalog: {e}')


# create a lambda function

def createLambdaFunction(client_name:BaseClient, function_name:str, run_time:str ,role_arn:str, lambda_handler, code_file):
    try:
        client_name.create_function(
            FunctionName = function_name, # function name
            Runtime = run_time, # kernal of lambda 
            Role = role_arn, # role attached to lambda
            Handler = lambda_handler, # handler attach to lambda
            Code = {
                'ZipFile' : code_file # code need to update in lambda
            }
        )
        print(f'lambda function {function_name} is created successfully')
    except Exception as e:
        print(f'Error occured when creating a function: {e}')


# running the lambda function

def lambdaFuncInvoke(client_name:BaseClient, function_name:str, payload_type:dict):
    try: 
        response = client_name.invoke(
            FunctionName = function_name, # function name
            InvocationType = 'RequestResponse', # type of invocation
            Payload = json.dumps(payload_type) # 
        )
        result = json.loads(response['Payload'].read())
        print("the function is succussfully executed")
        print(f"This are the results {result}")
    except Exception as e:
        print(f"The following error occured when excuting the lambda function: {e}")


# automatically triggering the lambda function

def addPermission(client_name:BaseClient, function_name:str, statement_id:str, principal_name:str, source_arn:str):
    try:
        client_name.add_permission(
        FunctionName=function_name,
        StatementId= statement_id,
        Action='lambda:InvokeFunction',
        Principal = principal_name,
        SourceArn = source_arn
    )
        print(f"Permission to {statement_id} for {function_name} is added successfully")
    except Exception as e:
        print(f"The following error is occurred when adding permission: {e}")


def putBucketNotificationConfiguration(client_name:BaseClient, bucket_name:str, notification_configurations:dict):
    try:
        client_name.put_bucket_notification_configuration(
        Bucket=bucket_name,
        NotificationConfiguration=notification_configurations,
        SkipDestinationValidation=True 
    )
        print(f"The triggers are added succesfully")
    except Exception as e:
        print(f"The following error occurred : {e}")



def createJob(client_name:BaseClient, job_name:str, role_assigned:str,  script_name:str, script_location:str):
    try:
        client_name.create_job(
            Name=job_name,
            Role=role_assigned,
            Command={
                'Name':script_name,
                'ScriptLocation':script_location,
            },
            GlueVersion='5.0',
            NumberOfWorkers=10,
            WorkerType='G.1X'
        )
        print(f"The ETL Job {job_name} is succesfully created")
    except Exception as e:
       print(f"The foloowing error was occured when creating the job: {e}")


def startjob(client_name:BaseClient, job_name:str):
    try:
        client_name.start_job_run(
            JobName = job_name
        )
        print(f"The job {job_name} is successfully started")
    except Exception as e:
        print(f"The following error occuredd when starting the job: {e}")
