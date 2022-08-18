import json, boto3

emr = boto3.client("emr", region_name="us-east-2")

def lambda_handler(event, context):
    fileName = event['Records'][0]['s3']['object']['key'] # name of your raw files
    bucketName = event['Records'][0]['s3']['bucket']['name'] # name of your raw file bucket
    
    print(f"file name: {fileName}")
    print(f"bucket name: {bucketName}")
    
    sparkApp = "s3://air-pollution-spark-jobs/sparkApp.py"  
    
    spark_submit = [
    'spark-submit',
    '--master', 'yarn',
    '--deploy-mode', 'cluster',
    sparkApp,
    bucketName,
    fileName
    ]
    
    print(f"Spark Submit: {spark_submit}")
    
    cluster_id = emr.run_job_flow(Name="spark_job_cluster",
                                    Instances={'InstanceGroups': [{
                                                                    'Name': "Master",
                                                                    'Market': 'ON_DEMAND',
                                                                    'InstanceRole': 'MASTER',
                                                                    'InstanceType': 'm4.large',
                                                                    'InstanceCount': 1,
                                                                    },
                                                                    {
                                                                    'Name': "Slave",
                                                                    'Market': 'ON_DEMAND',
                                                                    'InstanceRole': 'CORE',
                                                                    'InstanceType': 'm4.large',
                                                                    'InstanceCount': 2,
                                                                    }
                                                                ],
                                                'KeepJobFlowAliveWhenNoSteps': False,
                                                'TerminationProtected': False,
                                                'Ec2SubnetId': 'subnet-08ed89f88ce25d50f',
                                                },
                                    LogUri="s3://aws-logs-864212374478-us-east-2/air-pollution-logs/",
                                    ReleaseLabel= 'emr-5.36.0',
                                    Steps=[{"Name": "Spark",
                                            'ActionOnFailure': 'TERMINATE_CLUSTER',
                                            'HadoopJarStep': {
                                            'Jar': 'command-runner.jar',
                                            'Args': spark_submit
                                            }
                                            }],
                                    BootstrapActions=[], 
                                    VisibleToAllUsers=True,
                                    JobFlowRole="EMR_EC2_DefaultRole",
                                    ServiceRole="EMR_DefaultRole",
                                    Applications = [{'Name': 'Spark'}, {'Name':'Hive'}])
