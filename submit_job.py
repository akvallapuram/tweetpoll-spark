import boto3
import json
import sys
import traceback

def lambda_handler(event, context):
    try:
        conn = boto3.client("emr")
        
        # chooses the first cluster which is Running or Waiting
        # possibly can also choose by name or already have the cluster id
        clusters = conn.list_clusters()
        
        # choose the correct cluster
        clusters = [c["Id"] for c in clusters["Clusters"]
                    if c["Status"]["State"] in ["RUNNING", "WAITING"]]
        
        if not clusters:
            sys.stderr.write("No valid clusters\n")
            sys.stderr.exit()
            return {
                'statusCode': 500,
                'body': json.dumps("No valid clusters")
            }

        # take the first relevant cluster
        cluster_id = clusters[0]
        
        # dataset to perform Spark Job on
        fileName = event['Records'][0]['s3']['object']['key']
        fileLoc = "s3://tweetpoll-cloud-tweetsbucket-w5amsqozsezb/"

        # spark configuration
        step_args = ["/usr/bin/spark-submit", "--deploy-mode", "cluster",
                     "s3://tweetpoll-backend/tweetSentiment.py", 
                     fileLoc + fileName # get file from Twitter datastream S3 bucket
                    ]

        step = {
                'Name': 'Analyse ' + fileLoc + fileName,
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 's3n://elasticmapreduce/libs/script-runner/script-runner.jar',
                    'Args': step_args
                }
            }
        
        action = conn.add_job_flow_steps(JobFlowId=cluster_id, Steps=[step])
        print("Added step: %s"%(action))
        return {
            'statusCode': 200,
            'body': json.dumps("Added step: %s"%(action))
        }
    
    except:
        print(traceback.format_exc())
        return {
            'statusCode': 404,
            'body': json.dumps("Spark Job Submission unsuccessful\n" + traceback.format_exc())
        }