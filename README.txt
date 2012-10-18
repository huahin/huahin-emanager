Huahin EManager

Huahin EManager is a Simple Management System for Amazon Elasctic MapReduce.
Huahin EManager can get a list of Job Flow, describe Job Flow, do a kill for the step, and Job queue management.
Huahin EManager is optimized to reduce the cost and start the instance.

Huahin EManager is distributed under Apache License 2.0.


-----------------------------------------------------------------------------
Documentation
  http://huahinframework.org/huahin-emanager/

-----------------------------------------------------------------------------
Requirements
  * Java 6+

-----------------------------------------------------------------------------
Install Huahin EManager
  ~ $ tar xzf huahin-emanager-x.x.x.tar.gz

-----------------------------------------------------------------------------
Setup of AWS Security Group

Set the your IP address and port(9010) for the Huahin Manager to the Security Group(ElasticMapReduce-master). For example:

  Port range: 9010
  Source: 1.1.1.1/32

-----------------------------------------------------------------------------
Configure Huahin EManager

Edit the huahin-emanager-x.x.x/conf/huahinEManager.properties file.
For example:

  # Elastitc MapReduce cluster size. The size of the cluster that start at parallel.(required)
  job.clusterSize=1

  # At the time of shutdown of EManager, specify whether it wait for the end of Job.(required)
  terminate.jobForWait=true

  # AWS access key(required)
  emr.accesskey=XXXXXXXXXXXXXXX

  # AWS secret key(required)
  emr.secretkey=YYYYYYYYYYYYYYY

  # Regions and Endpoints.(If not specified, the default of EMR is applied.)
  # (http://docs.amazonwebservices.com/general/latest/gr/rande.html)
  emr.endpoint=elasticmapreduce.us-east-1.amazonaws.com

  # Availability zone.(If not specified, the default of EMR is applied.)
  emr.availabilityzone=us-east-1a

  # Amazon EC2 Key Pair(If not specified, the default of EMR is applied.)
  emr.keypairname=keyname

  # Hadoop version.(If not specified, the default of EMR is applied.)
  emr.hadoopversion=1.0.3

  # Instance count.(required)
  emr.instancecount=2

  # Master Instance Type.(required)
  emr.masterInstancetype=m2.xlarge

  # Slave Instance Type.(If not specified, master instance type is applied.)
  emr.slaveInstancetype=m1.small

  # True means EMR will store an index of your logs (requires an emr.loguri propertie). (required)
  emr.debug=false

  # To copy log files from the job flow to Amazon S3, specify an Amazon S3 bucket.(optional)
  emr.loguri=s3://loguri/

When you change the boot port and shutdown port, edit the huahin-emanager-x.x.x/conf/port and shutdown port file.

-----------------------------------------------------------------------------
Start/Stop Huahin EManager

To start/stop Huahin Manager use Huahin EManager's bin/emanager script. For example:

  $ bin/emanager start

-----------------------------------------------------------------------------
Test Huahin EManager is working

  ~ $ curl -X GET "http://<HOSTNAME>:9020/jobflow/list"
[
  {
    "creationDate": "Tue Oct 16 21:02:16 JST 2012",
    "endDate": "Tue Oct 16 21:10:21 JST 2012",
    "jobFlow": "j-2R5KLHJORAE0W",
    "startDate": "Tue Oct 16 21:06:11 JST 2012",
    "state": "TERMINATED"
  }
]

-----------------------------------------------------------------------------
Huahin EManager REST Job Flow APIs

Get all job flow list.
  For example:
  ~ $ curl -X GET "http://<HOSTNAME>:9020/jobflow/list"

Get running job flow list.
  For example:
  ~ $ curl -X GET "http://<HOSTNAME>:9020/jobflow/runnings"

Get describe job flow.
  <JOB FLOW ID> specifies the job flow ID.
  ~ $ curl -X GET "http://<HOSTNAME>:9020/jobflow/describe/<JOB FLOW ID>"

  For example:
  ~ $ curl -X GET "http://<HOSTNAME>:9020/jobflow/describe/j-XXXXXXXXXX"

Kill step for step name.
  <STEP NAME> specifies the step name.
  ~ $ curl -X DELETE "http://<HOSTNAME>:9020/jobflow/kill/step/<STEP NAME>"

  For example:
  ~ $ curl -X DELETE "http://<HOSTNAME>:9020/jobflow/kill/step/S_XXXXXXXXXXX"

-----------------------------------------------------------------------------
Huahin EManager REST queue APIs

Get all queue list.
  For example:
  ~ $ curl -X GET "http://<HOSTNAME>:9020/queue/list"

Get running queue list.
  For example:
  ~ $ curl -X GET "http://<HOSTNAME>:9020/queue/runnings"

Get describe queue.
  <STEP NAME> specifies the step name.
  ~ $ curl -X GET "http://<HOSTNAME>:9020/queue/describe/<STEP NAME>"

  For example:
  ~ $ curl -X GET "http://<HOSTNAME>:9020/queue/describe/S_XXXXXXXXXXX"

Kill queue for step name.
  <STEP NAME> specifies the step name.
  ~ $ curl -X DELETE "http://<HOSTNAME>:9020/queue/kill/<STEP NAME>"

  For example:
  ~ $ curl -X DELETE "http://<HOSTNAME>:9020/queue/kill/S_XXXXXXXXXXX"

Register job
  Specifies of all ARGUMENTS is JSON.
  PUT method is specifies the scripts or jar on the S3.
  POST method upload the scripts or jar to S3.

  Register Hive and Pig job
    PUT method
      Required ARGUMENTS.
        script

    POST method
      Required SCRIPT.
      Required ARGUMENTS.
        script

    For PUT Hive example:
    ~ $ curl -X PUT http://localhost:9020/queue/register/hive \
        -F ARGUMENTS='{"script":"s3://huahin/wordcount.hql","arguments":["arg1","arg2"]}'

    For PUT Pig example:
    ~ $ curl -X PUT http://localhost:9020/queue/register/pig \
        -F ARGUMENTS='{"script":"s3://huahin/wordcount.pig","arguments":["arg1","arg2"]}'

    For POST Hive example:
    ~ $ curl -X POST http://localhost:9020/queue/register/hive
        -F SCRIPT=@wordcount.hql \
        -F ARGUMENTS='{"script":"s3://huahin/wordcount.hql","arguments":["arg1","arg2"]}'

    For POST Pig example:
    ~ $ curl -X POST http://localhost:9020/queue/register/pig
        -F SCRIPT=@wordcount.pig \
        -F ARGUMENTS='{"script":"s3://huahin/wordcount.pig","arguments":["arg1","arg2"]}'

    If deleteOnExit of arguments is true, the script is deleted after execution.
    For POST Hive example:
      ~ $ curl -X POST http://localhost:9020/queue/register/hive
          -F SCRIPT=@wordcount.hql \
          -F ARGUMENTS='{"script":"s3://huahin/wordcount.hql","arguments":["arg1","arg2"], "deleteOnExit":"true"}'

  Register Streaming job
    PUT method
      Required ARGUMENTS.
        input, output, mapper, reducer

    POST method
      Required MAPPER.
      Required REDUCER.
      Required ARGUMENTS.
        input, output, mapper, reducer

    For PUT example:
      ~ $ curl -X PUT http://localhost:9020/queue/register/streaming \
          -F ARGUMENTS='{"input":"s3://input/","output":"s3://output/","mapper":"s3://huahin/map.py","reducer":"s3://huahin/reduce.py"}'

    For POST example:
      ~ $ curl -X POST http://localhost:9020/queue/register/streaming \
          -F MAPPER=@map.py \
          -F REDUCER=@reduce.py \
          -F ARGUMENTS='{"input":"s3://input/","output":"s3://output/","mapper":"s3://huahin/map.py","reducer":"s3://huahin/reduce.py"}'

    If deleteOnExit of arguments is true, the script is deleted after execution.
    For POST Pig example:
      ~ $ curl -X POST http://localhost:9020/queue/register/streaming \
          -F MAPPER=@map.py \
          -F REDUCER=@reduce.py \
          -F ARGUMENTS='{"input":"s3://input/","output":"s3://output/","mapper":"s3://huahin/map.py","reducer":"s3://huahin/reduce.py", "deleteOnExit":"true"}'

  Register custom jar job
    PUT method
      Required ARGUMENTS.
        jar

    POST method
      Required JAR.
      Required ARGUMENTS.
        jar

    For PUT example:
      ~ $ curl -X PUT http://localhost:9020/queue/register/customjar \
          -F ARGUMENTS='{"jar":"s3://huahin/wordcount.jar","arguments":["s3://huahin/input/","-o","s3://huahin/output/"]}'

    For POST example:
      ~ $ curl -X POST http://localhost:9020/queue/register/customjar \
          -F JAR=@wordcount.jar \
          -F ARGUMENTS='{"jar":"s3://huahin/wordcount.jar","arguments":["s3://huahin/input/","-o","s3://huahin/output/"]}'

    If deleteOnExit of arguments is true, the script is deleted after execution.
    For POST Pig example:
      ~ $ curl -X POST http://localhost:9020/queue/register/customjar \
          -F JAR=@wordcount.jar \
          -F ARGUMENTS='{"jar":"s3://huahin/wordcount.jar","arguments":["s3://huahin/input/","-o","s3://huahin/output/", "deleteOnExit":"true"}'
