/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.huahinframework.emanager.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.huahinframework.emanager.rest.response.Response;
import org.json.JSONArray;
import org.json.JSONObject;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.model.BootstrapActionConfig;
import com.amazonaws.services.elasticmapreduce.model.BootstrapActionDetail;
import com.amazonaws.services.elasticmapreduce.model.DescribeJobFlowsRequest;
import com.amazonaws.services.elasticmapreduce.model.DescribeJobFlowsResult;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowDetail;
import com.amazonaws.services.elasticmapreduce.model.JobFlowExecutionStatusDetail;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesDetail;
import com.amazonaws.services.elasticmapreduce.model.ScriptBootstrapActionConfig;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.elasticmapreduce.model.StepDetail;
import com.amazonaws.services.elasticmapreduce.model.StepExecutionStatusDetail;

/**
 *
 */
public class JobUtils {
    public static final String JOB_FLOW_STATUS_STARTING = "STARTING";
    public static final String JOB_FLOW_STATUS_RUNNING = "RUNNING";
    public static final String JOB_FLOW_STATUS_WAITING = "WAITING";
    public static final String JOB_FLOW_STATUS_SHUTTING_DOWN = "SHUTTING_DOWN";
    public static final String JOB_FLOW_STATUS_COMPLETED = "COMPLETED";
    public static final String JOB_FLOW_STATUS_FAILED = "FAILED";
    public static final String JOB_FLOW_STATUS_TERMINATED = "TERMINATED";

    public static final String STEP_STATUS_PENDING = "PENDING";
    public static final String STEP_STATUS_RUNNING = "RUNNING";
    public static final String STEP_STATUS_COMPLETED = "COMPLETED";
    public static final String STEP_STATUS_CANCELLED = "CANCELLED";
    public static final String STEP_STATUS_FAILED = "FAILED";

    public static JSONArray listJobFlow(AmazonElasticMapReduce emr) {
        List<JSONObject> l = new ArrayList<JSONObject>();

        DescribeJobFlowsResult describeJobFlowsResult =
                emr.describeJobFlows(new DescribeJobFlowsRequest());
        for (JobFlowDetail jobFlowDetail : describeJobFlowsResult.getJobFlows()) {
            JobFlowExecutionStatusDetail executionStatusDetail =
                    jobFlowDetail.getExecutionStatusDetail();
            Map<String, String> m = new HashMap<String, String>();
            m.put(Response.JOB_FLOW, jobFlowDetail.getJobFlowId());
            m.put(Response.STATE, executionStatusDetail.getState());
            m.put(Response.CREATION_DATE, executionStatusDetail.getCreationDateTime().toString());
            m.put(Response.START_DATE,
                  object2String(executionStatusDetail.getStartDateTime(), true));
            m.put(Response.END_DATE,
                  object2String(executionStatusDetail.getEndDateTime(), true));
            l.add(new JSONObject(m));
        }

        return new JSONArray(l);
    }

    public static JSONArray runningsJobFlow(AmazonElasticMapReduce emr) {
        List<JSONObject> l = new ArrayList<JSONObject>();

        DescribeJobFlowsRequest describeJobFlowsRequest =
                new DescribeJobFlowsRequest()
                        .withJobFlowStates(JOB_FLOW_STATUS_STARTING,
                                           JOB_FLOW_STATUS_RUNNING,
                                           JOB_FLOW_STATUS_WAITING);
        DescribeJobFlowsResult describeJobFlowsResult =
                emr.describeJobFlows(describeJobFlowsRequest);
        for (JobFlowDetail jobFlowDetail : describeJobFlowsResult.getJobFlows()) {
            JobFlowExecutionStatusDetail executionStatusDetail =
                    jobFlowDetail.getExecutionStatusDetail();
            Map<String, Object> m = new HashMap<String, Object>();
            m.put(Response.JOB_FLOW, jobFlowDetail.getJobFlowId());
            m.put(Response.STATE, executionStatusDetail.getState());
            m.put(Response.CREATION_DATE, executionStatusDetail.getCreationDateTime().toString());
            m.put(Response.START_DATE,
                  object2String(executionStatusDetail.getStartDateTime(), true));
            m.put(Response.END_DATE,
                  object2String(executionStatusDetail.getEndDateTime(), true));

            if (!isEmpty(jobFlowDetail.getSteps())) {
                List<Object> ll = new ArrayList<Object>();
                for (StepDetail sd : jobFlowDetail.getSteps()) {
                    Map<String, Object> mm = new HashMap<String, Object>();
                    StepConfig sc = sd.getStepConfig();
                    StepExecutionStatusDetail sesd = sd.getExecutionStatusDetail();

                    mm.put(Response.NAME, sc.getName());
                    mm.put(Response.ACTION_ON_FAILURE, sc.getActionOnFailure());
                    mm.put(Response.STATE, object2String(sesd.getState(), false));
                    mm.put(Response.CREATION_DATE, object2String(sesd.getCreationDateTime(), true));
                    mm.put(Response.START_DATE, object2String(sesd.getStartDateTime(), true));
                    mm.put(Response.END_DATE, object2String(sesd.getEndDateTime(), true));

                    HadoopJarStepConfig hjsc = sc.getHadoopJarStep();
                    mm.put(Response.JAR, object2String(hjsc.getJar(), false));
                    mm.put(Response.MAIN_CLASS, object2String(hjsc.getMainClass(), false));

                    if (!isEmpty(hjsc.getArgs())) {
                        mm.put(Response.ARGS, hjsc.getArgs());
                    }

                    ll.add(mm);
                }
                m.put(Response.STEPS, ll);
            }

            l.add(new JSONObject(m));
        }

        return new JSONArray(l);
    }

    public static JSONObject getJobFlow(String jobFlow, AmazonElasticMapReduce emr) {
        DescribeJobFlowsRequest describeJobFlowsRequest =
                new DescribeJobFlowsRequest().withJobFlowIds(jobFlow);
        DescribeJobFlowsResult describeJobFlowsResult =
                emr.describeJobFlows(describeJobFlowsRequest);
        if (describeJobFlowsResult.getJobFlows().size() != 1) {
            return new JSONObject();
        }

        JobFlowDetail jobFlowDetail =
                describeJobFlowsResult.getJobFlows().get(0);
        JobFlowExecutionStatusDetail executionStatusDetail =
                jobFlowDetail.getExecutionStatusDetail();
        JobFlowInstancesDetail instancesDetail =
                jobFlowDetail.getInstances();

        Map<String, Object> m = new HashMap<String, Object>();
        m.put(Response.JOB_FLOW, jobFlowDetail.getJobFlowId());
        m.put(Response.STATE, executionStatusDetail.getState());
        m.put(Response.CREATION_DATE, executionStatusDetail.getCreationDateTime().toString());
        m.put(Response.START_DATE,
              object2String(executionStatusDetail.getStartDateTime(), true));
        m.put(Response.END_DATE,
              object2String(executionStatusDetail.getEndDateTime(), true));
        m.put(Response.AMI_VERSION, object2String(jobFlowDetail.getAmiVersion(), false));
        m.put(Response.NAME, jobFlowDetail.getName());
        m.put(Response.LOG_URI, object2String(jobFlowDetail.getLogUri(), false));

        if (!isEmpty(jobFlowDetail.getSupportedProducts())) {
            m.put(Response.SUPPORTED_PRODUCTS, jobFlowDetail.getSupportedProducts());
        }

        m.put(Response.EC2_KEY_NAME, object2String(instancesDetail.getEc2KeyName(), false));
        m.put(Response.EC2_SUBNET_ID, object2String(instancesDetail.getEc2SubnetId(), false));
        m.put(Response.HADOOP_VERSION, object2String(instancesDetail.getHadoopVersion(), false));
        m.put(Response.INSTANCE_COUNT, integer2String(instancesDetail.getInstanceCount()));
        m.put(Response.KEEP_JOB_FLOW_ALIVE_WHEN_NO_STEPS,
              object2String(instancesDetail.getKeepJobFlowAliveWhenNoSteps(), true));
        m.put(Response.MASTER_INSTANCE_ID, object2String(instancesDetail.getMasterInstanceId(), false));
        m.put(Response.MASTER_INSTANCE_TYPE, object2String(instancesDetail.getMasterInstanceType(), false));
        m.put(Response.MASTER_PUBLIC_DNS_NAME, object2String(instancesDetail.getMasterPublicDnsName(), false));
        m.put(Response.AVAILABILITY_ZONE, object2String(instancesDetail.getPlacement().getAvailabilityZone(), false));
        m.put(Response.SLAVE_INSTANCE_TYPE, object2String(instancesDetail.getSlaveInstanceType(), false));

        if (!isEmpty(jobFlowDetail.getBootstrapActions())) {
            List<Object> l = new ArrayList<Object>();
            for (BootstrapActionDetail bad : jobFlowDetail.getBootstrapActions()) {
                Map<String, Object> mm = new HashMap<String, Object>();

                BootstrapActionConfig bac = bad.getBootstrapActionConfig();
                ScriptBootstrapActionConfig sbac = bac.getScriptBootstrapAction();

                mm.put(Response.NAME, object2String(bac.getName(), false));
                mm.put(Response.PATH, object2String(sbac.getPath(), false));

                if (!isEmpty(sbac.getArgs())) {
                    mm.put(Response.ARGS, sbac.getArgs());
                }
                l.add(mm);
            }
            m.put(Response.BOOTSTRAP_ACTIONS, l);
        }

        if (!isEmpty(jobFlowDetail.getSteps())) {
            List<Object> l = new ArrayList<Object>();
            for (StepDetail sd : jobFlowDetail.getSteps()) {
                Map<String, Object> mm = new HashMap<String, Object>();
                StepConfig sc = sd.getStepConfig();
                StepExecutionStatusDetail sesd = sd.getExecutionStatusDetail();

                mm.put(Response.NAME, sc.getName());
                mm.put(Response.ACTION_ON_FAILURE, sc.getActionOnFailure());
                mm.put(Response.STATE, object2String(sesd.getState(), false));
                mm.put(Response.CREATION_DATE, object2String(sesd.getCreationDateTime(), true));
                mm.put(Response.START_DATE, object2String(sesd.getStartDateTime(), true));
                mm.put(Response.END_DATE, object2String(sesd.getEndDateTime(), true));

                HadoopJarStepConfig hjsc = sc.getHadoopJarStep();
                mm.put(Response.JAR, object2String(hjsc.getJar(), false));
                mm.put(Response.MAIN_CLASS, object2String(hjsc.getMainClass(), false));

                if (!isEmpty(hjsc.getArgs())) {
                    mm.put(Response.ARGS, hjsc.getArgs());
                }

                l.add(mm);
            }
            m.put(Response.STEPS, l);
        }

        return new JSONObject(m);
    }

    private static String object2String(Object o, boolean toString) {
        String s = "";
        if (o != null) {
            if (toString) {
                s = o.toString();
            } else {
                s = (String) o;
            }
        }
        return s;
    }

    private static String integer2String(Integer o) {
        String s = "";
        if (o != null) {
            s = String.valueOf(o);
        }
        return s;
    }

    private static boolean isEmpty(List<?> l) {
        return l == null || l.isEmpty();
    }
}
