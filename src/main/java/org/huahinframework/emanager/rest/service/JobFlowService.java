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
package org.huahinframework.emanager.rest.service;

import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.wink.client.Resource;
import org.apache.wink.client.RestClient;
import org.huahinframework.emanager.amazonaws.elasticmapreduce.Config;
import org.huahinframework.emanager.amazonaws.elasticmapreduce.EMRProperties;
import org.huahinframework.emanager.queue.QueueUtils;
import org.huahinframework.emanager.rest.response.Response;
import org.huahinframework.emanager.util.JobUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;

/**
 *
 */
@Path("/jobflow")
public class JobFlowService {
    private static final Log log = LogFactory.getLog(JobFlowService.class);

    private static final String JOB_FLOW = "JOB_FLOW";
    private static final String STEP_NAME = "STEP_NAME";

    private static final String RUNNING_PATH = "http://%s:9010/job/list/running";
    private static final String KILL_PATH = "http://%s:9010/job/kill/id/%s";

    private static final String STATUS_KILLED = "Killed job %s";

    private AmazonElasticMapReduce emr;
    private EMRProperties emrProperties;

    @Path("/list")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public JSONArray list() throws JSONException {
        return JobUtils.listJobFlow(emr);
    }

    @Path("/runnings")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public JSONArray runnings() throws JSONException {
        return JobUtils.runningsJobFlow(emr);
    }

    @Path("/describe/{" + JOB_FLOW + "}")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public JSONObject describe(@PathParam(JOB_FLOW) String jobFlow)
            throws JSONException {
        return JobUtils.getJobFlow(jobFlow, emr);
    }

    @Path("/kill/{" + STEP_NAME + "}")
    @DELETE
    @Produces(MediaType.APPLICATION_JSON)
    public JSONObject kill(@PathParam(STEP_NAME) String stepName)
            throws JSONException {
        Map<String, String> status = new HashMap<String, String>();
        status.put(Response.STATUS, stepName + " killed");

        try {
            Config config = QueueUtils.get(stepName);

            String masterPublicDnsName = config.getMasterPublicDnsName();
            if (config.getStatus() != Config.JOB_STATUS_RUNNING ||
                isEmpty(masterPublicDnsName)) {
                status.put(Response.STATUS, stepName + " not running");
                return new JSONObject(status);
            }

            Resource resource = null;

            RestClient client = new RestClient();
            resource = client.resource(String.format(RUNNING_PATH, masterPublicDnsName));
            JSONArray runnings = resource.get(JSONArray.class);
            if (runnings.length() != 1) {
                status.put(Response.STATUS, stepName + " not running");
                return new JSONObject(status);
            }

            String jobId = runnings.getJSONObject(0).getString("jobid");
            resource = client.resource(String.format(KILL_PATH, masterPublicDnsName, jobId));
            JSONObject killStatuses = resource.delete(JSONObject.class);
            String killStatus = killStatuses.getString("status");
            if (isEmpty(killStatus) || !killStatus.equals(String.format(STATUS_KILLED, jobId))) {
                status.put(Response.STATUS, "kill failed");
                return new JSONObject(status);
            }

            config.setStatus(Config.JOB_STATUS_KILLED);
            QueueUtils.updateQueue(config);
        } catch (Exception e) {
            log.error(e);
            status.put(Response.STATUS, e.getMessage());
        }

        return new JSONObject(status);
    }

    /**
     *
     */
    public void init() {
        emr = new AmazonElasticMapReduceClient(
                new BasicAWSCredentials(emrProperties.getAccessKey(),
                                        emrProperties.getSecretKey()));
        if (!isEmpty(emrProperties.getEndpoint())) {
            emr.setEndpoint(emrProperties.getEndpoint());
        }
    }

    /**
     *
     */
    private boolean isEmpty(String s) {
        return s == null || s.isEmpty();
    }

    /**
     * @param emrProperties the emrProperties to set
     */
    public void setEmrProperties(EMRProperties emrProperties) {
        this.emrProperties = emrProperties;
    }
}
