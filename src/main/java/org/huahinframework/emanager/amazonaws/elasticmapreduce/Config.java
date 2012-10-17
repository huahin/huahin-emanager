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
package org.huahinframework.emanager.amazonaws.elasticmapreduce;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class Config implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final int JOB_TYPE_HIVE = 1;
    public static final int JOB_TYPE_CUSTOM_JAR = 2;
    public static final int JOB_TYPE_STREAMING = 3;
    public static final int JOB_TYPE_PIG = 4;

    public static final int JOB_STATUS_PENDING = 1;
    public static final int JOB_STATUS_RUNNING = 2;
    public static final int JOB_STATUS_KILL = 3;
    public static final int JOB_STATUS_KILLED = 4;
    public static final int JOB_STATUS_ERROR = 5;
    public static final int JOB_STATUS_COMPLETE = 6;
    public static final int JOB_STATUS_CANCEL = 7;

    private int jobType;
    private Date date;
    private String name;
    private String run;
    private String mainClass;
    private String[] args;
    private Map<String, String> argMap;
    private int status;
    private String jobFlowId;
    private String masterPublicDnsName;
    private boolean deleteOnExit;

    private static Map<Integer, String> jobTypeMap = new HashMap<Integer, String>();
    private static Map<Integer, String> statusMap = new HashMap<Integer, String>();

    static {
        jobTypeMap.put(JOB_TYPE_HIVE, "Hive");
        jobTypeMap.put(JOB_TYPE_CUSTOM_JAR, "Custom Jar");
        jobTypeMap.put(JOB_TYPE_STREAMING, "Streaming");
        jobTypeMap.put(JOB_TYPE_PIG, "Pig");

        statusMap.put(JOB_STATUS_PENDING, "pending");
        statusMap.put(JOB_STATUS_RUNNING, "running");
        statusMap.put(JOB_STATUS_KILL, "kill");
        statusMap.put(JOB_STATUS_KILLED, "killed");
        statusMap.put(JOB_STATUS_ERROR, "error");
        statusMap.put(JOB_STATUS_COMPLETE, "complete");
        statusMap.put(JOB_STATUS_CANCEL, "cancel");
    }

    /**
     * @return the jobType
     */
    public int getJobType() {
        return jobType;
    }

    /**
     * @param jobType the jobType to set
     */
    public void setJobType(int jobType) {
        this.jobType = jobType;
    }

    /**
     * @return the date
     */
    public Date getDate() {
        return date;
    }

    /**
     * @param date the date to set
     */
    public void setDate(Date date) {
        this.date = date;
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return the run
     */
    public String getRun() {
        return run;
    }

    /**
     * @param run the run to set
     */
    public void setRun(String run) {
        this.run = run;
    }

    /**
     * @return the mainClass
     */
    public String getMainClass() {
        return mainClass;
    }

    /**
     * @param mainClass the mainClass to set
     */
    public void setMainClass(String mainClass) {
        this.mainClass = mainClass;
    }

    /**
     * @return the args
     */
    public String[] getArgs() {
        return args;
    }

    /**
     * @param args the args to set
     */
    public void setArgs(String[] args) {
        this.args = args;
    }

    /**
     * @return the argMap
     */
    public Map<String, String> getArgMap() {
        return argMap;
    }

    /**
     * @param argMap the argMap to set
     */
    public void setArgMap(Map<String, String> argMap) {
        this.argMap = argMap;
    }

    /**
     * @return the status
     */
    public int getStatus() {
        return status;
    }

    /**
     * @param status the status to set
     */
    public void setStatus(int status) {
        this.status = status;
    }

    /**
     * @return the jobFlowId
     */
    public String getJobFlowId() {
        return jobFlowId;
    }

    /**
     * @param jobFlowId the jobFlowId to set
     */
    public void setJobFlowId(String jobFlowId) {
        this.jobFlowId = jobFlowId;
    }

    /**
     * @return the masterPublicDnsName
     */
    public String getMasterPublicDnsName() {
        return masterPublicDnsName;
    }

    /**
     * @param masterPublicDnsName the masterPublicDnsName to set
     */
    public void setMasterPublicDnsName(String masterPublicDnsName) {
        this.masterPublicDnsName = masterPublicDnsName;
    }

    /**
     * @return the deleteOnExit
     */
    public boolean isDeleteOnExit() {
        return deleteOnExit;
    }

    /**
     * @param deleteOnExit the deleteOnExit to set
     */
    public void setDeleteOnExit(boolean deleteOnExit) {
        this.deleteOnExit = deleteOnExit;
    }

    /**
     * @param type
     * @return job type
     */
    public static String getJobTypeName(int type) {
        String jobType = jobTypeMap.get(type);
        if (jobType == null) {
            return "";
        }

        return jobType;
    }

    /**
     * @param status
     * @return job status
     */
    public static String getStatusName(int status) {
        String jobStatus = statusMap.get(status);
        if (jobStatus == null) {
            return "";
        }

        return jobStatus;
    }
}
