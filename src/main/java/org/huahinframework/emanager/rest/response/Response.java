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
package org.huahinframework.emanager.rest.response;

/**
 *
 */
public class Response {
    public static final String STATUS = "status";

    public static final String JOB_FLOW = "jobFlow";
    public static final String STATE = "state";
    public static final String CREATION_DATE = "creationDate";
    public static final String START_DATE = "startDate";
    public static final String END_DATE = "endDate";
    public static final String AMI_VERSION = "amiVersion";
    public static final String NAME = "name";
    public static final String LOG_URI= "logUri";
    public static final String SUPPORTED_PRODUCTS = "supportedProducts";
    public static final String EC2_KEY_NAME = "ec2KeyName";
    public static final String EC2_SUBNET_ID = "ec2SubnetId";
    public static final String HADOOP_VERSION = "hadoopVersion";
    public static final String INSTANCE_COUNT = "instanceCount";
    public static final String KEEP_JOB_FLOW_ALIVE_WHEN_NO_STEPS = "keepJobFlowAliveWhenNoSteps";
    public static final String MASTER_INSTANCE_ID = "masterInstanceId";
    public static final String MASTER_INSTANCE_TYPE = "masterInstanceType";
    public static final String MASTER_PUBLIC_DNS_NAME = "masterPublicDnsName";
    public static final String AVAILABILITY_ZONE = "availabilityZone";
    public static final String SLAVE_INSTANCE_TYPE = "slaveInstanceType";
    public static final String BOOTSTRAP_ACTIONS = "bootstrapActions";
    public static final String PATH = "path";
    public static final String ARGS = "args";
    public static final String STEPS = "steps";
    public static final String ACTION_ON_FAILURE = "actionOnFailure";
    public static final String JAR = "jar";
    public static final String MAIN_CLASS = "mainClass";
    public static final String STEP_NAME = "stepName";
    public static final String TYPE = "type";
    public static final String SCRIPT = "script";
    public static final String DELETE_ON_EXIT = "deleteOnExit";
}
