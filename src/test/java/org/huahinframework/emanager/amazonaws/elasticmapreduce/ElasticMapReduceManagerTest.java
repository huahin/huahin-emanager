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

import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.huahinframework.emanager.amazonaws.elasticmapreduce.Config;
import org.huahinframework.emanager.amazonaws.elasticmapreduce.ElasticMapReduceManager;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class ElasticMapReduceManagerTest {
    private ElasticMapReduceManager emrm;
    private boolean test;

    @Before
    public void setup() {
        if (!test) {
            return;
        }

        String accessKey = System.getProperty("accessKey");
        String secretKey = System.getProperty("secretKey");

        EMRProperties emrProperties = new EMRProperties();
        emrProperties.setAccessKey(accessKey);
        emrProperties.setSecretKey(secretKey);

        emrm = new ElasticMapReduceManager(emrProperties);
    }

    @Test
    public void testHive()
            throws URISyntaxException {
        if (!test) {
            return;
        }

        Config config = new Config();
        config.setName("ElasticMapReduceManager");
        config.setRun("s3://huahin/test/scripts/hive/wordcount.hql");

        emrm.runJob(config);
    }

    @Test
    public void testPig()
            throws URISyntaxException {
        if (!test) {
            return;
        }

        Config config = new Config();
        config.setName("ElasticMapReduceManager");
        config.setRun("s3://huahin/test/scripts/pig/wordcount.pig");

        emrm.runJob(config);
    }

    @Test
    public void testStreaming()
            throws URISyntaxException {
        if (!test) {
            return;
        }

      Config config = new Config();
      config.setName("ElasticMapReduceManager");
      Map<String, String> argMap = new HashMap<String, String>();
      argMap.put("input", "s3://huahin/test/input/");
      argMap.put("output", "s3://huahin/test/output/streaming/");
      argMap.put("mapper", "s3://huahin/test/scripts/streaming/map.py");
      argMap.put("reducer", "s3://huahin/test/scripts/streaming/reduce.py");
      config.setArgMap(argMap);

      emrm.runJob(config);
    }

    @Test
    public void testCustomJar()
            throws URISyntaxException {
        if (!test) {
            return;
        }

      Config config = new Config();
      config.setName("ElasticMapReduceManager");
      config.setRun("/home/hadoop/contrib/streaming/hadoop-streaming.jar");
      String[] args = new String[8];
      args[0] = "-input";
      args[1] = "s3://huahin/test/input/";
      args[2] = "-output";
      args[3] = "s3://huahin/test/output/streaming/";
      args[4] = "-mapper";
      args[5] = "s3://huahin/test/scripts/streaming/map.py";
      args[6] = "-reducer";
      args[7] = "s3://huahin/test/scripts/streaming/reduce.py";
      config.setArgs(args);

        emrm.runJob(config);
    }
}
