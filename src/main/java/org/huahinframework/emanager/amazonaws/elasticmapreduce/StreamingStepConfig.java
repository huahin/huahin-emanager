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

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;

/**
 *
 */
public class StreamingStepConfig implements CreateStepConfigger {
    private static final String STREAMING_JAR = "/home/hadoop/contrib/streaming/hadoop-streaming.jar";

    /**
     * @param args
     */
    public StreamingStepConfig() {
    }

    /* (non-Javadoc)
     * @see org.huahinframework.manager.amazonaws.elasticmapreduce.CreateStepConfigger#createStepConfig(org.huahinframework.manager.amazonaws.elasticmapreduce.Config)
     */
    @Override
    public StepConfig[] createStepConfig(Config config) {
        List<String> args = new ArrayList<String>();
        for (Entry<String, String> entry : config.getArgMap().entrySet()) {
            args.add("-" + entry.getKey());
            args.add(entry.getValue());
        }

        StepConfig stepConfig =
                new StepConfig()
                        .withName(config.getName())
                        .withActionOnFailure(ACTION_ON_FAILURE)
                        .withHadoopJarStep(new HadoopJarStepConfig()
                                                    .withJar(STREAMING_JAR)
                                                    .withArgs(args));
        StepConfig[] stepConfigs = new StepConfig[1];
        stepConfigs[0] = stepConfig;
        return stepConfigs;
    }
}
