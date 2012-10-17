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

import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;

/**
 *
 */
public class CustomJarStepConfig implements CreateStepConfigger {
    /* (non-Javadoc)
     * @see org.huahinframework.manager.amazonaws.elasticmapreduce.CreateStepConfigger#createStepConfig(org.huahinframework.manager.amazonaws.elasticmapreduce.Config)
     */
    @Override
    public StepConfig[] createStepConfig(Config config) {
        List<String> args = new ArrayList<String>();
        for (String s : config.getArgs()) {
            args.add(s);
        }

        HadoopJarStepConfig hadoopJarStepConfig =
                new HadoopJarStepConfig()
                        .withJar(config.getRun())
                        .withArgs(args);
        if (config.getMainClass() != null && !config.getMainClass().isEmpty()) {
            hadoopJarStepConfig.setMainClass(config.getMainClass());
        }

        StepConfig stepConfig =
                new StepConfig()
                        .withName(config.getName())
                        .withActionOnFailure(ACTION_ON_FAILURE)
                        .withHadoopJarStep(hadoopJarStepConfig);
        StepConfig[] stepConfigs = new StepConfig[1];
        stepConfigs[0] = stepConfig;
        return stepConfigs;
    }
}
