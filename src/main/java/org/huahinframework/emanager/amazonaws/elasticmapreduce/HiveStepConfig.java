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

import com.amazonaws.services.elasticmapreduce.model.ActionOnFailure;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;

/**
 *
 */
public class HiveStepConfig implements CreateStepConfigger {
    private boolean installed;
    private StepFactory stepFactory;

    /**
     * @param args
     */
    public HiveStepConfig() {
        this.stepFactory = new StepFactory();
    }

    /* (non-Javadoc)
     * @see org.huahinframework.manager.amazonaws.elasticmapreduce.CreateStepConfigger#createStepConfig()
     */
    @Override
    public StepConfig[] createStepConfig(Config config) {
        StepConfig installHive = null;
        if (!installed) {
            installHive = new StepConfig()
                                .withName("Install Hive")
                                .withActionOnFailure(ActionOnFailure.TERMINATE_JOB_FLOW)
                                .withHadoopJarStep(stepFactory.newInstallHiveStep());
            installed = true;
        }

        HadoopJarStepConfig hadoopJarStepConfig = null;
        if (config.getArgs() != null) {
            hadoopJarStepConfig = stepFactory.newRunHiveScriptStep(config.getRun(), config.getArgs());
        } else {
            hadoopJarStepConfig = stepFactory.newRunHiveScriptStep(config.getRun());
        }

        StepConfig stepConfig =
                new StepConfig()
                        .withName(config.getName())
                        .withActionOnFailure(ACTION_ON_FAILURE)
                        .withHadoopJarStep(hadoopJarStepConfig);

        StepConfig[] stepConfigs = null;
        if (installHive != null) {
            stepConfigs = new StepConfig[2];
            stepConfigs[0] = installHive;
            stepConfigs[1] = stepConfig;
        } else {
            stepConfigs = new StepConfig[1];
            stepConfigs[0] = stepConfig;
        }

        return stepConfigs;
    }
}
