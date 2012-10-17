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
package org.huahinframework.emanager.queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.huahinframework.emanager.amazonaws.elasticmapreduce.Config;
import org.huahinframework.emanager.amazonaws.elasticmapreduce.ElasticMapReduceManager;

/**
 *
 */
public class RunningJobThread extends Thread {
    private static final Log log = LogFactory.getLog(RunningJobThread.class);

    private static final int POLLING_SECOND = (30 * 1000);

    private ElasticMapReduceManager manager;
    private Config runningConfig;
    private boolean terminated;

    /**
     * @param manager
     */
    public RunningJobThread(ElasticMapReduceManager manager) {
        this.manager = manager;
    }

    /* (non-Javadoc)
     * @see java.lang.Thread#run()
     */
    @Override
    public void run() {
        log.info(" RunningJobThread start");

        for (;;) {
            if (runningConfig != null) {
                log.info("create job: " + runningConfig.getName());
                try {
                    manager.runJob(runningConfig);
                } catch (Exception e) {
                    log.error(e);
                    e.printStackTrace();
                }
                runningConfig = null;
            }

            if (sleep()) {
                break;
            }
        }

        if (!manager.isRunning()) {
            manager.instanceTerminate();
        }

        try {
            manager.terminate();
            manager.join();
        } catch (InterruptedException e) {
        }

        log.info(" RunningJobThread end");
    }

    /**
     * @return terminated
     * @throws InterruptedException
     */
    private boolean sleep() {
        if (terminated) {
            return terminated;

        }

        try {
            super.sleep(POLLING_SECOND);
        } catch (InterruptedException e) {
            return true;
        }

        return false;
    }

    /**
     *
     */
    public void terminate(boolean jobForWait) {
        terminated = true;
        if (!jobForWait) {
            manager.terminate();
        }
        super.interrupt();
    }

    /**
     *
     */
    public void instanceTerminate() {
        manager.instanceTerminate();
    }

    /**
     * @param runningConfig the runningConfig to set
     */
    public void setRunningConfig(Config runningConfig) {
        this.runningConfig = runningConfig;
    }

    /**
     *
     */
    public boolean isRunnsing() {
        return runningConfig != null;
    }

    /**
     *
     */
    public boolean isTimeAfter() {
        return manager.isTimeAfter();
    }
}
