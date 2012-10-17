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


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.huahinframework.emanager.Properties;
import org.huahinframework.emanager.amazonaws.elasticmapreduce.Config;
import org.huahinframework.emanager.amazonaws.elasticmapreduce.EMRProperties;
import org.huahinframework.emanager.amazonaws.elasticmapreduce.ElasticMapReduceManager;

/**
 *
 */
public class QueueManager extends Thread {
    private static final Log log = LogFactory.getLog(QueueManager.class);

    private static final int POLLING_SECOND = (30 * 1000);

    private int clusterSize;
    private EMRProperties emrProperties;

    /**
     * @param properties
     * @param emrProperties
     */
    public QueueManager(Properties properties, EMRProperties emrProperties) {
        this.clusterSize = properties.getClusterSize();
        this.emrProperties = emrProperties;
    }

    /* (non-Javadoc)
     * @see java.lang.Thread#run()
     */
    @Override
    public void run() {
        log.info("QueueManager start");

        List<RunningJobThread> runningThreads = new ArrayList<RunningJobThread>();
        for (int i = 0; i < clusterSize; i++) {
            RunningJobThread rjt = new RunningJobThread(new ElasticMapReduceManager(emrProperties));
            rjt.start();
            runningThreads.add(rjt);
        }

        try {
            for (;;) {
                Map<String, Config> runQueueMap = QueueUtils.listRemoveQueue();
                for (Entry<String, Config> entry : runQueueMap.entrySet()) {
                    QueueUtils.removeQueue(entry.getValue().getName());
                }

                int runningSize = 0;
                for (RunningJobThread rjt : runningThreads) {
                    if (rjt.isRunnsing()) {
                        runningSize++;
                    }
                }
                if (clusterSize > 0 && (runningSize >= clusterSize)) {
                    if (sleep()) {
                        break;
                    }
                    continue;
                }

                Map<String, Config> queues = QueueUtils.list();
                for (Entry<String, Config> entry : queues.entrySet()) {
                    Config config = entry.getValue();

                    runningSize = 0;
                    for (RunningJobThread rjt : runningThreads) {
                        runningSize++;
                        if (!rjt.isRunnsing()) {
                            try {
                                config.setStatus(Config.JOB_STATUS_RUNNING);
                                QueueUtils.updateQueue(config);
                            } catch (IOException e) {
                                e.printStackTrace();
                                log.error(e);
                            }
                            rjt.setRunningConfig(config);
                            break;
                        }
                    }

                    if (runningThreads.size() == runningSize) {
                        break;
                    }
                }

                for (RunningJobThread rjt : runningThreads) {
                    if (!rjt.isRunnsing() && rjt.isTimeAfter()) {
                        rjt.instanceTerminate();
                    }
                }

                if (sleep()) {
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error(e);
        }

        for (RunningJobThread rjt : runningThreads) {
            // wait for threaed terminate
            try {
                rjt.terminate(emrProperties.isJobForWait());
                rjt.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
                log.error(e);
            }
        }

        log.info("QueueManager end");
    }

    /**
     * @return terminated
     */
    private boolean sleep() {
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
    public void terminate() {
        super.interrupt();
    }
}
