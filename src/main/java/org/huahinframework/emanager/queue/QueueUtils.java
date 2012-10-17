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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;

import org.huahinframework.emanager.amazonaws.elasticmapreduce.Config;

/**
 *
 */
public class QueueUtils {
    private static final SimpleDateFormat KEY_FORMAT = new SimpleDateFormat("yyyyMMddHHmmssSSS");

    private static final long REMOVE_PERIOS = 2 * 24 * 60 * 60 * 1000; // 2 days

    private static final String QUEUE_PATH = System.getProperty("huahin.home") + "/queue/";

    /**
     * @return queue map
     * @throws Exception
     */
    public static synchronized Map<String, Config> list() throws Exception {
        Map<String, Config> queueMap = new TreeMap<String, Config>();

        File filePath = new File(QUEUE_PATH);
        for (String fileName : filePath.list()) {
            FileInputStream inFile = new FileInputStream(QUEUE_PATH + fileName);
            ObjectInputStream inObject = new ObjectInputStream(inFile);
            Config config = (Config) inObject.readObject();
            if (config.getStatus() == Config.JOB_STATUS_PENDING) {
                synchronized (KEY_FORMAT) {
                    queueMap.put(KEY_FORMAT.format(config.getDate()), config);
                }
            }

            inObject.close();
            inFile.close();
        }

        return queueMap;
    }

    /**
     * @return queue map
     * @throws Exception
     */
    public static synchronized Map<String, Config> runnings() throws Exception {
        Map<String, Config> queueMap = new TreeMap<String, Config>();

        File filePath = new File(QUEUE_PATH);
        for (String fileName : filePath.list()) {
            FileInputStream inFile = new FileInputStream(QUEUE_PATH + fileName);
            ObjectInputStream inObject = new ObjectInputStream(inFile);
            Config config = (Config) inObject.readObject();
            if (config.getStatus() == Config.JOB_STATUS_RUNNING) {
                synchronized (KEY_FORMAT) {
                    queueMap.put(KEY_FORMAT.format(config.getDate()), config);
                }
            }

            inObject.close();
            inFile.close();
        }

        return queueMap;
    }

    /**
     * @param stepName
     * @return Config
     * @throws Exception
     */
    public static synchronized Config get(String stepName) throws Exception {
        Config config = null;
        try {
            FileInputStream inFile = new FileInputStream(QUEUE_PATH + stepName);
            ObjectInputStream inObject = new ObjectInputStream(inFile);
            config = (Config) inObject.readObject();

            inObject.close();
            inFile.close();
        } catch (FileNotFoundException e) {
            throw new RuntimeException("step name not found: " + stepName);
        }

        return config;
    }

    /**
     * @return remove queue map
     * @throws Exception
     */
    public static synchronized Map<String, Config> listRemoveQueue() throws Exception {
        Map<String, Config> queueMap = new TreeMap<String, Config>();

        File filePath = new File(QUEUE_PATH);
        for (String fileName : filePath.list()) {
            FileInputStream inFile = new FileInputStream(QUEUE_PATH + fileName);
            ObjectInputStream inObject = new ObjectInputStream(inFile);
            Config config = (Config) inObject.readObject();
            if (config.getStatus() != Config.JOB_STATUS_PENDING) {
                long diff = System.currentTimeMillis() - config.getDate().getTime();
                if (diff < REMOVE_PERIOS) {
                    continue;
                }

                synchronized (KEY_FORMAT) {
                    queueMap.put(KEY_FORMAT.format(config.getDate()), config);
                }
            }

            inObject.close();
            inFile.close();
        }

        return queueMap;
    }

    /**
     * @param config
     * @return Config
     * @throws IOException
     */
    public static synchronized Config registerQueue(Config config) throws IOException {
        synchronized (KEY_FORMAT) {
            config.setDate(new Date());
            String date = KEY_FORMAT.format(config.getDate());
            config.setName("S_" + date);
            config.setStatus(Config.JOB_STATUS_PENDING);

            FileOutputStream outFile = new FileOutputStream(QUEUE_PATH + config.getName());
            ObjectOutputStream outObject = new ObjectOutputStream(outFile);
            outObject.writeObject(config);

            outObject.close();
            outFile.close();

            return config;
        }
    }

    /**
     * @param config
     * @throws IOException
     */
    public static synchronized void updateQueue(Config config) throws IOException {
        synchronized (KEY_FORMAT) {
            FileOutputStream outFile = new FileOutputStream(QUEUE_PATH + config.getName());
            ObjectOutputStream outObject = new ObjectOutputStream(outFile);
            outObject.writeObject(config);

            outObject.close();
            outFile.close();
        }
    }

    /**
     * @param config
     */
    public static void removeQueue(String stepName) {
        String queueFile = QUEUE_PATH + stepName;
        new File(queueFile).delete();
    }
}
