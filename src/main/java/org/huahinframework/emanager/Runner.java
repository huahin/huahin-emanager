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
package org.huahinframework.emanager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.huahinframework.emanager.amazonaws.elasticmapreduce.EMRProperties;
import org.huahinframework.emanager.queue.QueueManager;
import org.huahinframework.emanager.util.StopUtils;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.webapp.WebAppContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 *
 */
public class Runner {
    private static final Log log = LogFactory.getLog(Runner.class);

    private static final String OPERATION_START = "start";
    private static final String OPERATION_STOP = "stop";

    /**
     * @param args
     */
    public static void main(String[] args) {
        if (args.length != 4) {
            System.err.println("argument error: [start | stop] [war path] [port] [shutdown port]");
            System.exit(-1);
        }

        String operation = args[0];
        String war = args[1];
        int port = Integer.valueOf(args[2]);
        int shutDownPort = Integer.valueOf(args[3]);

        Runner runner = new Runner();
        if (operation.equals(OPERATION_START)) {
            runner.start(war, port, shutDownPort);
        } else if (operation.equals(OPERATION_STOP)) {
            runner.stop(shutDownPort);
        }

        System.exit(0);
    }

    /**
     * @param war
     * @param port
     * @param shutDownPort
     */
    public void start(String war, int port, int shutDownPort) {
        log.info("huahin-emanager start");

        ConfigurableApplicationContext applicationContext = null;
        try {
            applicationContext
                = new ClassPathXmlApplicationContext("huahinEManagerProperties.xml",
                                                     "huahinEManagerEMRProperties.xml");
            Properties properties =
                    (Properties) applicationContext.getBean("properties");
            EMRProperties emrProperties =
                    (EMRProperties) applicationContext.getBean("emrProperties");

            QueueManager queueManager = new QueueManager(properties, emrProperties);
            queueManager.start();

            SelectChannelConnector connector = new SelectChannelConnector();
            connector.setPort(port);

            Server server = new Server();
            server.setConnectors(new Connector[] { connector });

            WebAppContext web = new WebAppContext();
            web.setContextPath("/");
            web.setWar(war);

            ShutDowner shutDowner = new ShutDowner(shutDownPort, server, queueManager);
            shutDowner.start();

            server.addHandler(web);
            server.start();
            server.join();
            shutDowner.join();
            queueManager.join();
        } catch (Exception e) {
            log.error("huahin-emanager aborted", e);
            System.exit(-1);
        } finally {
            if (applicationContext != null) {
                applicationContext.close();
            }
        }

        log.info("huahin-emanager end");
    }

    /**
     * @param port
     */
    public void stop(int port) {
        StopUtils.stop(port, OPERATION_STOP);
    }
}
