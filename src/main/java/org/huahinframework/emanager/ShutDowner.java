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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.huahinframework.emanager.queue.QueueManager;
import org.mortbay.jetty.Server;

/**
 *
 */
public class ShutDowner extends Thread {
    private static final Log log = LogFactory.getLog(ShutDowner.class);

    private static final String OPERATION_STOP = "stop";

    private int port;
    private Server server;
    private QueueManager queueManager;

    /**
     * @param port
     * @param server
     * @param queueManager
     */
    public ShutDowner(int port,
                      Server server,
                      QueueManager queueManager) {
        this.port = port;
        this.server = server;
        this.queueManager = queueManager;
    }

    /* (non-Javadoc)
     * @see java.lang.Thread#run()
     */
    @Override
    public void run() {
        log.info("ShutDowner start");

        try {
            ServerSocket svsock = new ServerSocket(port);
            for (;;) {
                Socket socket = svsock.accept();
                BufferedReader reader =
                    new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String line = reader.readLine();

                reader.close();
                socket.close();

                if(line.matches(OPERATION_STOP)){
                    server.stop();
                    queueManager.terminate();
                    break;
                }
            }

            svsock.close();
        } catch (Exception e) {
            log.error(e);
            e.printStackTrace();
        }

        log.info("ShutDowner end");
    }
}
