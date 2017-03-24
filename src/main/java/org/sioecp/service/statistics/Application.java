/*
 * Copyright (c) 2016, WSO2 Inc. (http://wso2.com) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.sioecp.service.statistics;

import org.wso2.msf4j.MicroservicesRunner;

/**
 * Application entry point.
 *
 * @since 1.0-SNAPSHOT
 */
public class Application {
    // Default Port
    static int port = 8080;
    // Default config file
    static String configFile = "config.properties";
    // Max row to clean
    static int maxRowToClean = 1000;

    public static void main(String[] args) {
        // Handle parameters
        // Call should be -> java -jar Cleaning-Service.jar [-conf <propertiesFile>] [-port <port>]
        for (int i = 0; i < args.length-1;i+=2){
            if (args[i].equals("-port"))
                port = Integer.parseInt(args[i+1]);
            else if (args[i].equals("-conf"))
                configFile = args[i+1];
            else if (args[i].equals("-maxrow"))
                maxRowToClean = Integer.parseInt(args[i+1]);
        }

        // Run microservice
        new MicroservicesRunner(port)
                .deploy(new StatisticsService(configFile,maxRowToClean))
                .start();
    }
}
