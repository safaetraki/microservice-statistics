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

import org.sioecp.service.statistics.engine.StatisticsEngine;
import org.sioecp.service.statistics.tools.SqlConnector;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;

/**
 * Statistics service.
 */
@Path("/statistics")
public class StatisticsService {

    private String propertiesPath;

    public StatisticsService(){
        // Default value
        this.propertiesPath = "config.properties";
    }

    public StatisticsService(String propertiesPath){
        this.propertiesPath = propertiesPath;
    }


    @GET
    @Path("/")
    public String get() {
        // TODO: Implementation for HTTP GET request
        System.out.println("GET invoked");
        return "Hello from WSO2 MSF4J";
    }

    @GET
    @Path("/movements")
    public void addDifference() {
        // Setup SQL connection
        SqlConnector sql = new SqlConnector();
        sql.importPropertiesFromFile(propertiesPath);

        // Init cleaner class
        StatisticsEngine engine = new StatisticsEngine(sql);

        // Start filling movements
        engine.fillMovements();
    }

    @GET
    @Path("/meansByPeriod")
    public void calculateMeansByPeriod() {
        // Setup SQL connection
        SqlConnector sql = new SqlConnector();
        sql.importPropertiesFromFile(propertiesPath);

        // Init cleaner class
        StatisticsEngine engine = new StatisticsEngine(sql);

        // Start filling movements
        engine.fillStationMeansTable();
    }

    @POST
    @Path("/")
    public void post() {
        // TODO: Implementation for HTTP POST request
        System.out.println("POST invoked");
    }

    @PUT
    @Path("/")
    public void put() {
        // TODO: Implementation for HTTP PUT request
        System.out.println("PUT invoked");
    }

    @DELETE
    @Path("/")
    public void delete() {
        // TODO: Implementation for HTTP DELETE request
        System.out.println("DELETE invoked");
    }
}
