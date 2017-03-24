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

import org.sioecp.service.statistics.engine.AStatisticsEngine;
import org.sioecp.service.statistics.engine.FillMovementsEngine;
import org.sioecp.service.statistics.engine.SampleStatisticsEngine;
import org.sioecp.service.statistics.engine.StationMeansEngine;
import org.sioecp.service.statistics.tools.SqlConnector;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 * Statistics service.
 */
@Path("/statistics")
public class StatisticsService {

    private String propertiesPath;

    public StatisticsService(String propertiesPath, int maxRowToClean){
        this.propertiesPath = propertiesPath;
        AStatisticsEngine.MAXROWS = maxRowToClean;
    }

    @GET
    @Path("/movements")
    public void addDifference() {
        // Setup SQL connection
        SqlConnector sql = new SqlConnector();
        sql.importPropertiesFromFile(propertiesPath);

        // Init AStatisticsEngine class
        FillMovementsEngine engine = new FillMovementsEngine(sql);

        // Start filling movements
        engine.runCleaning();
    }

    @GET
    @Path("/means")
    public void calculateMeans() {
        // Setup SQL connection
        SqlConnector sql = new SqlConnector();
        sql.importPropertiesFromFile(propertiesPath);

        // Init AStatisticsEngine class
        StationMeansEngine engine = new StationMeansEngine(sql);

        // Start filling movements
        engine.fillStationMeansTable();
    }

    @GET
    @Path("/meansSampled")
    public void calculateMeansSampled() {
        // Setup SQL connection
        SqlConnector sql = new SqlConnector();
        sql.importPropertiesFromFile(propertiesPath);

        // Init AStatisticsEngine class
        SampleStatisticsEngine engine = new SampleStatisticsEngine(sql);

        // Start filling movements
        engine.runCleaning();
    }
}
