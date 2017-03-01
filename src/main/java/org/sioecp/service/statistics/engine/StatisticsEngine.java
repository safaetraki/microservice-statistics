package org.sioecp.service.statistics.engine;

import org.sioecp.service.statistics.tools.SqlConnector;

import java.util.List;

/**
 * Created by Safae on 01/03/2017.
 */
public class StatisticsEngine {

    private static final int MAXROWS = 1000;
    private SqlConnector dbconnector;
    public int cleanedRows = 0;
    public int lastStatsStartTime = -1;

    public StatisticsEngine(SqlConnector sql){
        dbconnector = sql;
    }

    private int getfirstSSRow() {
        List<String> res = dbconnector.execRead("SELECT id FROM DW_station_state " +
                "ORDER BY id ASC " +
                "LIMIT 1").get(0);
        return Integer.parseInt(res.get(0));
    }

    private int getlastSMRow() {
        List<String> res = dbconnector.execRead("SELECT range_end FROM DW_station_means " +
                "ORDER BY id DESC " +
                "LIMIT 1").get(0);
        return Integer.parseInt(res.get(0));
    }

    // Fills the movements column with the absolute value of the difference between the actual available_bikes
    // and the available_bikes of the previous row for each row with the movements column null
    public void fillMovements(){
        int firstRow=getfirstSSRow();
        dbconnector.execWrite("UPDATE DW_station_state as state SET state.movements = ABS(state.available_bikes - " +
                "(SELECT available_bikes FROM DW_station_state where id = state.id - 1)) " +
                "WHERE state.movements = NULL " +
                "AND state.id <> " + firstRow);
    }

    // Fills station means table with calculated statistics
    public void fillStationMeansTable(){
        int lastRangeEnd=getlastSMRow();
        dbconnector.execWrite("INSERT INTO DW_station_means (id, id_station, week_day, range_start, range_end, " +
                "movement_mean, availability_mean, velib_nb_mean, movement_mean_rain, movement_mean_sun) " +
                "SELECT NULL, id_station, NULL, round(FROM_UNIXTIME(timestamp_start)/1800) as intervalH, NULL, " +
                "round(AVG(movement_mean),1), " +
                "round(AVG(availability_mean),1), " +
                "round(AVG(velib_nb_mean),1), NULL, NULL " +
                "FROM DW_station_sampled " +
                "WHERE timestamp_start >= " + lastRangeEnd +
                "GROUP BY intervalH");
    }
}
