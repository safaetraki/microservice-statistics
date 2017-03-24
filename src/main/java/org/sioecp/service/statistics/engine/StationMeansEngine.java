package org.sioecp.service.statistics.engine;

import org.sioecp.service.statistics.tools.SqlConnector;

import java.util.List;


public class StationMeansEngine {

    private SqlConnector dbconnector;

    public StationMeansEngine(SqlConnector sql){
        dbconnector = sql;
    }

    // Fills station means table with calculated statistics
    public void fillStationMeansTable(){
        long rangeEnd=getRangeEnd();
        long rangeStart=getRangeStart();
        String strMovementWeather="round(AVG(movement_mean)/(select count(weather) from DW_station_sampled where weather=";
        dbconnector.execWrite("INSERT INTO DW_station_means (id, id_station, week_day, range_start, range_end, " +
                "movement_mean, availability_mean, velib_nb_mean, movement_mean_rain, movement_mean_sun) " +
                "(SELECT null,id_station, dayofweek(from_unixtime(" + rangeStart +
                " * 0.001)), (" + rangeStart +
                " * 0.001), (" + rangeEnd +
                " * 0.001), " +
                "round(AVG(movement_mean),1), " +
                "round(AVG(availability_mean),1), " +
                "round(AVG(velib_nb_mean),1), " +
                strMovementWeather + "'Rain'),1), " +
                strMovementWeather + "'Sun'),1) " +
                "FROM DW_station_sampled group by id_station)");
    }

    private long getRangeStart() {
        List<String> res = dbconnector.execRead("SELECT last_update FROM DW_station_state " +
                "ORDER BY last_update ASC " +
                "LIMIT 1").get(0);
        return Long.parseLong(res.get(0));
    }

    private long getRangeEnd() {
        List<String> res = dbconnector.execRead("SELECT last_update FROM DW_station_state " +
                "ORDER BY last_update DESC " +
                "LIMIT 1").get(0);
        return Long.parseLong(res.get(0));
    }
}
