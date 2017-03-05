package org.sioecp.service.statistics.engine;

import org.sioecp.service.statistics.tools.SqlConnector;

import java.util.List;

/**
 * Created by Safae on 01/03/2017.
 */
public class StatisticsEngine {

    private SqlConnector dbconnector;

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

    private int getlastSSARow() {
        List<String> res = dbconnector.execRead("SELECT timestamp_end FROM DW_station_sampled " +
                "ORDER BY id DESC " +
                "LIMIT 1").get(0);
        return Integer.parseInt(res.get(0));
    }

    // Fills the movements column with the absolute value of the difference between the actual available_bikes
    // and the available_bikes of the previous row for each row with the movements column null
    public void fillMovements(){
        int firstRow=getfirstSSRow();
        dbconnector.execWrite("UPDATE dw_station_state as st_state, " +
                "(select state.id as state_id, " +
                "ABS(" +
                "state.available_bikes - " +
                "(SELECT st.available_bikes FROM dw_station_state as st " +
                "where st.id = state.id - 1)" +
                ") " +
                "as move " +
                "from dw_station_state as state " +
                "WHERE state.movements is null AND state.id <> "+ firstRow +") as calculus " +
                "SET st_state.movements = calculus.move" +
                "WHERE st_state.id = calculus.state_id");
    }

    // Fills station means table with calculated statistics
    public void fillStationMeansTable(){
        int lastRangeEnd=getlastSMRow();
        dbconnector.execWrite("INSERT INTO DW_station_means (id, id_station, week_day, range_start, range_end, " +
                "movement_mean, availability_mean, velib_nb_mean, movement_mean_rain, movement_mean_sun) " +
                "(SELECT null,id_station, dayofweek(from_unixtime(timestamp_start)), timestamp_start, " +
                "unix_timestamp(addtime(FROM_UNIXTIME(timestamp_start), '00:30:00')), " +
                "round(AVG(movement_mean),1), " +
                "round(AVG(availability_mean),1), " +
                "round(AVG(velib_nb_mean),1), round(count(weather=\"rain\")/count(*),1), " +
                "round(count(weather=\"sun\")/count(*),1) " +
                "FROM DW_station_sampled " +
                "WHERE timestamp_start >= " + lastRangeEnd +
                " GROUP BY round(timestamp_start / 1800))");
    }

    // Fills station sampled table with calculated statistics
    public void fillStationSampledTable(){
        int lastRangeEnd=getlastSSARow();
        dbconnector.execWrite("INSERT INTO dw_station_sampled (id, id_station, timestamp_start, timestamp_end, " +
                "movement_mean, availability_mean, velib_nb_mean, weather) " +
                "(select null, id_station, last_update, " +
                "unix_timestamp(addtime(FROM_UNIXTIME(last_update), '00:02:00')), " +
                "round(AVG(movements),1), round(AVG(available_bike_stands),1), round(AVG(available_bikes),1), null " +
                "from dw_station_state " +
                "WHERE last_update >= " + lastRangeEnd +
                " GROUP BY round(last_update / 120))");
    }
}
