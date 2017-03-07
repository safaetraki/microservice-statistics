package org.sioecp.service.statistics.engine;

import org.sioecp.service.statistics.tools.SqlConnector;

import java.time.LocalDate;
import java.util.Date;
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

    private int getRangeStart() {
        List<String> res = dbconnector.execRead("SELECT last_update FROM DW_station_state " +
                "ORDER BY id ASC " +
                "LIMIT 1").get(0);
        return Integer.parseInt(res.get(0));
    }

    private int getRangeEnd() {
        List<String> res = dbconnector.execRead("SELECT last_update FROM DW_station_state " +
                "ORDER BY id DESC " +
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
                "SET st_state.movements = calculus.move " +
                "WHERE st_state.id = calculus.state_id");
    }

    // Fills station means table with calculated statistics
    public void fillStationMeansTable(){
        int rangeEnd=getRangeEnd();
        int rangeStart=getRangeStart();
        dbconnector.execWrite("INSERT INTO DW_station_means (id, id_station, week_day, range_start, range_end, " +
                "movement_mean, availability_mean, velib_nb_mean, movement_mean_rain, movement_mean_sun) " +
                "(SELECT null,id_station, dayofweek(from_unixtime(" + rangeStart +
                ")), " + rangeStart +
                ", " + rangeEnd +
                ", " +
                "round(AVG(movement_mean),1), " +
                "round(AVG(availability_mean),1), " +
                "round(AVG(velib_nb_mean),1), " +
                "round(AVG(movement_mean)/(select count(weather) from DW_station_sampled where weather='rain'),1), " +
                "round(AVG(movement_mean)/(select count(weather) from DW_station_sampled where weather='sun'),1) " +
                "FROM DW_station_sampled)");
    }

    // Fills station sampled table with calculated statistics
    public void fillStationSampledTable(){
        int lastRangeEnd=getlastSSARow();
        dbconnector.execWrite("INSERT INTO dw_station_sampled (id, id_station, timestamp_start, timestamp_end, " +
                "movement_mean, availability_mean, velib_nb_mean, weather) " +
                "(select null, id_station, last_update, " +
                "unix_timestamp(addtime(FROM_UNIXTIME(last_update), '00:30:00')), " +
                "round(AVG(movements),1), round(AVG(available_bike_stands),1), round(AVG(available_bikes),1), null " +
                "from dw_station_state " +
                "WHERE last_update >= " + lastRangeEnd +
                " GROUP BY round(last_update / 1800))");
    }

    private List<List<String>> getStatisticsPerDay(LocalDate date) {
        List<List<String>> res = dbconnector.execRead("SELECT * FROM DW_station_means " +
                "Where date(from_unixtime(range_start)) = " + java.sql.Date.valueOf(date));
        return res;
    }
}
