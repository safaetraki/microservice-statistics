package org.sioecp.service.statistics.engine;

import org.sioecp.service.statistics.tools.SqlConnector;

import java.time.LocalDate;
import java.util.Date;
import java.util.List;


public class StatisticsEngine {

    private SqlConnector dbconnector;

    public StatisticsEngine(SqlConnector sql){
        dbconnector = sql;
    }

    private long getfirstSSRow() {
        List<String> res = dbconnector.execRead("SELECT id FROM DW_station_state " +
                "ORDER BY id ASC " +
                "LIMIT 1").get(0);
        return Long.parseLong(res.get(0));
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

    private long getlastSSaRow() {
        List<List<String>> res = dbconnector.execRead("SELECT timestamp_end FROM DW_station_sampled " +
                "ORDER BY timestamp_end DESC " +
                "LIMIT 1");
        if(res!=null&&!res.isEmpty()&&res.get(0)!=null&&!res.get(0).isEmpty()){
            return Long.parseLong(res.get(0).get(0));
        }
        return getRangeStart();
    }

    // Fills the movements column with the absolute value of the difference between the actual available_bikes
    // and the available_bikes of the previous row for each row with the movements column null
    public void fillMovements(){
        long firstRow=getfirstSSRow();
        dbconnector.execWrite("UPDATE DW_station_state as st_state, " +
                "(select state.id as state_id, " +
                "ABS(" +
                "state.available_bikes - " +
                "(SELECT st.available_bikes FROM DW_station_state as st " +
                "where st.id = state.id - 1)" +
                ") " +
                "as move " +
                "from DW_station_state as state " +
                "WHERE state.movements is null AND state.id <> "+ firstRow +") as calculus " +
                "SET st_state.movements = calculus.move " +
                "WHERE st_state.id = calculus.state_id");
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

    // Fills station sampled table with calculated statistics
    public void fillStationSampledTable(){
        long lastRangeEnd=getlastSSaRow();
        dbconnector.execWrite("INSERT INTO DW_station_sampled (id, id_station, timestamp_start, timestamp_end, " +
                "movement_mean, availability_mean, velib_nb_mean, weather) " +
                "(select null, ss.id_station, (ss.last_update * 0.001) as time_start, " +
                "unix_timestamp(addtime(FROM_UNIXTIME(ss.last_update * 0.001), '00:30:00')) as time_end, " +
                "round(AVG(ss.movements),1), round(AVG(ss.available_bike_stands),1), round(AVG(ss.available_bikes),1), " +
                "(select w.weather_group from dw_weather w, DW_station s " +
                "where w.calculation_time >= time_start and w.calculation_time <= time_end"+ // lastRangeEnd +
                " and w.city_id = s.city_id " +
                "and ss.id_station = s.id limit 1) " +
                "from DW_station_state ss " +
                "WHERE ss.last_update >= " + lastRangeEnd +
                " GROUP BY ss.id_station, round(time_start / 1800))");
    }
}
