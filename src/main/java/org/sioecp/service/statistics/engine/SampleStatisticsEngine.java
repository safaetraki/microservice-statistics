package org.sioecp.service.statistics.engine;


import org.sioecp.service.statistics.tools.SqlConnector;

import java.util.List;

public class SampleStatisticsEngine extends AStatisticsEngine {

    public SampleStatisticsEngine(SqlConnector sql){
        dbconnector = sql;
    }

    // Fills station sampled table with calculated statistics
    public void fillTable(long firstRow, long lastRow){
        dbconnector.execWrite("INSERT INTO DW_station_sampled (id, id_station, timestamp_start, timestamp_end, " +
                "movement_mean, availability_mean, velib_nb_mean, weather) " +
                "(select null, ss.id_station, (ss.last_update * 0.001) as time_start, " +
                "unix_timestamp(addtime(FROM_UNIXTIME(ss.last_update * 0.001), '00:30:00')) as time_end, " +
                "round(AVG(ss.movements),1), round(AVG(ss.available_bike_stands),1), round(AVG(ss.available_bikes),1), " +
                "(select w.weather_group from DW_weather w, DW_station s " +
                "where w.calculation_time >= time_start and w.calculation_time <= time_end"+ // lastRangeEnd +
                " and w.city_id = s.city_id " +
                "and ss.id_station = s.id limit 1) " +
                "from DW_station_state ss " +
                "WHERE ss.id >= " + firstRow +" AND ss.id < " + lastRow +
                " GROUP BY ss.id_station, round(time_start / 1800))");
    }

    protected void setLastCleanedRow(long lastRow){
        dbconnector.execWrite("UPDATE MS_DataCleaning_conf SET value='"+lastRow+"' " +
                "WHERE name='sample_last_cleaned_row'");
    }


    // Returns the latest row number added to the Data Lake
    protected long getLastDLRow() {
        List<String> res = dbconnector.execRead("SELECT id FROM DW_station_state " +
                "ORDER BY id DESC " +
                "LIMIT 1").get(0);
        return Long.parseLong(res.get(0));
    }

    // Returns the latest cleaned row from the configuration table
    protected long getLastCleanedRow() {
        List<String> res = dbconnector.execRead("SELECT value FROM MS_DataCleaning_conf " +
                "WHERE name='sample_last_cleaned_row'").get(0);
        return Long.parseLong(res.get(0));
    }
}
