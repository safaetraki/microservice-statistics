package org.sioecp.service.statistics.engine;


import org.sioecp.service.statistics.tools.SqlConnector;

import java.util.List;

public class FillMovementsEngine extends AStatisticsEngine {

    public FillMovementsEngine(SqlConnector sql){
        dbconnector = sql;
    }

    // Fills the movements column with the absolute value of the difference between the actual available_bikes
    // and the available_bikes of the previous row for each row with the movements column null
    public void fillTable(long firstRow, long lastRow){
        dbconnector.execWrite("UPDATE DW_station_state as st_state, " +
                "(select state.id as state_id, " +
                "ABS(" +
                "state.available_bikes - " +
                "(SELECT st.available_bikes FROM DW_station_state as st " +
                "where st.id = state.id - 1)" +
                ") " +
                "as move " +
                "from DW_station_state as state " +
                "WHERE state.movements is null AND state.id >= "+ firstRow +" AND state.id < "+ lastRow +") as calculus " +
                "SET st_state.movements = calculus.move " +
                "WHERE st_state.id = calculus.state_id");
    }


    protected void setLastCleanedRow(long lastRow){
        dbconnector.execWrite("UPDATE MS_DataCleaning_conf SET value='"+lastRow+"' " +
                "WHERE name='movements_last_cleaned_row'");
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
                "WHERE name='movements_last_cleaned_row'").get(0);
        return Long.parseLong(res.get(0));
    }

}
