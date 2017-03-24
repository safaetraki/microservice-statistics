package org.sioecp.service.statistics.engine;

import org.sioecp.service.statistics.tools.SqlConnector;

import java.util.List;
import java.util.Map;


public abstract class AStatisticsEngine {

    public static int MAXROWS = 1000;
    protected SqlConnector dbconnector;
    public long cleanedRows = 0;
    public long lastCleanedRow = -1;

    public boolean runCleaning(){
        // Get last cleaned row number
        long firstRowToClean = getLastCleanedRow()+1;

        // Get last added row number
        long lastAddedRow = getLastDLRow();

        // Set max row to clean
        long maxRow = Math.min(firstRowToClean+MAXROWS,lastAddedRow+1);

        // Fill DW_station from Data Lake station data

        fillTable(firstRowToClean, maxRow);

        // Update counter
        cleanedRows = Math.max(maxRow - firstRowToClean, 0);
        lastCleanedRow = maxRow-1;

        // Set last cleaned row
        setLastCleanedRow(lastCleanedRow);

        return true;
    }

    abstract void fillTable(long firstRow, long lastRow);

    abstract void setLastCleanedRow(long lastRow);

    abstract long getLastDLRow();

    abstract long getLastCleanedRow();




}
