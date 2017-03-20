package org.sioecp.service.statistics;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.sioecp.service.statistics.tools.SqlConnector;

import static org.junit.jupiter.api.Assertions.*;


class StatisticsServiceTest {
    private static final String CONFIG_FILE_PATH = "src/test/resources/config-test.properties";
    private static SqlConnector sql;
    private static StatisticsService service;

    @BeforeAll
    static void setUp() throws Exception {
        sql = new SqlConnector();
        sql.importPropertiesFromFile(CONFIG_FILE_PATH);
        service = new StatisticsService(CONFIG_FILE_PATH);
    }

    @Test
    void testAddDifference() throws Exception {
        // Count rows with movements not calculated
        int noDifferenceRows = sql.execCount("DW_station_state","movements is null");
        assertEquals(3,noDifferenceRows);

        // Exec add difference service
        service.addDifference();

        // Count rows with movements calculated
        int differenceRows = sql.execCount("DW_station_state","movements is null");
        assertEquals(1,differenceRows);
    }

    @Test
    void testCalculateMeansSampled() throws Exception {
        // Count sampled rows before insert
        int beforeRows = sql.execCount("DW_station_sampled",null);
        assertEquals(0,beforeRows);

        // Exec calculate sampled service
        service.calculateMeansSampled();

        // Count sampled rows after insert
        int afterRows = sql.execCount("DW_station_sampled",null);
        assertEquals(1,afterRows);
    }

    @Test
    void testCalculateMeans() throws Exception {
        // Count means rows before insert
        int beforeRows = sql.execCount("DW_station_means",null);
        assertEquals(0,beforeRows);

        // Exec calculate means service
        service.calculateMeans();

        // Count means rows after insert
        int afterRows = sql.execCount("DW_station_means",null);
        assertEquals(1,afterRows);
    }

}