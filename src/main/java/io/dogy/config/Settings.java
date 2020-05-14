package io.dogy.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Set;

/**
 * This class stores dynamic setting.
 */
public class Settings {

    private static final Logger logger = LoggerFactory.getLogger(Settings.class);
    private static volatile Settings instance = null;
    private static final Object mutex = new Object();

    public int EVENT_LOOP_COUNT = 10;
    public int TTL_IN_SECONDS = (int) Duration.ofDays(3).getSeconds();

    public Set<String> HBASE_IP = Collections.singleton("localhost");
    public int HBASE_PORT = 2181;
    public String HBASE_LOCATION = "/hbase-unsecure";
    public int HBASE_SALT = 6;
    public String HBASE_TABLE = "timeseries_tbl";
    public boolean HBASE_COMPRESSION = false;

    public String TIMESCALEDB_IP = "localhost";
    public int TIMESCALEDB_PORT = 5432;
    public String TIMESCALEDB_DB = "postgres";
    public String TIMESCALEDB_TABLE = "timeseries_tbl";
    public String TIMESCALEDB_USER = "postgres";
    public String TIMESCALEDB_PASSWORD = "";
    public int TIMESCALEDB_POOL_SIZE = 50;

    public String INFLUXDB_IP = "localhost";
    public int INFLUXDB_PORT = 9999;
    public String INFLUXDB_ORG = "my-org";
    public String INFLUXDB_BUCKET = "my-bucket";
    public String INFLUXDB_TOKEN = "my-token";
    public String INFLUXDB_MEASUREMENT = "timeseries_tbl";

    public String TSDB_METRIC = "my.tsdb.test.metric";
    public String TSDB_HBASE_HOST ="127.0.0.1";
    public String TSDB_HBASE_PORT ="2181";
    public String TSDB_LOCATION = "/hbase";
    public String TSDB_TCP_PORT="4242";
    public String TSDB_HBASE_DATA_TABLE="tsdb";
    public String TSDB_HBASE_UID_TABLE="tsdb-uid";
    public String TSDB_HBASE_TREE_TABLE="tsdb-tree";
    public String TSDB_HBASE_META_TABLE="tsdb-meta";

    public static Settings getInstance() {
        Settings result = instance;
        if (result == null) {
            synchronized (mutex) {
                result = instance;
                if (result == null) {
                    try {
                        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
                        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

                        File file = new File("settings.yaml");
                        logger.info("Overiding Application Settings: \n\n" + FileUtils.readFileToString(file, StandardCharsets.UTF_8));
                        logger.info("##################\n");

                        instance = result = mapper.readValue(file, Settings.class);
                    } catch (FileNotFoundException e1) {
                        instance = result = new Settings();
                        logger.info("Using default settings!");
                    } catch (Exception e) {
                        instance = result = new Settings();
                        logger.error("Error when loading setting properties", e);
                        logger.info("Using default settings!");
                    }
                }
            }
        }
        return result;
    }
}
