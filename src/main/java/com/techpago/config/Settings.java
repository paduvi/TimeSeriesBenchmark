package com.techpago.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Set;

/**
 * This class stores dynamic setting.
 */
public class Settings {

    private static final Logger logger = Logger.getLogger(Settings.class);
    private static volatile Settings instance = null;
    private static final Object mutex = new Object();

    public Set<String> HBASE_IP = Collections.singleton("localhost");
    public int HBASE_PORT = 2181;
    public String HBASE_LOCATION = "/hbase-unsecure";
    public int HBASE_SALT = 6;
    public String HBASE_TABLE = "timeseries_tbl";

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
                        logger.info("Overiding Application Settings:");
                        logger.info(FileUtils.readFileToString(file, StandardCharsets.UTF_8));
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
