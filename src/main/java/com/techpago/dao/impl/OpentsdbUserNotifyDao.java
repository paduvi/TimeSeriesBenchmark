package com.techpago.dao.impl;

import com.sun.xml.internal.ws.api.config.management.policy.ManagementAssertion;
import com.techpago.config.Settings;
import com.techpago.dao.IUserNotifyDao;
import com.techpago.model.UserNotify;
import net.opentsdb.core.TSDB;
import net.opentsdb.tools.OpenTSDBMain;

import net.opentsdb.utils.Config;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
@Component
public class OpentsdbUserNotifyDao implements IUserNotifyDao {
    public OpentsdbUserNotifyDao(){
        TSDB tsdbWrite = createTsdb();
        TSDB tsdbRead = createTsdb();
    }
    @PostConstruct
    void init() throws IOException{

    }
    @Override
    public void insert(UserNotify userNotify) throws Exception {

    }

    @Override
    public CompletableFuture<Object> insertAsync(UserNotify userNotify) throws Exception {
        return null;
    }

    @Override
    public List<UserNotify> fetchDesc(String userID, Long fromTime) throws Exception {
        return null;
    }

    @Override
    public List<UserNotify> fetchAsc(String userID, Long fromTime) throws Exception {
        return null;
    }

    @Override
    public void flushDB() throws Exception {

    }

    private TSDB createTsdb(){
        Settings setting = Settings.getInstance();
        Config config = new Config();

        config.overrideConfig("tsd.network.port", "4242"); //The TCP port to use for accepting connections
        config.overrideConfig("tsd.http.staticroot", "/usr/share/opentsdb/static"); //Location of a directory where static files
        config.overrideConfig("tsd.http.cachedir", "/tmp/opentsdb"); //The full path to a location where temporary files can be written
        config.overrideConfig("tsd.core.auto_create_metrics", "true"); //Create new metrics or throw exception if it not exist.
        config.overrideConfig("tsd.core.meta.enable_tsuid_incrementing", "true");
        config.overrideConfig("tsd.storage.hbase.data_table", "tsdb");//Name of the HBase table where data points are stored
        config.overrideConfig("tsd.storage.hbase.uid_table", "tsdb-uid");//Name of the HBase table where UID information is stored
        config.overrideConfig("tsd.storage.hbase.zk_quorum",  String.join(",", setting.HBASE_IP)); //List of Zookeeper hosts that manage the HBase cluster
        config.overrideConfig("tsd.storage.fix_duplicates", "true");

        TSDB tsdb = new TSDB(config);
        return  tsdb;
    }
}
