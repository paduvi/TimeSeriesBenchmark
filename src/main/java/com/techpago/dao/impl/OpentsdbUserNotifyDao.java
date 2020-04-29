package com.techpago.dao.impl;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import com.techpago.config.Settings;
import com.techpago.dao.IUserNotifyDao;
import com.techpago.model.Pair;
import com.techpago.model.UserNotify;
import com.techpago.validator.IValidator;
import net.opentsdb.core.TSDB;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import net.opentsdb.utils.Config;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

@Component
public class OpentsdbUserNotifyDao implements IUserNotifyDao {

    private final TSDB tsdb;
    private final BlockingQueue<Pair<UserNotify, CompletableFuture<Object>>> queue = new LinkedBlockingQueue<>();

    @Autowired
    private IValidator<UserNotify> validator;

    public OpentsdbUserNotifyDao() {
        Settings setting = Settings.getInstance();
        Config config = new Config();

        config.overrideConfig("tsd.network.port", "4242"); //The TCP port to use for accepting connections
        config.overrideConfig("tsd.http.staticroot", "/usr/share/opentsdb/static"); //Location of a directory where static files
        config.overrideConfig("tsd.http.cachedir", "/tmp/opentsdb"); //The full path to a location where temporary files can be written
        config.overrideConfig("tsd.core.auto_create_metrics", "true"); //Create new metrics or throw exception if it not exist.
        config.overrideConfig("tsd.core.meta.enable_tsuid_incrementing", "true");
        config.overrideConfig("tsd.storage.hbase.data_table", "tsdb");//Name of the HBase table where data points are stored
        config.overrideConfig("tsd.storage.hbase.uid_table", "tsdb-uid");//Name of the HBase table where UID information is stored
        config.overrideConfig("tsd.storage.hbase.zk_quorum", String.join(",", setting.HBASE_IP)); //List of Zookeeper hosts that manage the HBase cluster
        config.overrideConfig("tsd.storage.fix_duplicates", "true");
        this.tsdb = new TSDB(config);
    }

    @Override
    public void insert(UserNotify userNotify) throws Exception {
        CompletableFuture<Object> future = new CompletableFuture<>();
        validator.validate(userNotify);
        // Write a number of data points at 30 second intervals. Each write will
        // return a deferred (similar to a Java Future or JS Promise) that will
        // be called on completion with either a "null" value on success or an
        // exception.
        String metricName = "";

        long value = userNotify.getTimestamp();
        Map<String, String> tags = new HashMap<>();
        tags.put("user_id", userNotify.getUserID());
        tags.put("notify_id", userNotify.getNotifyID());

        Deferred<Object> deferred = tsdb
                .addPoint(metricName, userNotify.getTimestamp(), value, tags)
                .addBoth(new BothCallBack(future));
        deferred.join();
        future.get();
    }

    //    private void createDB(TSDB tsdb) throws IOException{
//        // Declare new metric
//        String metricName = "my.tsdb.test.metric";
//        // First check to see it doesn't already exist
//        byte[] byteMetricUID; // we don't actually need this for the first
//        // .addPoint() call below.
//        // TODO: Ideally we could just call a not-yet-implemented tsdb.uIdExists()
//        // function.
//        // Note, however, that this is optional. If auto metric is enabled
//        // (tsd.core.auto_create_metrics), the UID will be assigned in call to
//        // addPoint().
//        try {
//            byteMetricUID = tsdb.getUID(UniqueIdType.METRIC, metricName);
//        } catch (IllegalArgumentException iae) {
//            System.out.println("Metric name not valid.");
//            iae.printStackTrace();
//            System.exit(1);
//        } catch (NoSuchUniqueName nsune) {
//            // If not, great. Create it.
//            byteMetricUID = tsdb.assignUid("metric", metricName);
//        }
//
//        // Make a single datum
//        long timestamp = System.currentTimeMillis() / 1000;
//        long value = 314159;
//        // Make key-val
//    }
    @Override
    public CompletableFuture<Object> insertAsync(UserNotify userNotify) throws Exception {
        validator.validate(userNotify);
        CompletableFuture<Object> future = new CompletableFuture<>();
        queue.add(new Pair<>(userNotify, future));
        return future;
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
        // Declare new metric
        String metricName = "my.tsdb.test.metric";
        // First check to see it doesn't already exist
        byte[] byteMetricUID; // we don't actually need this for the first
        // .addPoint() call below.
        // TODO: Ideally we could just call a not-yet-implemented tsdb.uIdExists()
        // function.
        // Note, however, that this is optional. If auto metric is enabled
        // (tsd.core.auto_create_metrics), the UID will be assigned in call to
        // addPoint().
        try {
            byteMetricUID = tsdb.getUID(UniqueIdType.METRIC, metricName);
        } catch (IllegalArgumentException iae) {
            System.out.println("Metric name not valid.");
            iae.printStackTrace();
            System.exit(1);
        } catch (NoSuchUniqueName nsune) {
            // If not, great. Create it.
            byteMetricUID = tsdb.assignUid("metric", metricName);
        }
    }

    private static class BothCallBack implements Callback<Object, Object> {
        private final CompletableFuture<Object> future;

        public BothCallBack(CompletableFuture<Object> future) {
            this.future = future;
        }

        @Override
        public Object call(Object arg) {
            if (arg instanceof Exception) {
                Exception e = (Exception) arg;
                future.completeExceptionally(e);
                return e.getMessage();
            }
            future.complete(arg);
            return arg;
        }
    }

}
