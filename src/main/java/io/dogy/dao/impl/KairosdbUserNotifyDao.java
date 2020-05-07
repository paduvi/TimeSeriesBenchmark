package io.dogy.dao.impl;

import io.dogy.dao.IUserNotifyDao;
import io.dogy.model.UserNotify;
import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.Metric;
import org.kairosdb.client.builder.MetricBuilder;
import org.kairosdb.client.response.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;


public class KairosdbUserNotifyDao implements IUserNotifyDao {
    private static final Logger logger = LoggerFactory.getLogger(KairosdbUserNotifyDao.class);
    private String dbUrl = "localhost";
    private String port = "8083";
    private String metricName = "test";
    HttpClient client;


    @Override
    public void insert(UserNotify userNotify) throws Exception {

    }

    public KairosdbUserNotifyDao() throws Exception {
        client = new HttpClient("https:" + dbUrl + ":" + port);
        MetricBuilder metricBuilder = MetricBuilder.getInstance();
        Metric metric = metricBuilder.addMetric(metricName);
        metric.addTag("user_id", "123456");
        client.pushMetrics(metricBuilder);
    }

    @Override
    public CompletableFuture<Object> insertAsync(UserNotify userNotify) throws Exception {
        CompletableFuture<Object> future = new CompletableFuture<>();

        MetricBuilder metricBuilder = MetricBuilder.getInstance();
        Metric metric = metricBuilder.addMetric(metricName);
        metric.addTag("user_id", userNotify.getUserID());
        metric.addTag("notify_id", userNotify.getNotifyID());
        metric.addDataPoint(userNotify.getTimestamp(), userNotify.getData());
        Response response = client.pushMetrics(metricBuilder);

        if (response.getErrors().isEmpty()) {
            future.complete("Sent successful");
        } else {
            future.completeExceptionally(new Throwable("Error"));
        }
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

    }
    //    private HttpClient createHttpClient(){
//
//    }
}
