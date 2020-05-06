package com.techpago.dao.impl;

import com.techpago.dao.IUserNotifyDao;
import com.techpago.model.UserNotify;

import org.kairosdb.client.builder.DataPoint;
import org.kairosdb.client.builder.Metric;
import org.kairosdb.client.builder.MetricBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.kairosdb.client.HttpClient;


@Component
public class KairosdbUserNotifyDao implements IUserNotifyDao {
    private static final Logger logger = LoggerFactory.getLogger(KairosdbUserNotifyDao.class);
    private String dbUrl = "localhost";
    private String port = "8083";
    private String metricName="test";
    HttpClient client;


    @Override
    public void insert(UserNotify userNotify) throws Exception {

    }
    public KairosdbUserNotifyDao() throws Exception {
       client = new HttpClient("https:"+dbUrl+":"+port);
       MetricBuilder metricBuilder = MetricBuilder.getInstance();
       Metric metric = metricBuilder.addMetric(metricName);
       client.pushMetrics(metricBuilder);

    }
    @Override
    public CompletableFuture<Object> insertAsync(UserNotify userNotify) throws Exception {
        CompletableFuture<String> future = new CompletableFuture<>();

        MetricBuilder metricBuilder = MetricBuilder.getInstance();
        Metric metric = metricBuilder.addMetric(metricName);
        metric.addTag("user_id", userNotify.getUserID());
        metric.addTag("notify_id", userNotify.getNotifyID());
        metric.addDataPoint(userNotify.getTimestamp(),userNotify.getData());
        client.pushMetrics(metricBuilder);

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
    //    private HttpClient createHttpClient(){
//
//    }
}
