package io.dogy.dao.impl;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.dogy.dao.IUserNotifyDao;
import io.dogy.model.UserNotify;
import io.dogy.utility.Util;
import org.apache.commons.lang.StringUtils;
import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.*;
import org.kairosdb.client.response.Queries;
import org.kairosdb.client.response.QueryResponse;
import org.kairosdb.client.response.Response;
import org.kairosdb.client.response.Results;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;


public class KairosdbUserNotifyDao implements IUserNotifyDao {
    private static final Logger logger = LoggerFactory.getLogger(KairosdbUserNotifyDao.class);
    private String dbUrl = "localhost";
    private String port = "8083";
    private String metricName = "test";
    private HttpClient client;
    private MetricBuilder metricBuilder;
    private QueryBuilder queryBuilder;
    private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm");


    @Override
    public void insert(UserNotify userNotify) throws Exception {
        HashMap<String, String> tags = new HashMap<>();
        tags.put("user_id", userNotify.getUserID());
        tags.put("notify_id", userNotify.getNotifyID());
        metricBuilder.getMetrics().get(0).addTags(tags).addDataPoint(userNotify.getTimestamp(), userNotify.getData());

        Response response = client.pushMetrics(metricBuilder);

        CompletableFuture<String> future = new CompletableFuture<>();
        int statusCode = client.pushMetrics(metricBuilder).getStatusCode();
        switch (statusCode){
            case 204:
                future.complete("Success!");
                break;
            case 500:
            case 400:
               List<String> errors = response.getErrors();
               future.completeExceptionally(new Throwable(StringUtils.join(errors,"\n")));
        }
    }

    public KairosdbUserNotifyDao() throws Exception {
        client = new HttpClient("https:" + dbUrl + ":" + port);
        metricBuilder = MetricBuilder.getInstance();
        Metric metric = metricBuilder.addMetric(metricName);
//        metric.addTag("user_id", "123456");
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

        queryBuilder.setStart(null)
                .setEnd(new Date(fromTime))
                .addMetric(metricName)
                .addTag("user_id", userID);
        QueryResponse response = client.query(queryBuilder);

        CompletableFuture<String> future = new CompletableFuture<>();
        int statusCode = client.pushMetrics(metricBuilder).getStatusCode();
        List<UserNotify> userNotifyList = new ArrayList<>();
        switch (statusCode){
            case 204:
                future.complete("Success!");
                List<Results> results;
                List<DataPoint> dataPoints;
                for (Queries query : response.getQueries()){
                    results = query.getResults();
                    for(Results result:results){
                        dataPoints = result.getDataPoints();
                        for(DataPoint dataPoint:dataPoints){
                            userNotifyList.add( Util.OBJECT_MAPPER.readValue(dataPoint.stringValue(), UserNotify.class ));
                        }
                    }
                }
                break;
            case 500:
            case 400:
                List<String> errors = response.getErrors();
                future.completeExceptionally(new Throwable(StringUtils.join(errors,"\n")));
        }
        return userNotifyList;
    }

    @Override
    public List<UserNotify> fetchAsc(String userID, Long fromTime) throws Exception {
        queryBuilder.setStart(new Date(fromTime))
                .setEnd(new Date(System.currentTimeMillis()))
                .addMetric(metricName)
                .addTag("user_id", userID);
        QueryResponse response = client.query(queryBuilder);

        CompletableFuture<String> future = new CompletableFuture<>();
        int statusCode = client.pushMetrics(metricBuilder).getStatusCode();
        List<UserNotify> userNotifyList = new ArrayList<>();
        switch (statusCode){
            case 204:
                future.complete("Success!");
                List<Results> results;
                List<DataPoint> dataPoints;
                for (Queries query : response.getQueries()){
                    results = query.getResults();
                    for(Results result:results){
                        dataPoints = result.getDataPoints();
                        for(DataPoint dataPoint:dataPoints){
                            userNotifyList.add( Util.OBJECT_MAPPER.readValue(dataPoint.stringValue(), UserNotify.class ));
                        }
                    }
                }
                break;
            case 500:
            case 400:
                List<String> errors = response.getErrors();
                future.completeExceptionally(new Throwable(StringUtils.join(errors,"\n")));
        }
        return userNotifyList;
    }

    @Override
    public void flushDB() throws Exception {
        client.deleteMetric(metricName);
    }
    //    private HttpClient createHttpClient(){
//
//    }
}
