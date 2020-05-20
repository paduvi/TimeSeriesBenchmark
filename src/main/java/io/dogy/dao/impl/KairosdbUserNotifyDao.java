package io.dogy.dao.impl;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.dogy.config.Settings;
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
    private HttpClient client;
    private MetricBuilder metricBuilder;
    private QueryBuilder queryBuilder;
    private Settings setting;


    public KairosdbUserNotifyDao() throws Exception {
        setting = Settings.getInstance();

        String connectionString = String.format("http://%s:%s",setting.KAIROS_URL,setting.KAIROS_PORT);
        client = new HttpClient(connectionString);
        metricBuilder = MetricBuilder.getInstance();
        queryBuilder = QueryBuilder.getInstance();

    }

    @Override
    public void insert(UserNotify userNotify) throws Exception {

        Metric metric = metricBuilder.addMetric(setting.KAIROS_METRIC)
                .addTag("user_id", userNotify.getUserID())
                .addTag("notify_id", userNotify.getNotifyID());
        metric.addDataPoint(userNotify.getTimestamp(), Util.OBJECT_MAPPER.writeValueAsString(userNotify));
        Response response = client.pushMetrics(metricBuilder);

        CompletableFuture<String> future = new CompletableFuture<>();
        int statusCode = client.pushMetrics(metricBuilder).getStatusCode();
        System.out.println(statusCode);
        switch (statusCode){
            case 204:
                future.complete("Success!");
                break;
            case 500:
            case 400:
                logger.info(String.valueOf(statusCode));
               List<String> errors = response.getErrors();
               future.completeExceptionally(new Throwable(StringUtils.join(errors,"\n")));
                break;
        }
    }



    @Override
    public CompletableFuture<Object> insertAsync(UserNotify userNotify) throws Exception {
        Metric metric = metricBuilder.addMetric(setting.KAIROS_METRIC)
                .addTag("user_id", userNotify.getUserID())
                .addTag("notify_id", userNotify.getNotifyID());
        metric.addDataPoint(userNotify.getTimestamp(), Util.OBJECT_MAPPER.writeValueAsString(userNotify));
        Response response = client.pushMetrics(metricBuilder);

        CompletableFuture<Object> future = new CompletableFuture<>();
        int statusCode = client.pushMetrics(metricBuilder).getStatusCode();
        switch (statusCode){
            case 204:
                future.complete("Success!");
                break;
            case 500:
            case 400:
                List<String> errors = response.getErrors();
                future.completeExceptionally(new Throwable(StringUtils.join(errors,"\n")));
                break;
        }
        return future;
    }


    @Override
    public List<UserNotify> fetchDesc(String userID, Long fromTime) throws Exception {

        if (fromTime == null) {
            fromTime = System.currentTimeMillis();
        }

        queryBuilder.setStart(null)
                .setEnd(new Date(fromTime))
                .addMetric(setting.KAIROS_METRIC)
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
                logger.info(String.valueOf(statusCode));
                List<String> errors = response.getErrors();
                System.out.println(StringUtils.join(errors,"\n"));
                future.completeExceptionally(new Throwable(StringUtils.join(errors,"\n")));
                break;
        }
        return userNotifyList;
    }

    @Override
    public List<UserNotify> fetchAsc(String userID, Long fromTime) throws Exception {

        if (fromTime==null)
        {
            fromTime =0L;
        }

        queryBuilder.setStart(new Date(fromTime))
                .setEnd(new Date(System.currentTimeMillis()))
                .addMetric(setting.KAIROS_METRIC)
                .addTag("user_id", userID);
        QueryResponse response = client.query(queryBuilder);

        CompletableFuture<String> future = new CompletableFuture<>();
        int statusCode = client.query(queryBuilder).getStatusCode();
        List<UserNotify> userNotifyList = new ArrayList<>();
        switch (statusCode){
            case 200:
                future.complete("Success!");
                List<Results> results;
                List<DataPoint> dataPoints;
                for (Queries query : response.getQueries()){
                    results = query.getResults();
                    for(Results result:results){
                        dataPoints = result.getDataPoints();
                        for(DataPoint dataPoint:dataPoints){
                            System.out.println(dataPoints.toString());
                            userNotifyList.add( Util.OBJECT_MAPPER.readValue(dataPoint.stringValue(), UserNotify.class ));
                        }
                    }
                }
                break;
            case 500:
            case 400:
                logger.info(String.valueOf(statusCode));
                List<String> errors = response.getErrors();
                future.completeExceptionally(new Throwable(StringUtils.join(errors,"\n")));
        }
        return userNotifyList;
    }

    @Override
    public void flushDB() throws Exception {
        logger.info("Flushing "+setting.KAIROS_METRIC);
        client.deleteMetric(setting.KAIROS_METRIC);
    }

    public static void main (String[] args){
        try{
            KairosdbUserNotifyDao kairos = new KairosdbUserNotifyDao();
            kairos.flushDB();
            for (int i = 0; i < 10; i++) {
                UserNotify userNotify = new UserNotify();
                userNotify.setNotifyID(String.valueOf(i));
                userNotify.setUserID(String.valueOf(i % 5));
                userNotify.setData(Util.OBJECT_MAPPER.createObjectNode());
                userNotify.setTimestamp(System.currentTimeMillis());
                System.out.println(Util.OBJECT_MAPPER.writeValueAsString(userNotify));
                kairos.insert(userNotify);
            }

            for (UserNotify result : kairos.fetchAsc("1", null)) {
                System.out.println(result);
            }
            kairos.flushDB();

            for (UserNotify result : kairos.fetchAsc("1", null)) {
                System.out.println(result);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
