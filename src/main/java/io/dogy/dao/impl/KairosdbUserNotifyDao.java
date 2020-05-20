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
    private String port = "8080";
    private String metricName = "test";
    private HttpClient client;
    private MetricBuilder metricBuilder;
    private QueryBuilder queryBuilder;


    public KairosdbUserNotifyDao() throws Exception {
        client = new HttpClient("http://" + dbUrl + ":" + port);
        metricBuilder = MetricBuilder.getInstance();
        queryBuilder = QueryBuilder.getInstance();

    }

    @Override
    public void insert(UserNotify userNotify) throws Exception {
        ;

//        HashMap<String, String> tags = new HashMap<>();
//        tags.put("user_id", userNotify.getUserID());
//        tags.put("notify_id", userNotify.getNotifyID());

        Metric metric = metricBuilder.addMetric(metricName)
                .addTag("user_id", userNotify.getUserID())
                .addTag("notify_id", userNotify.getNotifyID());
        metric.addDataPoint(userNotify.getTimestamp(), Util.OBJECT_MAPPER.writeValueAsString(userNotify.getData()));
        Response response = client.pushMetrics(metricBuilder);

        CompletableFuture<String> future = new CompletableFuture<>();
        int statusCode = client.pushMetrics(metricBuilder).getStatusCode();
        System.out.println(statusCode);
        switch (statusCode){
            case 204:
                System.out.println(("insert success"));
                future.complete("Success!");
                break;
            case 500:
            case 400:
                System.out.println(("insert failed"));
               List<String> errors = response.getErrors();
               System.out.println(StringUtils.join(errors,"\n"));
               future.completeExceptionally(new Throwable(StringUtils.join(errors,"\n")));
                break;
        }
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
            System.out.println(("insert success"));
            future.complete("Sent successful");
        } else {
            System.out.println(("insert failed"));
            future.completeExceptionally(new Throwable("Error"));
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
                System.out.println(("failed"));
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
                .addMetric(metricName)
                .addTag("user_id", userID);
        QueryResponse response = client.query(queryBuilder);

        CompletableFuture<String> future = new CompletableFuture<>();
        int statusCode = client.query(queryBuilder).getStatusCode();
        System.out.println(statusCode);
        List<UserNotify> userNotifyList = new ArrayList<>();
        switch (statusCode){
            case 200:
                System.out.println(("success"));
                future.complete("Success!");
                List<Results> results;
                List<DataPoint> dataPoints;
                for (Queries query : response.getQueries()){
                    System.out.println(query.toString());
                    results = query.getResults();
                    for(Results result:results){
                        System.out.println(result.toString());
                        dataPoints = result.getDataPoints();
                        System.out.println("num of dataPoint: "+dataPoints.size());
                        for(DataPoint dataPoint:dataPoints){
                            System.out.println(dataPoint.toString());
                            userNotifyList.add( Util.OBJECT_MAPPER.readValue(dataPoint.stringValue(), UserNotify.class ));
                        }
                    }
                }
                break;
            case 500:
                System.out.println(("failed 500"));
            case 400:
                System.out.println(("failed 400"));
                List<String> errors = response.getErrors();
                System.out.println(errors.size()+StringUtils.join(errors,"\n"));
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


           int i = 0;
            for (UserNotify result : kairos.fetchAsc("1", null)) {
                System.out.println(i++);
                System.out.println(result);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
