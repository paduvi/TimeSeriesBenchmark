package io.dogy.dao.impl;

import io.dogy.config.Settings;
import io.dogy.dao.IUserNotifyDao;
import io.dogy.model.Pair;
import io.dogy.model.UserNotify;
import io.dogy.utility.Util;
import io.dogy.validator.IValidator;
import org.apache.commons.lang.StringUtils;
import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.DataPoint;
import org.kairosdb.client.builder.Metric;
import org.kairosdb.client.builder.MetricBuilder;
import org.kairosdb.client.builder.QueryBuilder;
import org.kairosdb.client.response.Queries;
import org.kairosdb.client.response.QueryResponse;
import org.kairosdb.client.response.Response;
import org.kairosdb.client.response.Results;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class KairosdbUserNotifyDao implements IUserNotifyDao {

    private static final Logger logger = LoggerFactory.getLogger(KairosdbUserNotifyDao.class);
    private final HttpClient client;
    private final MetricBuilder metricBuilder;
    private final QueryBuilder queryBuilder;
    private final Settings setting;
    private final BlockingQueue<Pair<UserNotify, CompletableFuture<Object>>> queue = new LinkedBlockingQueue<>();

    @Autowired
    private IValidator<UserNotify> validator;

    public KairosdbUserNotifyDao() throws Exception {
        setting = Settings.getInstance();

        String connectionString = String.format("http://%s:%s", setting.KAIROS_URL, setting.KAIROS_PORT);
        client = new HttpClient(connectionString);
        metricBuilder = MetricBuilder.getInstance();
        queryBuilder = QueryBuilder.getInstance();

        AtomicBoolean isAvailable = new AtomicBoolean(true);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> isAvailable.set(false)));
        for (int i = 0; i < Settings.getInstance().EVENT_LOOP_COUNT; i++) {
            CompletableFuture.runAsync(() -> {
                while (isAvailable.get()) {
                    List<Pair<UserNotify, CompletableFuture<Object>>> batch = new ArrayList<>();

                    try {
                        final int BATCH_SIZE = 100;
                        int n = queue.drainTo(batch, BATCH_SIZE);
                        if (n == 0) {
                            Thread.sleep(50);
                            continue;
                        }

                        for (Pair<UserNotify, CompletableFuture<Object>> pair : batch) {
                            UserNotify userNotify = pair._1;

                            Metric metric = metricBuilder.addMetric(setting.KAIROS_METRIC)
                                    .addTag("user_id", userNotify.getUserID())
                                    .addTag("notify_id", userNotify.getNotifyID());
                            metric.addDataPoint(userNotify.getTimestamp(), Util.OBJECT_MAPPER.writeValueAsString(userNotify));
                        }

                        Response response = client.pushMetrics(metricBuilder);
                        int statusCode = response.getStatusCode();
                        switch (statusCode) {
                            case 204:
                                break;
                            case 500:
                            case 400:
                                logger.info(String.valueOf(statusCode));
                                throw new Exception(StringUtils.join(response.getErrors(), "\n"));
                        }
                    } catch (Exception e) {
                        logger.error("Exception when insert kairosdb row: ", e);

                        for (Pair<UserNotify, CompletableFuture<Object>> pair : batch) {
                            pair._2.completeExceptionally(e);
                        }
                    } finally {
                        for (Pair<UserNotify, CompletableFuture<Object>> pair : batch) {
                            pair._2.complete(new Object());
                        }
                    }
                }
            });
        }
    }

    @Override
    public void insert(UserNotify userNotify) throws Exception {
        if (!validator.validate(userNotify)) {
            throw new RuntimeException("Invalid data");
        }
        Metric metric = metricBuilder.addMetric(setting.KAIROS_METRIC)
                .addTag("user_id", userNotify.getUserID())
                .addTag("notify_id", userNotify.getNotifyID());
        metric.addDataPoint(userNotify.getTimestamp(), Util.OBJECT_MAPPER.writeValueAsString(userNotify));
        Response response = client.pushMetrics(metricBuilder);

        int statusCode = response.getStatusCode();
        switch (statusCode) {
            case 204:
                break;
            case 500:
            case 400:
                logger.info(String.valueOf(statusCode));
                throw new Exception(StringUtils.join(response.getErrors(), "\n"));
        }
    }

    @Override
    public CompletableFuture<Object> insertAsync(UserNotify userNotify) throws Exception {
        if (!validator.validate(userNotify)) {
            throw new RuntimeException("Invalid data");
        }
        CompletableFuture<Object> future = new CompletableFuture<>();
        queue.add(new Pair<>(userNotify, future));
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

        List<UserNotify> userNotifyList = new ArrayList<>();

        int statusCode = client.pushMetrics(metricBuilder).getStatusCode();
        switch (statusCode) {
            case 204:
                for (Queries query : response.getQueries()) {
                    List<Results> results = query.getResults();
                    for (Results result : results) {
                        List<DataPoint> dataPoints = result.getDataPoints();
                        for (DataPoint dataPoint : dataPoints) {
                            userNotifyList.add(Util.OBJECT_MAPPER.readValue(dataPoint.stringValue(), UserNotify.class));
                        }
                    }
                }
                break;
            case 500:
            case 400:
                logger.info(String.valueOf(statusCode));
                throw new Exception(StringUtils.join(response.getErrors(), "\n"));
        }
        return userNotifyList;
    }

    @Override
    public List<UserNotify> fetchAsc(String userID, Long fromTime) throws Exception {
        if (fromTime == null) {
            fromTime = 0L;
        }

        queryBuilder.setStart(new Date(fromTime))
                .setEnd(new Date(System.currentTimeMillis()))
                .addMetric(setting.KAIROS_METRIC)
                .addTag("user_id", userID);
        QueryResponse response = client.query(queryBuilder);

        List<UserNotify> userNotifyList = new ArrayList<>();

        int statusCode = client.pushMetrics(metricBuilder).getStatusCode();
        switch (statusCode) {
            case 204:
                for (Queries query : response.getQueries()) {
                    List<Results> results = query.getResults();
                    for (Results result : results) {
                        List<DataPoint> dataPoints = result.getDataPoints();
                        for (DataPoint dataPoint : dataPoints) {
                            userNotifyList.add(Util.OBJECT_MAPPER.readValue(dataPoint.stringValue(), UserNotify.class));
                        }
                    }
                }
                break;
            case 500:
            case 400:
                logger.info(String.valueOf(statusCode));
                throw new Exception(StringUtils.join(response.getErrors(), "\n"));
        }
        return userNotifyList;
    }

    @Override
    public void flushDB() throws Exception {
        logger.info("Flushing " + setting.KAIROS_METRIC);
        client.deleteMetric(setting.KAIROS_METRIC);
    }

    public static void main(String[] args) {
        try {
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
