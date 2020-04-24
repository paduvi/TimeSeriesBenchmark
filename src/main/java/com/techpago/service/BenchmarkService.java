package com.techpago.service;

import com.techpago.dao.IUserNotifyDao;
import com.techpago.model.UserNotify;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class BenchmarkService {

    private static final Logger logger = LoggerFactory.getLogger(BenchmarkService.class);

    private final IUserNotifyDao userNotifyDao;
    private final int numWriteEpoch;
    private final int numWriteThread;
    private final int numFetchEpoch;
    private final int numFetchThread;

    public BenchmarkService(IUserNotifyDao userNotifyDao, int numWriteEpoch, int numWriteThread, int numFetchEpoch, int numFetchThread) {
        this.userNotifyDao = userNotifyDao;
        this.numWriteEpoch = numWriteEpoch;
        this.numWriteThread = numWriteThread;
        this.numFetchEpoch = numFetchEpoch;
        this.numFetchThread = numFetchThread;
    }

    public void benchmarkWrite() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(numWriteThread);
        AtomicInteger leftCount = new AtomicInteger(numWriteEpoch);

        AtomicLong totalTime = new AtomicLong(0);
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < numWriteThread; i++) {
            executorService.submit(() -> {
                while (leftCount.getAndDecrement() > 0) {
                    try {
                        long temp = System.currentTimeMillis();
                        userNotifyDao.insert(UserNotify.createDumbObject());

                        totalTime.addAndGet(System.currentTimeMillis() - temp);
                    } catch (Exception e) {
                        logger.error("Error when insert: ", e);
                    }
                }
            });
        }

        executorService.shutdown();
        executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

        long processedTime = System.currentTimeMillis() - startTime;

        logger.info("Processed time insert: " + DurationFormatUtils.formatDurationHMS(processedTime) + "ms");
        logger.info("Total time insert: " + DurationFormatUtils.formatDurationHMS(totalTime.get()) + "ms");
        logger.info("Average time insert: " + DurationFormatUtils.formatDurationHMS(totalTime.get() / numWriteEpoch) + "ms");
    }

    public void benchmarkWriteCallback() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(numWriteThread);
        AtomicInteger leftCount = new AtomicInteger(numWriteEpoch);

        AtomicLong totalTime = new AtomicLong(0);
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < numWriteThread; i++) {
            executorService.submit(() -> {
                while (leftCount.getAndDecrement() > 0) {
                    try {
                        long temp = System.currentTimeMillis();
                        userNotifyDao.insertAsync(UserNotify.createDumbObject()).get();

                        totalTime.addAndGet(System.currentTimeMillis() - temp);
                    } catch (Exception e) {
                        logger.error("Error when insert: ", e);
                    }
                }
            });
        }

        executorService.shutdown();
        executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

        long processedTime = System.currentTimeMillis() - startTime;

        logger.info("Processed time insert callback: " + DurationFormatUtils.formatDurationHMS(processedTime) + "ms");
        logger.info("Total time insert callback: " + DurationFormatUtils.formatDurationHMS(totalTime.get()) + "ms");
        logger.info("Average time insert callback: " + DurationFormatUtils.formatDurationHMS(totalTime.get() / numWriteEpoch) + "ms");
    }

    public void benchmarkFetchAsc() throws InterruptedException {
        ExecutorService fetchExecutorService = Executors.newFixedThreadPool(numFetchThread);
        AtomicInteger leftCount = new AtomicInteger(numFetchEpoch);

        ExecutorService writeExecutorService = Executors.newFixedThreadPool(numWriteThread);
        for (int i = 0; i < numWriteThread; i++) {
            writeExecutorService.submit(() -> {
                while (leftCount.get() > 0) {
                    try {
                        userNotifyDao.insertAsync(UserNotify.createDumbObject()).get();
                    } catch (Exception e) {
                        logger.error("Error when insert: ", e);
                    }
                }
            });
        }

        AtomicLong totalFirstFetchTime = new AtomicLong(0);
        AtomicLong totalFetchMoreTime = new AtomicLong(0);
        AtomicInteger fetchMoreCount = new AtomicInteger(0);

        final Random random = new Random();

        for (int i = 0; i < numFetchThread; i++) {
            fetchExecutorService.submit(() -> {
                while (leftCount.getAndDecrement() > 0) {
                    try {
                        String userID = String.valueOf(random.nextInt(10000));
                        long temp = System.currentTimeMillis();
                        List<UserNotify> result = userNotifyDao.fetchAsc(userID, null);
                        totalFirstFetchTime.addAndGet(System.currentTimeMillis() - temp);

                        if (result.isEmpty()) {
                            continue;
                        }
                        Long fromTime = null;
                        for (UserNotify userNotify : result) {
                            fromTime = userNotify.getTimestamp();
                        }
                        temp = System.currentTimeMillis();
                        userNotifyDao.fetchAsc(userID, fromTime);

                        totalFetchMoreTime.addAndGet(System.currentTimeMillis() - temp);
                        fetchMoreCount.incrementAndGet();
                    } catch (Exception e) {
                        logger.error("Error when insert: ", e);
                    }
                }
            });
        }

        fetchExecutorService.shutdown();
        fetchExecutorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

        writeExecutorService.shutdown();
        writeExecutorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

        logger.info("Average time first fetch asc: " + DurationFormatUtils.formatDurationHMS(totalFirstFetchTime.get() / numFetchEpoch) + "ms");
        logger.info("Average time fetch more asc: " + DurationFormatUtils.formatDurationHMS(totalFetchMoreTime.get() / fetchMoreCount.get()) + "ms");
    }

    public void benchmarkFetchDesc() throws InterruptedException {
        ExecutorService fetchExecutorService = Executors.newFixedThreadPool(numFetchThread);
        AtomicInteger leftCount = new AtomicInteger(numFetchEpoch);

        ExecutorService writeExecutorService = Executors.newFixedThreadPool(numWriteThread);
        for (int i = 0; i < numWriteThread; i++) {
            writeExecutorService.submit(() -> {
                while (leftCount.get() > 0) {
                    try {
                        userNotifyDao.insertAsync(UserNotify.createDumbObject()).get();
                    } catch (Exception e) {
                        logger.error("Error when insert: ", e);
                    }
                }
            });
        }

        AtomicLong totalFirstFetchTime = new AtomicLong(0);
        AtomicLong totalFetchMoreTime = new AtomicLong(0);
        AtomicInteger fetchMoreCount = new AtomicInteger(0);

        final Random random = new Random();

        for (int i = 0; i < numFetchThread; i++) {
            fetchExecutorService.submit(() -> {
                while (leftCount.getAndDecrement() > 0) {
                    try {
                        String userID = String.valueOf(random.nextInt(10000));
                        long temp = System.currentTimeMillis();
                        List<UserNotify> result = userNotifyDao.fetchDesc(userID, null);
                        totalFirstFetchTime.addAndGet(System.currentTimeMillis() - temp);

                        if (result.isEmpty()) {
                            continue;
                        }
                        Long fromTime = null;
                        for (UserNotify userNotify : result) {
                            fromTime = userNotify.getTimestamp();
                        }
                        temp = System.currentTimeMillis();
                        userNotifyDao.fetchDesc(userID, fromTime);

                        totalFetchMoreTime.addAndGet(System.currentTimeMillis() - temp);
                        fetchMoreCount.incrementAndGet();
                    } catch (Exception e) {
                        logger.error("Error when insert: ", e);
                    }
                }
            });
        }

        fetchExecutorService.shutdown();
        fetchExecutorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

        writeExecutorService.shutdown();
        writeExecutorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

        logger.info("Average time first fetch desc: " + DurationFormatUtils.formatDurationHMS(totalFirstFetchTime.get() / numFetchEpoch) + "ms");
        logger.info("Average time fetch more desc: " + DurationFormatUtils.formatDurationHMS(totalFetchMoreTime.get() / fetchMoreCount.get()) + "ms");
    }

}
