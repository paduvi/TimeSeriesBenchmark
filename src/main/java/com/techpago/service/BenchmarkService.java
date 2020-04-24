package com.techpago.service;

import com.techpago.dao.IUserNotifyDao;
import com.techpago.model.UserNotify;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class BenchmarkService {

    private static final Logger logger = LoggerFactory.getLogger(BenchmarkService.class);

    private final IUserNotifyDao userNotifyDao;
    private final int numEpoch;
    private final int numThread;

    public BenchmarkService(IUserNotifyDao userNotifyDao, int numEpoch, int numThread) {
        this.userNotifyDao = userNotifyDao;
        this.numEpoch = numEpoch;
        this.numThread = numThread;
    }

    public void start() {
        try {
            benchmarkWrite();
        } catch (InterruptedException ignored) {
        }
    }

    private void benchmarkWrite() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(numThread);
        AtomicInteger leftCount = new AtomicInteger(numEpoch);

        AtomicLong totalTime = new AtomicLong(0);
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < numThread; i++) {
            executorService.submit(() -> {
                while (leftCount.getAndDecrement() > 0) {
                    try {
                        long temp = System.currentTimeMillis();
                        userNotifyDao.insert(UserNotify.createDumbObject());

                        totalTime.addAndGet(System.currentTimeMillis() - temp);
                        leftCount.decrementAndGet();
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
        logger.info("Average time insert: " + DurationFormatUtils.formatDurationHMS(totalTime.get() / numEpoch) + "ms");
    }

}
