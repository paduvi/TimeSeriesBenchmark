package com.techpago.service;

import com.techpago.dao.IUserNotifyDao;
import com.techpago.model.UserNotify;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class BenchmarkService {

    private static final Logger logger = LoggerFactory.getLogger(BenchmarkService.class);
    private final ExecutorService executorService = Executors.newCachedThreadPool();

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
        for (int i = 0; i < numWriteThread; i++) {
            executorService.submit(() -> {
                while (leftCount.getAndDecrement() > 0) {
                    try {
                        if (leftCount.get() % 100 == 0) {
                            Thread.sleep(10);
                        }
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

        logger.info("Average time insert: " + (totalTime.get() / numWriteEpoch) + "ms");
    }

    public void benchmarkWriteCallback() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(numWriteThread);
        AtomicInteger leftCount = new AtomicInteger(numWriteEpoch);

        AtomicLong totalTime = new AtomicLong(0);
        for (int i = 0; i < numWriteThread; i++) {
            executorService.submit(() -> {
                while (leftCount.getAndDecrement() > 0) {
                    try {
                        if (leftCount.get() % 100 == 0) {
                            Thread.sleep(10);
                        }
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

        logger.info("Average time insert callback: " + (totalTime.get() / numWriteEpoch) + "ms");
    }

    public void benchmarkFetchAsc() throws InterruptedException {
        ExecutorService fetchExecutorService = Executors.newFixedThreadPool(numFetchThread);
        AtomicInteger leftCount = new AtomicInteger(numFetchEpoch);

        ExecutorService writeExecutorService = Executors.newFixedThreadPool(numWriteThread);
        for (int i = 0; i < numWriteThread; i++) {
            writeExecutorService.submit(() -> {
                while (leftCount.get() > 0) {
                    try {
                        if (leftCount.get() % 100 == 0) {
                            Thread.sleep(10);
                        }
                        userNotifyDao.insertAsync(UserNotify.createDumbObject()).get();
                    } catch (Exception e) {
                        logger.error("Error when insert: ", e);
                    }
                }
            });
        }

        AtomicLong totalFirstFetchTime = new AtomicLong(0);
        AtomicLong totalFetchMoreTime = new AtomicLong(0);

        final Random random = new Random();
        AtomicInteger readCount = new AtomicInteger(numFetchEpoch);
        while (readCount.getAndDecrement() > 0) {
            fetchExecutorService.submit(() -> {
                try {
                    String userID = String.valueOf(random.nextInt(10000));
                    long temp = System.currentTimeMillis();
                    List<UserNotify> result = userNotifyDao.fetchAsc(userID, null);
                    totalFirstFetchTime.addAndGet(System.currentTimeMillis() - temp);

                    long fromTime = temp;
                    for (UserNotify userNotify : result) {
                        fromTime = userNotify.getTimestamp();
                    }

                    userID = String.valueOf(random.nextInt(10000));
                    temp = System.currentTimeMillis();
                    userNotifyDao.fetchAsc(userID, fromTime);

                    totalFetchMoreTime.addAndGet(System.currentTimeMillis() - temp);
                } catch (Exception e) {
                    logger.error("Error when fetch: ", e);
                } finally {
                    leftCount.decrementAndGet();
                }
            });
        }

        fetchExecutorService.shutdown();
        fetchExecutorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

        writeExecutorService.shutdown();
        writeExecutorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

        logger.info("Avg time simultaneous first fetch asc: " + (totalFirstFetchTime.get() / numFetchEpoch) + "ms");
        logger.info("Avg time simultaneous fetch more asc: " + (totalFetchMoreTime.get() / numFetchEpoch) + "ms");
    }

    public void benchmarkFetchDesc() throws InterruptedException {
        ExecutorService fetchExecutorService = Executors.newFixedThreadPool(numFetchThread);
        AtomicInteger leftCount = new AtomicInteger(numFetchEpoch);

        ExecutorService writeExecutorService = Executors.newFixedThreadPool(numWriteThread);
        for (int i = 0; i < numWriteThread; i++) {
            writeExecutorService.submit(() -> {
                while (leftCount.get() > 0) {
                    try {
                        if (leftCount.get() % 100 == 0) {
                            Thread.sleep(10);
                        }
                        userNotifyDao.insertAsync(UserNotify.createDumbObject()).get();
                    } catch (Exception e) {
                        logger.error("Error when insert: ", e);
                    }
                }
            });
        }

        AtomicLong totalFirstFetchTime = new AtomicLong(0);
        AtomicLong totalFetchMoreTime = new AtomicLong(0);

        final Random random = new Random();
        AtomicInteger readCount = new AtomicInteger(numFetchEpoch);
        while (readCount.getAndDecrement() > 0) {
            fetchExecutorService.submit(() -> {
                try {
                    String userID = String.valueOf(random.nextInt(10000));
                    long temp = System.currentTimeMillis();
                    List<UserNotify> result = userNotifyDao.fetchDesc(userID, null);
                    totalFirstFetchTime.addAndGet(System.currentTimeMillis() - temp);

                    long fromTime = temp;
                    for (UserNotify userNotify : result) {
                        fromTime = userNotify.getTimestamp();
                    }

                    userID = String.valueOf(random.nextInt(10000));
                    userNotifyDao.fetchDesc(userID, fromTime);

                    totalFetchMoreTime.addAndGet(System.currentTimeMillis() - temp);
                } catch (Exception e) {
                    logger.error("Error when fetch: ", e);
                } finally {
                    leftCount.decrementAndGet();
                }
            });
        }

        fetchExecutorService.shutdown();
        fetchExecutorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

        writeExecutorService.shutdown();
        writeExecutorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

        logger.info("Avg time simultaneous first fetch desc: " + (totalFirstFetchTime.get() / numFetchEpoch) + "ms");
        logger.info("Avg time simultaneous fetch more desc: " + (totalFetchMoreTime.get() / numFetchEpoch) + "ms");
    }

    public void benchmarkFetchAscRampUp(int seconds) throws InterruptedException {
        ExecutorService fetchExecutorService = Executors.newFixedThreadPool(numFetchThread);
        AtomicInteger leftCount = new AtomicInteger(numFetchEpoch);

        ExecutorService writeExecutorService = Executors.newFixedThreadPool(numWriteThread);
        for (int i = 0; i < numWriteThread; i++) {
            writeExecutorService.submit(() -> {
                while (leftCount.get() > 0) {
                    try {
                        if (leftCount.get() % 100 == 0) {
                            Thread.sleep(10);
                        }
                        userNotifyDao.insertAsync(UserNotify.createDumbObject()).get();
                    } catch (Exception e) {
                        logger.error("Error when insert: ", e);
                    }
                }
            });
        }

        AtomicLong totalFirstFetchTime = new AtomicLong(0);
        AtomicLong totalFetchMoreTime = new AtomicLong(0);

        final Random random = new Random();

        for (int i = 0; i < numFetchThread; i++) {
            final AtomicInteger count = new AtomicInteger(numFetchEpoch / numFetchThread);
            if (i == numFetchThread - 1) {
                count.addAndGet(numFetchEpoch % numFetchThread);
            }
            long interval = Duration.ofSeconds(seconds).toMillis() / count.get();
            fetchExecutorService.submit(() -> {
                while (count.getAndDecrement() > 0) {
                    try {
                        CompletableFuture.runAsync(() -> {
                            try {
                                String userID = String.valueOf(random.nextInt(10000));
                                long temp = System.currentTimeMillis();
                                List<UserNotify> result = userNotifyDao.fetchAsc(userID, null);
                                totalFirstFetchTime.addAndGet(System.currentTimeMillis() - temp);

                                long fromTime = temp;
                                for (UserNotify userNotify : result) {
                                    fromTime = userNotify.getTimestamp();
                                }

                                userID = String.valueOf(random.nextInt(10000));
                                temp = System.currentTimeMillis();
                                userNotifyDao.fetchAsc(userID, fromTime);

                                totalFetchMoreTime.addAndGet(System.currentTimeMillis() - temp);
                            } catch (Exception e) {
                                logger.error("Error when fetch: ", e);
                            } finally {
                                leftCount.getAndDecrement();
                            }
                        }, executorService);
                        Thread.sleep(interval);
                    } catch (InterruptedException ignored) {
                    }
                }
            });
        }

        fetchExecutorService.shutdown();
        fetchExecutorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

        writeExecutorService.shutdown();
        writeExecutorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

        logger.info("Avg time RAMP first fetch asc: " + (totalFirstFetchTime.get() / numFetchEpoch) + "ms");
        logger.info("Avg time RAMP fetch more asc: " + (totalFetchMoreTime.get() / numFetchEpoch) + "ms");
    }

    public void benchmarkFetchDescRampUp(int seconds) throws InterruptedException {
        ExecutorService fetchExecutorService = Executors.newFixedThreadPool(numFetchThread);
        AtomicInteger leftCount = new AtomicInteger(numFetchEpoch);

        ExecutorService writeExecutorService = Executors.newFixedThreadPool(numWriteThread);
        for (int i = 0; i < numWriteThread; i++) {
            writeExecutorService.submit(() -> {
                while (leftCount.get() > 0) {
                    try {
                        if (leftCount.get() % 100 == 0) {
                            Thread.sleep(10);
                        }
                        userNotifyDao.insertAsync(UserNotify.createDumbObject()).get();
                    } catch (Exception e) {
                        logger.error("Error when insert: ", e);
                    }
                }
            });
        }

        AtomicLong totalFirstFetchTime = new AtomicLong(0);
        AtomicLong totalFetchMoreTime = new AtomicLong(0);

        final Random random = new Random();

        for (int i = 0; i < numFetchThread; i++) {
            final AtomicInteger count = new AtomicInteger(numFetchEpoch / numFetchThread);
            if (i == numFetchThread - 1) {
                count.addAndGet(numFetchEpoch % numFetchThread);
            }
            long interval = Duration.ofSeconds(seconds).toMillis() / count.get();
            fetchExecutorService.submit(() -> {
                while (count.getAndDecrement() > 0) {
                    try {
                        CompletableFuture.runAsync(() -> {
                            try {
                                String userID = String.valueOf(random.nextInt(10000));
                                long temp = System.currentTimeMillis();
                                List<UserNotify> result = userNotifyDao.fetchDesc(userID, null);
                                totalFirstFetchTime.addAndGet(System.currentTimeMillis() - temp);

                                long fromTime = temp;
                                for (UserNotify userNotify : result) {
                                    fromTime = userNotify.getTimestamp();
                                }

                                userID = String.valueOf(random.nextInt(10000));
                                temp = System.currentTimeMillis();
                                userNotifyDao.fetchDesc(userID, fromTime);

                                totalFetchMoreTime.addAndGet(System.currentTimeMillis() - temp);
                            } catch (Exception e) {
                                logger.error("Error when fetch: ", e);
                            } finally {
                                leftCount.getAndDecrement();
                            }
                        }, executorService);
                        Thread.sleep(interval);
                    } catch (InterruptedException ignored) {
                    }
                }
            });
        }

        fetchExecutorService.shutdown();
        fetchExecutorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

        writeExecutorService.shutdown();
        writeExecutorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

        logger.info("Avg time RAMP first fetch desc: " + (totalFirstFetchTime.get() / numFetchEpoch) + "ms");
        logger.info("Avg time RAMP fetch more desc: " + (totalFetchMoreTime.get() / numFetchEpoch) + "ms");
    }

}
