package com.techpago.service;

import com.techpago.dao.IUserNotifyDao;
import com.techpago.model.UserNotify;
import com.techpago.utility.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.*;
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
    private final int numBootstrap;

    public BenchmarkService(IUserNotifyDao userNotifyDao, int numWriteEpoch, int numWriteThread, int numFetchEpoch, int numFetchThread, int numBootstrap) {
        this.userNotifyDao = userNotifyDao;
        this.numWriteEpoch = numWriteEpoch;
        this.numWriteThread = numWriteThread;
        this.numFetchEpoch = numFetchEpoch;
        this.numFetchThread = numFetchThread;
        this.numBootstrap = numBootstrap;
    }

    public void bootstrap() throws Exception {
        this.userNotifyDao.flushDB();
        if (this.numBootstrap <= 0) {
            return;
        }

        ExecutorService executorService = Executors.newFixedThreadPool(500);
        CountDownLatch latch = new CountDownLatch(this.numBootstrap);
        for (int i = 0; i < Math.min(this.numBootstrap, this.numWriteThread); i++) {
            executorService.submit(() -> {
                try {
                    userNotifyDao.insert(UserNotify.createDumbObject());
                } catch (Exception e) {
                    logger.error("Error when insert: ", e);
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await();
    }

    public void benchmarkWrite() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(numWriteThread);
        AtomicInteger leftCount = new AtomicInteger(numWriteEpoch);

        long startTime = System.currentTimeMillis();
        for (int i = 0; i < numWriteThread; i++) {
            executorService.submit(() -> {
                while (leftCount.getAndDecrement() > 0) {
                    try {
                        userNotifyDao.insert(UserNotify.createDumbObject());
                    } catch (Exception e) {
                        logger.error("Error when insert: ", e);
                    }
                }
            });
        }

        executorService.shutdown();
        executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

        logger.info("Elapsed time insert: " + Util.formatDuration(System.currentTimeMillis() - startTime));
    }

    public void benchmarkWriteCallback() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(numWriteThread);
        AtomicInteger leftCount = new AtomicInteger(numWriteEpoch);

        long startTime = System.currentTimeMillis();
        for (int i = 0; i < numWriteThread; i++) {
            executorService.submit(() -> {
                while (leftCount.getAndDecrement() > 0) {
                    try {
                        userNotifyDao.insertAsync(UserNotify.createDumbObject()).get();
                    } catch (Exception e) {
                        logger.error("Error when insert: ", e);
                    }
                }
            });
        }

        executorService.shutdown();
        executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

        logger.info("Elapsed time insert bulk: " + Util.formatDuration(System.currentTimeMillis() - startTime));
    }

    public void benchmarkFetchAsc(long minTime, long maxTime) throws InterruptedException {
        ExecutorService fetchExecutorService = Executors.newFixedThreadPool(numFetchThread);
        AtomicInteger leftCount = new AtomicInteger(2 * numFetchEpoch);

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

        {
            AtomicInteger readCount = new AtomicInteger(numFetchEpoch);
            while (readCount.getAndDecrement() > 0) {
                fetchExecutorService.submit(() -> {
                    try {
                        String userID = String.valueOf(ThreadLocalRandom.current().nextLong(1000));
                        long temp = System.currentTimeMillis();
                        userNotifyDao.fetchAsc(userID, null);

                        totalFirstFetchTime.addAndGet(System.currentTimeMillis() - temp);
                    } catch (Exception e) {
                        logger.error("Error when fetch: ", e);
                    } finally {
                        leftCount.decrementAndGet();
                    }
                });
            }
        }
        Thread.sleep(500);
        {
            AtomicInteger readCount = new AtomicInteger(numFetchEpoch);
            while (readCount.getAndDecrement() > 0) {
                fetchExecutorService.submit(() -> {
                    try {
                        String userID = String.valueOf(ThreadLocalRandom.current().nextLong(1000));
                        long fromTime = ThreadLocalRandom.current().nextLong(minTime, maxTime);
                        long temp = System.currentTimeMillis();
                        userNotifyDao.fetchAsc(userID, fromTime);

                        totalFetchMoreTime.addAndGet(System.currentTimeMillis() - temp);
                    } catch (Exception e) {
                        logger.error("Error when fetch: ", e);
                    } finally {
                        leftCount.decrementAndGet();
                    }
                });
            }
        }

        fetchExecutorService.shutdown();
        fetchExecutorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

        writeExecutorService.shutdown();
        writeExecutorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

        logger.info("Avg time simultaneous first fetch asc: " + Util.formatDuration(totalFirstFetchTime.get() / numFetchEpoch));
        logger.info("Avg time simultaneous fetch more asc: " + Util.formatDuration(totalFetchMoreTime.get() / numFetchEpoch));
    }

    public void benchmarkFetchDesc(long minTime, long maxTime) throws InterruptedException {
        ExecutorService fetchExecutorService = Executors.newFixedThreadPool(numFetchThread);
        AtomicInteger leftCount = new AtomicInteger(2 * numFetchEpoch);

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

        {
            AtomicInteger readCount = new AtomicInteger(numFetchEpoch);
            while (readCount.getAndDecrement() > 0) {
                fetchExecutorService.submit(() -> {
                    try {
                        String userID = String.valueOf(ThreadLocalRandom.current().nextLong(1000));
                        long temp = System.currentTimeMillis();
                        userNotifyDao.fetchDesc(userID, null);

                        totalFirstFetchTime.addAndGet(System.currentTimeMillis() - temp);
                    } catch (Exception e) {
                        logger.error("Error when fetch: ", e);
                    } finally {
                        leftCount.decrementAndGet();
                    }
                });
            }
        }
        Thread.sleep(500);
        {
            AtomicInteger readCount = new AtomicInteger(numFetchEpoch);
            while (readCount.getAndDecrement() > 0) {
                fetchExecutorService.submit(() -> {
                    try {
                        String userID = String.valueOf(ThreadLocalRandom.current().nextLong(1000));
                        long fromTime = ThreadLocalRandom.current().nextLong(minTime, maxTime);
                        long temp = System.currentTimeMillis();
                        userNotifyDao.fetchDesc(userID, fromTime);

                        totalFetchMoreTime.addAndGet(System.currentTimeMillis() - temp);
                    } catch (Exception e) {
                        logger.error("Error when fetch: ", e);
                    } finally {
                        leftCount.decrementAndGet();
                    }
                });
            }
        }

        fetchExecutorService.shutdown();
        fetchExecutorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

        writeExecutorService.shutdown();
        writeExecutorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

        logger.info("Avg time simultaneous first fetch desc: " + Util.formatDuration(totalFirstFetchTime.get() / numFetchEpoch));
        logger.info("Avg time simultaneous fetch more desc: " + Util.formatDuration(totalFetchMoreTime.get() / numFetchEpoch));
    }

    public void benchmarkFetchAscRampUp(int seconds, long minTime, long maxTime) throws InterruptedException {
        ExecutorService fetchExecutorService = Executors.newFixedThreadPool(numFetchThread);
        AtomicInteger leftCount = new AtomicInteger(2 * numFetchEpoch);

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
                                String userID = String.valueOf(ThreadLocalRandom.current().nextLong(1000));
                                long temp = System.currentTimeMillis();
                                userNotifyDao.fetchAsc(userID, null);
                                totalFirstFetchTime.addAndGet(System.currentTimeMillis() - temp);
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
        Thread.sleep(500);
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
                                String userID = String.valueOf(ThreadLocalRandom.current().nextLong(1000));
                                long fromTime = ThreadLocalRandom.current().nextLong(minTime, maxTime);
                                long temp = System.currentTimeMillis();
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

        logger.info("Avg time RAMP first fetch asc: " + Util.formatDuration(totalFirstFetchTime.get() / numFetchEpoch));
        logger.info("Avg time RAMP fetch more asc: " + Util.formatDuration(totalFetchMoreTime.get() / numFetchEpoch));
    }

    public void benchmarkFetchDescRampUp(int seconds, long minTime, long maxTime) throws InterruptedException {
        ExecutorService fetchExecutorService = Executors.newFixedThreadPool(numFetchThread);
        AtomicInteger leftCount = new AtomicInteger(2 * numFetchEpoch);

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
                                String userID = String.valueOf(ThreadLocalRandom.current().nextLong(1000));
                                long temp = System.currentTimeMillis();
                                userNotifyDao.fetchDesc(userID, null);
                                totalFirstFetchTime.addAndGet(System.currentTimeMillis() - temp);
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
        Thread.sleep(500);
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
                                String userID = String.valueOf(ThreadLocalRandom.current().nextLong(1000));
                                long fromTime = ThreadLocalRandom.current().nextLong(minTime, maxTime);
                                long temp = System.currentTimeMillis();
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

        logger.info("Avg time RAMP first fetch desc: " + Util.formatDuration(totalFirstFetchTime.get() / numFetchEpoch));
        logger.info("Avg time RAMP fetch more desc: " + Util.formatDuration(totalFetchMoreTime.get() / numFetchEpoch));
    }

}
