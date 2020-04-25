package com.techpago.dao.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ServiceException;
import com.techpago.config.Settings;
import com.techpago.dao.IUserNotifyDao;
import com.techpago.model.Pair;
import com.techpago.model.UserNotify;
import com.techpago.utility.ByteUtil;
import com.techpago.utility.Util;
import com.techpago.validator.IValidator;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

@Component("HBaseUserNotifyDao")
public class HBaseUserNotifyDao implements IUserNotifyDao {

    private static final Logger logger = LoggerFactory.getLogger(HBaseUserNotifyDao.class);
    private final Connection writeConnection;
    private final Connection readConnection;
    private final BlockingQueue<Pair<UserNotify, CompletableFuture<Object>>> queue = new LinkedBlockingQueue<>();

    private final static String TABLE_NAME = Settings.getInstance().HBASE_TABLE;
    private final static Duration TTL = Duration.ofDays(3);
    private final static byte[] FAMILY = Bytes.toBytes("cf");

    private final static byte[] ID_COLUMN = Bytes.toBytes("notify_id");
    private final static byte[] USER_COLUMN = Bytes.toBytes("user_id");
    private final static byte[] TIMESTAMP_COLUMN = Bytes.toBytes("timestamp");
    private final static byte[] DATA_COLUMN = Bytes.toBytes("data");

    @Autowired
    private IValidator<UserNotify> validator;

    public HBaseUserNotifyDao() throws Exception {
        writeConnection = createConnection();
        readConnection = createConnection();
    }

    @PostConstruct
    void init() throws IOException {
        try (Admin admin = writeConnection.getAdmin()) {
            TableName tableName = TableName.valueOf(TABLE_NAME);
            if (!admin.tableExists(tableName)) {
                //creating table descriptor
                HTableDescriptor table = new HTableDescriptor(tableName);

                //creating column family descriptor
                HColumnDescriptor family = new HColumnDescriptor(FAMILY)
                        .setTimeToLive((int) TTL.getSeconds());
                if (Settings.getInstance().HBASE_COMPRESSION) {
                    family.setCompressionType(Compression.Algorithm.SNAPPY);
                }

                //adding column family to HTable
                table.addFamily(family);

                admin.createTable(table);
            }
        }

        AtomicBoolean isAvailable = new AtomicBoolean(true);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> isAvailable.set(false)));

        for (int i = 0; i < Settings.getInstance().EVENT_LOOP_COUNT; i++) {
            CompletableFuture.runAsync(() -> {
                while (isAvailable.get()) {
                    List<Pair<UserNotify, CompletableFuture<Object>>> batch = new ArrayList<>();

                    try {
                        final int BATCH_SIZE = 1000;
                        int n = queue.drainTo(batch, BATCH_SIZE);
                        if (n == 0) {
                            Thread.sleep(50);
                            continue;
                        }

                        List<Put> puts = new ArrayList<>();
                        for (Pair<UserNotify, CompletableFuture<Object>> pair : batch) {
                            puts.add(map2Put(pair._1));
                        }

                        try (Table table = writeConnection.getTable(TableName.valueOf(TABLE_NAME))) {
                            table.put(puts);
                        }
                    } catch (Exception e) {
                        logger.error("Exception when insert hbase record: ", e);

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
    public void flushDB() throws Exception {
        try (Admin admin = writeConnection.getAdmin()) {
            TableName tableName = TableName.valueOf(TABLE_NAME);
            if (admin.tableExists(tableName)) {
                admin.flush(tableName);
            }
        }
    }

    @Override
    public void insert(UserNotify userNotify) throws Exception {
        if (!validator.validate(userNotify)) {
            throw new RuntimeException("Invalid data");
        }
        try (Table table = writeConnection.getTable(TableName.valueOf(TABLE_NAME))) {
            table.put(map2Put(userNotify));
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
        byte[] prefix = Bytes.toBytes(serializeKey(userID) + ":");
        Filter filter = new PrefixFilter(prefix);

        Scan scan = new Scan();
        scan.setReversed(true);
        scan.setFilter(filter);
        scan.setCaching(10);
        scan.setLimit(20);
        if (fromTime != null) {
            scan.withStartRow(ArrayUtils.addAll(prefix, Bytes.toBytes(fromTime)), false);
        } else {
            scan.withStartRow(ArrayUtils.addAll(prefix, Bytes.toBytes(System.currentTimeMillis())));
        }
        scan.withStopRow(ArrayUtils.addAll(prefix, Bytes.toBytes(0)));

        try (Table table = readConnection.getTable(TableName.valueOf(TABLE_NAME))) {
            List<UserNotify> results = new ArrayList<>();

            try (ResultScanner scanner = table.getScanner(scan)) {
                for (Result r = scanner.next(); r != null; r = scanner.next()) {
                    if (r.isEmpty()) {
                        continue;
                    }
                    results.add(map2UserNotify(r));
                }
            }
            return results;
        }
    }

    @Override
    public List<UserNotify> fetchAsc(String userID, Long fromTime) throws Exception {
        byte[] prefix = Bytes.toBytes(serializeKey(userID) + ":");
        Filter filter = new PrefixFilter(prefix);

        Scan scan = new Scan();
        scan.setFilter(filter);
        scan.setCaching(10);
        scan.setLimit(20);
        if (fromTime != null) {
            scan.withStartRow(ArrayUtils.addAll(prefix, Bytes.toBytes(fromTime)), false);
        } else {
            scan.withStartRow(ArrayUtils.addAll(prefix, Bytes.toBytes(System.currentTimeMillis() - TTL.toMillis())));
        }

        try (Table table = readConnection.getTable(TableName.valueOf(TABLE_NAME))) {
            List<UserNotify> results = new ArrayList<>();

            try (ResultScanner scanner = table.getScanner(scan)) {
                for (Result r = scanner.next(); r != null; r = scanner.next()) {
                    if (r.isEmpty()) {
                        continue;
                    }
                    results.add(map2UserNotify(r));
                }
            }
            return results;
        }
    }

    private static Connection createConnection() throws IOException, ServiceException {
        Settings setting = Settings.getInstance();
        Configuration config = HBaseConfiguration.create();

        config.set("hbase.zookeeper.quorum", String.join(",", setting.HBASE_IP));
        config.setInt("hbase.zookeeper.property.clientPort", setting.HBASE_PORT);
        config.set("zookeeper.znode.parent", setting.HBASE_LOCATION);
        config.set("hbase.rpc.timeout", "10000");

        HBaseAdmin.checkHBaseAvailable(config);
        return ConnectionFactory.createConnection(config);
    }

    private static Put map2Put(UserNotify userNotify) throws JsonProcessingException {
        byte[] prefix = Bytes.toBytes(serializeKey(userNotify.getUserID()) + ":");
        byte[] row = ArrayUtils.addAll(prefix, Bytes.toBytes(userNotify.getTimestamp()));
        Put put = new Put(row);

        put.addColumn(FAMILY, ID_COLUMN, Bytes.toBytes(userNotify.getNotifyID()));
        put.addColumn(FAMILY, USER_COLUMN, Bytes.toBytes(userNotify.getUserID()));
        put.addColumn(FAMILY, TIMESTAMP_COLUMN, Bytes.toBytes(userNotify.getTimestamp()));
        put.addColumn(FAMILY, DATA_COLUMN, Util.OBJECT_MAPPER.writeValueAsBytes(userNotify.getData()));

        return put;
    }

    private static UserNotify map2UserNotify(Result r) throws IOException {
        UserNotify userNotify = new UserNotify();
        userNotify.setNotifyID(ByteUtil.toString(r.getValue(FAMILY, ID_COLUMN)));
        userNotify.setUserID(ByteUtil.toString(r.getValue(FAMILY, USER_COLUMN)));
        userNotify.setTimestamp(ByteUtil.toLong(r.getValue(FAMILY, TIMESTAMP_COLUMN)));
        userNotify.setData(Util.OBJECT_MAPPER.readValue(r.getValue(FAMILY, DATA_COLUMN), ObjectNode.class));
        return userNotify;
    }

    private static String serializeKey(String originalKey) {
        int mod = Math.abs(originalKey.hashCode() % Settings.getInstance().HBASE_SALT);
        int length = (int) Math.ceil(Math.log10(Settings.getInstance().HBASE_SALT));
        String prefix = String.format("%0" + length + "d", mod);
        return prefix + "-" + originalKey;
    }

}
