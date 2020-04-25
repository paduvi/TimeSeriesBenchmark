package com.techpago.dao.impl;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import com.techpago.config.Settings;
import com.techpago.dao.IUserNotifyDao;
import com.techpago.model.Pair;
import com.techpago.model.UserNotify;
import com.techpago.utility.Util;
import com.techpago.validator.IValidator;
import org.apache.commons.dbcp2.BasicDataSource;
import org.postgresql.Driver;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

@Component("TimescaledbUserNotifyDao")
public class TimescaledbUserNotifyDao implements IUserNotifyDao {

    private static final Logger logger = LoggerFactory.getLogger(TimescaledbUserNotifyDao.class);
    private final BlockingQueue<Pair<UserNotify, CompletableFuture<Object>>> queue = new LinkedBlockingQueue<>();

    private final JdbcTemplate jdbcWriteTemplate;
    private final JdbcTemplate jdbcReadTemplate;
    private static final String TABLE_NAME = "\"" + Settings.getInstance().TIMESCALEDB_TABLE + "\"";
    private static final RowMapper<UserNotify> ROW_MAPPER = new UserNotifyRowMapper();

    @Autowired
    private IValidator<UserNotify> validator;

    public TimescaledbUserNotifyDao() {
        jdbcWriteTemplate = createJdbcTemplate();
        jdbcReadTemplate = createJdbcTemplate();
    }

    @PostConstruct
    void init() {
        // create table if not exists
        jdbcWriteTemplate.execute(String.format(
                "CREATE TABLE IF NOT EXISTS %s (" +
                        "timestamp      TIMESTAMPTZ     NOT NULL," +
                        "user_id         TEXT            NOT NULL," +
                        "notify_id       TEXT            NOT NULL," +
                        "data           JSONB" +
                        ")",
                TABLE_NAME));

        // create hypertable if not exists
        jdbcWriteTemplate.execute(String.format("SELECT create_hypertable('%s', 'timestamp', if_not_exists => TRUE)",
                TABLE_NAME));
        jdbcWriteTemplate.execute(String.format("TRUNCATE TABLE %s", TABLE_NAME));

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

                        List<Pair<UserNotify, PGobject>> list = new ArrayList<>();
                        for (Pair<UserNotify, CompletableFuture<Object>> pair : batch) {
                            PGobject data = new PGobject();
                            data.setType("jsonb");
                            data.setValue(Util.OBJECT_MAPPER.writeValueAsString(pair._1.getData()));

                            list.add(new Pair<>(pair._1, data));
                        }

                        String sql = String.format("INSERT INTO %s(timestamp, user_id, notify_id, data)" +
                                "  VALUES (?, ?, ?, ?)", TABLE_NAME);

                        jdbcWriteTemplate.batchUpdate(sql,
                                new BatchPreparedStatementSetter() {
                                    public void setValues(PreparedStatement ps, int i) throws SQLException {
                                        Pair<UserNotify, PGobject> pair = list.get(i);
                                        UserNotify userNotify = pair._1;
                                        PGobject data = pair._2;

                                        ps.setTimestamp(1, new Timestamp(userNotify.getTimestamp()), Calendar.getInstance());
                                        ps.setString(2, userNotify.getUserID());
                                        ps.setString(3, userNotify.getNotifyID());
                                        ps.setObject(4, data);
                                    }

                                    public int getBatchSize() {
                                        return list.size();
                                    }
                                });
                    } catch (Exception e) {
                        logger.error("Exception when insert timescaledb row: ", e);

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
    public void flushDB() {
        jdbcWriteTemplate.execute(String.format("TRUNCATE TABLE %s", TABLE_NAME));
    }

    @Override
    public void insert(UserNotify userNotify) throws Exception {
        if (!validator.validate(userNotify)) {
            throw new RuntimeException("Invalid data");
        }
        String sql = String.format("INSERT INTO %s(timestamp, user_id, notify_id, data)" +
                "  VALUES (?, ?, ?, ?)", TABLE_NAME);

        PGobject data = new PGobject();
        data.setType("jsonb");
        data.setValue(Util.OBJECT_MAPPER.writeValueAsString(userNotify.getData()));

        jdbcWriteTemplate.update(sql, ps -> {
            ps.setTimestamp(1, new Timestamp(userNotify.getTimestamp()), Calendar.getInstance());
            ps.setString(2, userNotify.getUserID());
            ps.setString(3, userNotify.getNotifyID());
            ps.setObject(4, data);
        });
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
    public List<UserNotify> fetchDesc(String userID, Long fromTime) {
        List<Object> params = Lists.newArrayList(userID);
        String sql = String.format("SELECT * FROM %s WHERE user_id=?", TABLE_NAME);
        if (fromTime != null) {
            sql += " AND timestamp < ?";
            params.add(new Timestamp(fromTime));
        }
        sql += " ORDER BY timestamp DESC LIMIT 20";

        return jdbcReadTemplate.query(sql, params.toArray(), ROW_MAPPER);
    }

    @Override
    public List<UserNotify> fetchAsc(String userID, Long fromTime) {
        List<Object> params = Lists.newArrayList(userID);
        String sql = String.format("SELECT * FROM %s WHERE user_id=?", TABLE_NAME);
        if (fromTime != null) {
            sql += " AND timestamp > ?";
            params.add(new Timestamp(fromTime));
        }
        sql += " ORDER BY timestamp ASC LIMIT 20";

        return jdbcReadTemplate.query(sql, params.toArray(), ROW_MAPPER);
    }

    private static JdbcTemplate createJdbcTemplate() {
        Settings setting = Settings.getInstance();

        BasicDataSource dataSource = new BasicDataSource();
        String connectionString = String.format("jdbc:postgresql://%s:%d/%s", setting.TIMESCALEDB_IP,
                setting.TIMESCALEDB_PORT, setting.TIMESCALEDB_DB);

        dataSource.setDriverClassName(Driver.class.getName());
        dataSource.setUsername(setting.TIMESCALEDB_USER);
        dataSource.setPassword(setting.TIMESCALEDB_PASSWORD);
        dataSource.setUrl(connectionString);
        dataSource.setInitialSize(0);
        dataSource.setMaxTotal(setting.TIMESCALEDB_POOL_SIZE);

        dataSource.setTestOnBorrow(true);
        dataSource.setTestWhileIdle(true);
        dataSource.setValidationQueryTimeout(3);
        dataSource.setValidationQuery("SELECT 1");

        dataSource.setMaxConnLifetimeMillis(900000);
        dataSource.setTimeBetweenEvictionRunsMillis(300000);
        dataSource.setLogExpiredConnections(false);

        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        jdbcTemplate.setResultsMapCaseInsensitive(false);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                dataSource.close();
            } catch (SQLException e) {
                logger.error("Error: ", e);
            }
        }));

        return jdbcTemplate;
    }

    private static class UserNotifyRowMapper implements RowMapper<UserNotify> {

        @Override
        public UserNotify mapRow(ResultSet rs, int rowNum) throws SQLException {
            UserNotify notification = new UserNotify();
            notification.setNotifyID(rs.getString("notify_id"));
            notification.setUserID(rs.getString("user_id"));
            notification.setTimestamp(rs.getTimestamp("timestamp").getTime());

            try {
                notification.setData(Util.OBJECT_MAPPER.readValue(rs.getString("data"), ObjectNode.class));
            } catch (IOException e) {
                logger.error("Error when parsing data format");
            }

            return notification;
        }

    }

}
