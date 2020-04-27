package com.techpago.dao.impl;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.domain.Bucket;
import com.influxdb.client.domain.BucketRetentionRules;
import com.influxdb.client.domain.Organization;
import com.techpago.config.Settings;
import com.techpago.dao.IUserNotifyDao;
import com.techpago.model.UserNotify;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@Component("InfluxDbUserNotifyDao")
public class InfluxDbUserNotifyDao implements IUserNotifyDao {

    private final Settings setting = Settings.getInstance();
    private final InfluxDBClient influxDBClient;

    public InfluxDbUserNotifyDao() {
        this.influxDBClient = InfluxDBClientFactory.create(
                String.format("http://%s:%s", setting.INFLUXDB_IP, setting.INFLUXDB_PORT),
                setting.INFLUXDB_TOKEN.toCharArray(), setting.INFLUXDB_ORG
        );
        this.influxDBClient.enableGzip();
        Runtime.getRuntime().addShutdownHook(new Thread(this.influxDBClient::close));
    }

    @Override
    public void flushDB() {
        // Delete bucket
        Bucket bucket = influxDBClient.getBucketsApi().findBucketByName(setting.INFLUXDB_BUCKET);
        String orgId = null;
        if (bucket != null) {
            orgId = bucket.getOrgID();
            influxDBClient.getBucketsApi().deleteBucket(bucket);
        } else {
            for (Organization organization : influxDBClient.getOrganizationsApi().findOrganizations()) {
                if (organization.getName().equals(setting.INFLUXDB_ORG)) {
                    orgId = organization.getId();
                    break;
                }
            }
            if (orgId == null) {
                throw new RuntimeException("Not found org: " + setting.INFLUXDB_ORG);
            }
        }

        // Create bucket again with retention policy
        BucketRetentionRules retention = new BucketRetentionRules();
        retention.setEverySeconds(setting.TTL_IN_SECONDS);

        influxDBClient.getBucketsApi().createBucket(setting.INFLUXDB_BUCKET, retention, orgId);
    }

    @Override
    public void insert(UserNotify userNotify) throws Exception {

    }

    @Override
    public CompletableFuture<Object> insertAsync(UserNotify userNotify) throws Exception {
        return null;
    }

    @Override
    public List<UserNotify> fetchDesc(String userID, Long fromTime) throws Exception {
        return null;
    }

    @Override
    public List<UserNotify> fetchAsc(String userID, Long fromTime) throws Exception {
        return null;
    }

}
