package com.techpago.dao.impl;

import com.techpago.dao.IUserNotifyDao;
import com.techpago.model.UserNotify;
import net.opentsdb.core.TSDB;
import net.opentsdb.tools.OpenTSDBMain;

import net.opentsdb.utils.Config;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CompletableFuture;
@Component
public class OpentsdbUserNotifyDao implements IUserNotifyDao {
    public OpentsdbUserNotifyDao(){
        TSDB tsdbWrite = createTsdb();
        TSDB tsdbRead = createTsdb();
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

    private TSDB createTsdb(){
        Config config = new Config();
        TSDB tsdb = new TSDB(config);
        return  tsdb;
    }
}
