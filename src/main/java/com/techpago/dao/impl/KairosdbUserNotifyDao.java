package com.techpago.dao.impl;

import com.techpago.dao.IUserNotifyDao;
import com.techpago.model.UserNotify;
import org.kairosdb.core.KairosDBService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CompletableFuture;


@Component
public class KairosdbUserNotifyDao implements IUserNotifyDao {
    private static final Logger logger = LoggerFactory.getLogger(KairosdbUserNotifyDao.class);

    public KairosdbUserNotifyDao() {

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

    @Override
    public void flushDB() throws Exception {

    }
    //    private HttpClient createHttpClient(){
//
//    }
}
