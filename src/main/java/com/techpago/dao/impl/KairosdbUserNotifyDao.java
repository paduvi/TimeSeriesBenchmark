package com.techpago.dao.impl;

import com.techpago.dao.IUserNotifyDao;
import com.techpago.model.UserNotify;
import org.apache.http.impl.client.HttpClientBuilder;
import org.kairosdb.core.KairosDBService;
import org.kairosdb.core.KairosDataPointFactory;
import org.kairosdb.core.exception.KairosDBException;
import org.kairosdb.core.scheduler.KairosDBJobFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.kairosdb.*;
import sun.net.www.http.HttpClient;

import javax.servlet.http.HttpServlet;


@Component
public class KairosdbUserNotifyDao implements IUserNotifyDao {
    private static final Logger logger = LoggerFactory.getLogger(TimescaledbUserNotifyDao.class);

    public KairosdbUserNotifyDao(){

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

//    private HttpClient createHttpClient(){
//
//    }
}
