package com.techpago.dao;

import com.techpago.model.UserNotify;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface IUserNotifyDao {

    void insert(UserNotify userNotify) throws Exception;

    CompletableFuture<Object> insertAsync(UserNotify userNotify) throws Exception;

    List<UserNotify> fetchDesc(String userID, Long fromTime) throws Exception;

    List<UserNotify> fetchAsc(String userID, Long fromTime) throws Exception;
}
