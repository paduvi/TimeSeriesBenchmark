package io.dogy.dao;

import io.dogy.model.UserNotify;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface IUserNotifyDao {

    void flushDB() throws Exception;

    void insert(UserNotify userNotify) throws Exception;

    CompletableFuture<Object> insertAsync(UserNotify userNotify) throws Exception;

    List<UserNotify> fetchDesc(String userID, Long fromTime) throws Exception;

    List<UserNotify> fetchAsc(String userID, Long fromTime) throws Exception;
}
