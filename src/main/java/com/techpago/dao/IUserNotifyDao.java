package com.techpago.dao;

import com.techpago.model.UserNotify;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

public interface IUserNotifyDao {

    void insert(UserNotify userNotify) throws Exception;

    CompletableFuture<Object> insertAsync(UserNotify userNotify) throws Exception;

    void bulkInsert(Collection<UserNotify> listUserNotify) throws Exception;

}
