package com.techpago.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.techpago.utility.Util;

public class UserNotify {

    @JsonProperty("user_id")
    private String userID;

    @JsonProperty("notify_id")
    private String notifyID;

    private long timestamp;

    private ObjectNode data = Util.OBJECT_MAPPER.createObjectNode();

    public String getUserID() {
        return userID;
    }

    public void setUserID(String userID) {
        this.userID = userID;
    }

    public String getNotifyID() {
        return notifyID;
    }

    public void setNotifyID(String notifyID) {
        this.notifyID = notifyID;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public ObjectNode getData() {
        return data;
    }

    public void setData(ObjectNode data) {
        this.data = data;
    }
}
