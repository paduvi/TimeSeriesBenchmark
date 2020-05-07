package io.dogy.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.dogy.utility.ULID;
import io.dogy.utility.Util;

import java.util.Random;

public class UserNotify {

    @JsonProperty("user_id")
    private String userID;

    @JsonProperty("notify_id")
    private String notifyID;

    private long timestamp;

    private ObjectNode data = Util.OBJECT_MAPPER.createObjectNode();

    private final static Random random = new Random();
    private final static ULID ulid = new ULID();

    public static UserNotify createDumbObject() {
        UserNotify userNotify = new UserNotify();
        userNotify.setUserID(String.valueOf(random.nextInt(1000)));
        userNotify.setNotifyID(ulid.nextULID());
        userNotify.setTimestamp(System.currentTimeMillis());
        userNotify.getData().put("foo", "bar");

        return userNotify;
    }

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

    @Override
    public String toString() {
        return "UserNotify{" +
                "userID='" + userID + '\'' +
                ", notifyID='" + notifyID + '\'' +
                ", timestamp=" + timestamp +
                ", data=" + data +
                '}';
    }
}
