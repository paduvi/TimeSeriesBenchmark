package io.dogy.utility;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class ByteUtil {

    public static String toString(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        return Bytes.toString(bytes);
    }

    public static Long toLong(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        return Bytes.toLong(bytes);
    }

    public static Integer toInteger(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        return Bytes.toInt(bytes);
    }

    public static boolean toBoolean(byte[] bytes) {
        if (bytes == null) {
            return false;
        }
        return Bytes.toBoolean(bytes);
    }

    public static <T> T toPOJO(byte[] bytes, Class<T> clazz) {
        if (bytes == null) {
            return null;
        }
        try {
            return Util.OBJECT_MAPPER.readValue(bytes, clazz);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static <T> T toPOJO(byte[] bytes, TypeReference<T> type) {
        if (bytes == null) {
            return null;
        }
        try {
            return Util.OBJECT_MAPPER.readValue(bytes, type);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

}
