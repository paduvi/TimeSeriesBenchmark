package com.techpago.utility;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.commons.lang3.time.DurationFormatUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.CompletableFuture;

public class Util {

    public static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    static {
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        OBJECT_MAPPER.setSerializationInclusion(Include.NON_NULL);
        OBJECT_MAPPER.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    }

    public static <T> T deepCopy(Object value, Class<T> clazz) throws IOException {
        return OBJECT_MAPPER.readValue(OBJECT_MAPPER.writeValueAsString(value), clazz);
    }

    public static <T> T deepCopy(Object value, TypeReference<T> type) throws IOException {
        return OBJECT_MAPPER.readValue(OBJECT_MAPPER.writeValueAsString(value), type);
    }

    public static String readFromInputStream(InputStream inputStream) throws IOException {
        StringBuilder resultStringBuilder = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            while ((line = br.readLine()) != null) {
                resultStringBuilder.append(line).append("\n");
            }
        }
        return resultStringBuilder.toString();
    }

    public static CompletableFuture<?> allOfTerminateOnFailure(CompletableFuture<?>... futures) {
        CompletableFuture<?> failure = new CompletableFuture<>();
        for (CompletableFuture<?> f : futures) {
            f.exceptionally(ex -> {
                failure.completeExceptionally(ex);
                return null;
            });
        }
        return CompletableFuture.anyOf(failure, CompletableFuture.allOf(futures));
    }

    public static String formatDuration(long durationMillis) {
        return DurationFormatUtils.formatDuration(durationMillis, "s.SSS", true);
    }

}
