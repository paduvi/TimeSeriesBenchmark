package io.dogy.utility;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.http.HeaderElement;
import org.apache.http.HeaderElementIterator;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeaderElementIterator;
import org.apache.http.protocol.HTTP;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Duration;
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

    public static PoolingHttpClientConnectionManager createHttpConnManager(int maxConn) {
        PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager();
        connManager.setMaxTotal(maxConn);
        connManager.setDefaultMaxPerRoute(maxConn);

        return connManager;
    }

    public static ConnectionKeepAliveStrategy createKeepAliveStrategy() {
        return (response, context) -> {
            HeaderElementIterator it = new BasicHeaderElementIterator(
                    response.headerIterator(HTTP.CONN_KEEP_ALIVE));
            while (it.hasNext()) {
                HeaderElement he = it.nextElement();
                String param = he.getName();
                String value = he.getValue();
                if (value != null && param.equalsIgnoreCase("timeout")) {
                    return Long.parseLong(value) * 1000;
                }
            }
            return Duration.ofSeconds(30).toMillis();
        };
    }

    public static RequestConfig createRequestConfig() {
        int timeout = 5;
        return createRequestConfig(timeout);
    }

    public static RequestConfig createRequestConfig(int timeout) {
        return RequestConfig.custom().setConnectTimeout(timeout * 1000)
                .setConnectionRequestTimeout(timeout * 1000).setSocketTimeout(timeout * 1000).build();
    }

}
