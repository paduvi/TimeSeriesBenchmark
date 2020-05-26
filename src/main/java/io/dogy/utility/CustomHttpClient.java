package io.dogy.utility;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.kairosdb.client.HttpClient;

import java.net.MalformedURLException;

public class CustomHttpClient extends HttpClient {
    /**
     * Creates a client to talk to the host on the specified port.
     *
     * @param url url to KairosDB server
     */
    private CloseableHttpClient client;
    public CustomHttpClient(String url, int maxTotal, int defaultMaxPerRoute) throws MalformedURLException {
        super(url);
        PoolingHttpClientConnectionManager manager = new PoolingHttpClientConnectionManager();
        manager.setMaxTotal(maxTotal);
        manager.setDefaultMaxPerRoute(defaultMaxPerRoute);

        HttpClientBuilder builder = HttpClientBuilder.create();
        client = HttpClients.custom()
                .setConnectionManager(manager)
                .setConnectionManagerShared(true).build();
        client = builder.build();
    }
}
