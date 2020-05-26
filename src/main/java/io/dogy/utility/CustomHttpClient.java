package io.dogy.utility;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.kairosdb.client.HttpClient;

import java.net.MalformedURLException;

public class CustomHttpClient extends HttpClient {

    public CustomHttpClient(String url, int maxTotal) throws MalformedURLException {
        super(url);

        PoolingHttpClientConnectionManager connManager = Util.createHttpConnManager(maxTotal);
        ConnectionKeepAliveStrategy strategy = Util.createKeepAliveStrategy();
        RequestConfig config = Util.createRequestConfig();

        setClient(HttpClients.custom().setKeepAliveStrategy(strategy)
                .setConnectionManager(connManager).setDefaultRequestConfig(config).build());
    }

}
