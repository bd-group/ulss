/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.dataredistribution.tools;

import cn.ac.iie.ulss.dataredistribution.commons.RuntimeEnv;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

/**
 *
 * @author evan
 */
public class HttpConnectionManager {
//
//    public static DefaultHttpClient getHttpClient(int PoolSize) {
//        SchemeRegistry schemeRegistry = new SchemeRegistry();
//        schemeRegistry.register(new Scheme("http", 80, PlainSocketFactory.getSocketFactory()));
//        schemeRegistry.register(new Scheme("https", 443, SSLSocketFactory.getSocketFactory()));
//
//        PoolingClientConnectionManager cm = null;
//        HttpParams params = null;
//
//        cm = new PoolingClientConnectionManager(schemeRegistry);
//        cm.setMaxTotal(PoolSize);
//        cm.setDefaultMaxPerRoute(PoolSize / 2);
//
//        params = new BasicHttpParams();
//        params.setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, TIMEOUT);
//        params.setParameter(CoreConnectionPNames.SO_TIMEOUT, SO_TIMEOUT);
//
//        DefaultHttpClient client = new DefaultHttpClient(cm, params);
//        
//        return client;
//    }
//    

    public static CloseableHttpClient getHttpClient(int PoolSize) {
        PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
        cm.setMaxTotal(PoolSize);
        cm.setDefaultMaxPerRoute(PoolSize);
        cm.setDefaultConnectionConfig(ConnectionConfig.DEFAULT);
        CloseableHttpClient httpClient = HttpClients.custom()
                .setConnectionManager(cm)
                .build();
        return httpClient;
    }
}
