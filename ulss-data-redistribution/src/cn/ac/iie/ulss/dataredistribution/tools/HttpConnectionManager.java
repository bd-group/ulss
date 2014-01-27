/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.dataredistribution.tools;

import cn.ac.iie.ulss.dataredistribution.commons.RuntimeEnv;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.params.HttpParams;

/**
 *
 * @author evan
 */
public class HttpConnectionManager {

    static final int TIMEOUT = 5000;//连接超时时间
    static final int SO_TIMEOUT = ((Integer) RuntimeEnv.getParam(RuntimeEnv.SEND_TIMEOUT)) * 1000;//数据传输超时

    public static DefaultHttpClient getHttpClient(int PoolSize) {
        SchemeRegistry schemeRegistry = new SchemeRegistry();
        schemeRegistry.register(new Scheme("http", 80, PlainSocketFactory.getSocketFactory()));
        schemeRegistry.register(new Scheme("https", 443, SSLSocketFactory.getSocketFactory()));

        PoolingClientConnectionManager cm = null;
        HttpParams params = null;

        cm = new PoolingClientConnectionManager(schemeRegistry);
        cm.setMaxTotal(PoolSize);
        cm.setDefaultMaxPerRoute(PoolSize / 2);

        params = new BasicHttpParams();
        params.setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, TIMEOUT);
        params.setParameter(CoreConnectionPNames.SO_TIMEOUT, SO_TIMEOUT);

        DefaultHttpClient client = new DefaultHttpClient(cm, params);
        return client;
    }
}
