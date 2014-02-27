/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.indexer.metastore;

import iie.metastore.MetaStoreClient;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.log4j.Logger;

public class MetastoreClientPool {

    private static final int METASTORE_RETRY_INIT = 5000;
    public static Logger log = Logger.getLogger(MetastoreClientPool.class.getName());
    private final ConcurrentLinkedQueue<MetaStoreClient> clientPool = new ConcurrentLinkedQueue<MetaStoreClient>();
    private String url = "";

    public MetastoreClientPool(int initialSize, String url) {
        int i = 0;
        this.url = url;
        while (i < initialSize) {
            try {
                MetaStoreClient cli = new MetaStoreClient(this.url);
                if (cli != null) {
                    clientPool.add(cli);
                    i++;
                } else {
                    log.error("when init the metastore client pool error,the client is null");
                }
            } catch (Exception ex) {
                try {
                    Thread.sleep(5000);
                } catch (Exception ex1) {
                }
                log.error(ex, ex);
            }
        }
    }

    public MetaStoreClient getOneNewClient() {
        MetaStoreClient tmp = MetastoreClientPool.createMetaStoreClient(this.url);
        return tmp;
    }

    public MetaStoreClient getClient() {
        MetaStoreClient client = null;
        while ((client = clientPool.poll()) == null) {
            log.warn("can't get the metastoreclient from the MetastoreClientPool,the pool has no more client，will retry ...");
            try {
                Thread.sleep(500);
            } catch (InterruptedException ex) {
                log.error(ex, ex);
            }
        }
        return client;
    }

    public void realseOneClient(MetaStoreClient tmp) {
        this.clientPool.offer(tmp);
    }

    private static MetaStoreClient createMetaStoreClient(String url) {
        int retryInterval = METASTORE_RETRY_INIT;
        int retryAttempt = 0;
        while (true) {
            try {
                return new MetaStoreClient(url);
            } catch (Exception e) {
                log.error("Error initializing MetaStore client：", e);
            }
            if (retryInterval < 30000) {
                retryInterval = retryInterval + METASTORE_RETRY_INIT;
            } else {
                retryInterval = 30000;
            }
            log.info(String.format("On retry attempt %d . Sleeping %d seconds.", ++retryAttempt, retryInterval / 1000));
            try {
                Thread.sleep(retryInterval);
            } catch (InterruptedException ie) {
            }
        }
    }
}
