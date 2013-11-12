/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.dataredistribution.tools;

import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * Manages a pool of IMetaStoreClient connections. If the connection pool is
 * empty a new client is created and added to the pool. There is no size limit.
 */
public class MetaStoreClientPool {

    private static final int METASTORE_RETRY_INIT = 5000;
    private final ConcurrentLinkedQueue<MetaStoreClient> clientPool = new ConcurrentLinkedQueue<MetaStoreClient>();
    private Boolean poolClosed = false;
    private final HiveConf hiveConf;
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(MetaStoreClientPool.class.getName());
    }

    /**
     * A wrapper around the IMetaStoreClient that manages interations with the
     * connection pool.
     */
    public class MetaStoreClient {

        private final IMetaStoreClient cli;
        private boolean isInUse;

        private MetaStoreClient(HiveConf hiveConf) {
            try {
                logger.debug("Creating MetaStoreClient. Pool Size = " + clientPool.size());
                this.cli = createHiveClient(hiveConf);
            } catch (Exception e) {
                // Turn in to an unchecked exception
                throw new IllegalStateException(e);
            }
            this.isInUse = false;
        }

        /**
         * Returns the internal IMetaStoreClient object.
         */
        public IMetaStoreClient getHiveClient() {
            return cli;
        }

        /**
         * Returns this client back to the connection pool. If the connection
         * pool has been closed, just close the Hive client connection.
         */
        public void release() {
            if (isInUse == false) {
                return;
            }
            // Ensure the connection isn't returned to the pool if the pool has
            // been closed.
            // This lock is needed to ensure proper behavior when a thread reads
            // poolClosed
            // is false, but a call to pool.close() comes in immediately
            // afterward.
            isInUse = false;
            synchronized (poolClosed) {
                if (poolClosed) {
                    cli.close();
                } else {
                    clientPool.add(this);
                }
            }
        }

        // Marks this client as in use
        private void markInUse() {
            isInUse = true;
        }
    }

    public MetaStoreClientPool(int initialSize) {
        this(initialSize, new HiveConf(MetaStoreClientPool.class));
    }

    public MetaStoreClientPool(int initialSize, HiveConf hiveConf) {
        this.hiveConf = hiveConf;
        for (int i = 0; i < initialSize; ++i) {
            clientPool.add(new MetaStoreClient(this.hiveConf));
        }
    }

    /**
     * Gets a client from the pool. If the pool is empty a new client is
     * created.
     */
    public MetaStoreClient getClient() {
        MetaStoreClient client = clientPool.poll();
        // The pool was empty so create a new client and return that.
        if (client == null) {
            client = new MetaStoreClient(this.hiveConf);
        }
        client.markInUse();
        return client;
    }

    /**
     * Removes all items from the connection pool and closes all Hive Meta Store
     * client connections. Can be called multiple times.
     */
    public void close() {
        // Ensure no more items get added to the pool once close is called.
        synchronized (poolClosed) {
            if (poolClosed) {
                return;
            }
            poolClosed = true;
        }

        MetaStoreClient client = null;
        while ((client = clientPool.poll()) != null) {
            client.getHiveClient().close();
        }
    }

    /**
     * Creates a IMetaStoreClient, retrying the operation if MetaStore
     * exceptions occur. A random sleep is injected between retries to help
     * reduce the likelihood of flooding the Meta Store with many requests at
     * once.
     */
    private static IMetaStoreClient createHiveClient(HiveConf conf) throws Exception {
        // Ensure numbers are random across nodes.
        int retryInterval = METASTORE_RETRY_INIT;
        int retryAttempt = 0;
        while (true) {
            try {
                return new HiveMetaStoreClient(conf);
            } catch (Exception e) {
                logger.error("Error initializing Hive Meta Store client", e);
            }

            // Randomize the retry interval so the meta store isn't flooded with
            // attempts.
            if (retryInterval < 300000) {
                retryInterval = retryInterval + METASTORE_RETRY_INIT;
            } else {
                retryInterval = 300000;
            }

            logger.info(String.format("On retry attempt %d . Sleeping %d seconds.", ++retryAttempt, retryInterval / 1000));
            try {
                Thread.sleep(retryInterval);
            } catch (InterruptedException ie) {
                // Do nothing
            }
        }
        // Should never make it to here.
    }
}
