/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.indexer.worker;

import iie.metastore.MetaStoreClient;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.log4j.Logger;

/**
 *
 * @author liucuili
 */
public class Initmetastoreinfo implements Runnable {

    public static Logger log = Logger.getLogger(CreateIndex.class.getName());
    List<String> databases = null;

    public Initmetastoreinfo(List<String> dbs) {
        databases = dbs;
    }

    @Override
    public void run() {
        MetaStoreClient msc = Indexer.clientPool.getClient();
        for (String dbName : databases) {
            try {
                List<String> tbList = msc.client.getAllTables(dbName);
                for (String tbName : tbList) {
                    Table tb = msc.client.getTable(dbName, tbName);
                    Indexer.tablename2Table.put(dbName + "." + tbName, tb);
                    List<Index> idxList = msc.client.listIndexes(dbName, tbName, (short) -1);
                    Indexer.tablename2indexs.put(dbName + "." + tbName, idxList);
                    log.info("get " + dbName + "." + tbName + " ok ");
                }
            } catch (Exception ex) {
                log.error(ex, ex);
            } finally {
                Indexer.clientPool.realseOneClient(msc);
            }
        }
    }
}
