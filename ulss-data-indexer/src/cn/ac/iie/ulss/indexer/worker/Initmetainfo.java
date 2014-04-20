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
public class Initmetainfo implements Runnable {

    public static Logger log = Logger.getLogger(IndexControler.class.getName());
    List<String> databases = null;

    public Initmetainfo(List<String> dbs) {
        databases = dbs;
    }

    @Override
    public void run() {
        MetaStoreClient msc = Indexer.clientPool.getClient();
        for (String dbName : databases) {
            try {
                List<String> tbList = msc.client.getAllTables(dbName);
                log.info("the table list size for " + dbName + " is " + tbList.size());
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

        /*
         * ConcurrentHashMap<String, LinkedHashMap<String, List<FieldSchema>>> table2idx2colMap = new ConcurrentHashMap<String, LinkedHashMap<String, List<FieldSchema>>>(50, 0.8f);
         ConcurrentHashMap<String, LinkedHashMap<String, Field>> table2idx2LuceneFieldMap = table2idx2LuceneFieldMap = new ConcurrentHashMap<String, LinkedHashMap<String, Field>>(50, 0.8f);
         ConcurrentHashMap<String, LinkedHashMap<String, FieldType>> table2idx2LuceneFieldTypeMap = table2idx2LuceneFieldTypeMap = new ConcurrentHashMap<String, LinkedHashMap<String, FieldType>>(50, 0.8f);
         */
        //InitEnvTools.initIndexinfo(null, null, null, dbName, tbName);
        //log.info("init index information for " + dbName + "." + tbName + " ok ");
        // try {
        // log.info(Indexer.table2idx2LuceneFieldTypeMapPool.take().toString());
        // } catch (InterruptedException ex) {
        // java.util.logging.Logger.getLogger(Initmetainfo.class.getName()).log(Level.SEVERE, null, ex);
        // }
    }
}