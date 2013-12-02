/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ulss.se2db.handler;

import cn.gbase.gb8a.gbloadapi.GBLoadClient;
import cn.gbase.gb8a.gbloadapi.Row;
import cn.gbase.gb8a.gbloadapi.Statement;
import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.client.consumer.MessageConsumer;
import com.taobao.metamorphosis.client.consumer.MessageListener;
import com.taobao.metamorphosis.utils.ZkUtils;
import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import ulss.se2db.commons.RuntimeEnv;
import ulss.se2db.metastore.TableSe2DBRule;
import ulss.se2db.metastore.TableSe2DBRule.Column;

/**
 *
 * @author alexmu
 */
public class SE2GBDBWorker extends SE2DBWorker {

    private static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(SE2GBDBWorker.class.getName());
    }

    public SE2GBDBWorker(TableSe2DBHandler pTableSe2DBHandler) {
        super(pTableSe2DBHandler);
    }

    @Override
    public void run() {

        TableSe2DBRule tableSe2DBRule = tableSe2DBHandler.getTableSe2DBRule();

        try {

            logger.info("write to table " + tableSe2DBRule.getTableName());
            List<Column> columnSet = tableSe2DBRule.getColumnSet();
            List<String> columnNameSet = new ArrayList<String>();
            for (Column column : columnSet) {
                columnNameSet.add(column.getColumnName());
            }

            System.setProperty("notify.remoting.max_read_buffer_size", "10485760");
            MetaClientConfig metaClientConfig = new MetaClientConfig();
            final ZkUtils.ZKConfig zkConfig = new ZkUtils.ZKConfig();

            zkConfig.zkConnect = (String) RuntimeEnv.getParam(RuntimeEnv.ZK_CLUSTER);
            metaClientConfig.setZkConfig(zkConfig);

            final MessageSessionFactory sessionFactory = new MetaMessageSessionFactory(metaClientConfig);
            String consumerGroupName = "se2gbdw_" + tableSe2DBRule.getTableName() + "_" + tableSe2DBRule.getMqName() + "";
            ConsumerConfig cc = new ConsumerConfig();
            cc.setGroup(consumerGroupName);
            cc.setFetchTimeoutInMills(100000);
            cc.setMaxDelayFetchTimeInMills(500);
            cc.setFetchRunnerCount(10);
            cc.setCommitOffsetPeriodInMills(100);
            cc.setConsumerId(System.nanoTime() + "");
            final MessageConsumer consumer = sessionFactory.createConsumer(cc);

            // fetch messages
            logger.info("starting consumer worker of mq " + tableSe2DBRule.getMqName() + " for " + tableSe2DBRule.getTableName() + " ....");

            consumer.subscribe(tableSe2DBRule.getMqName(), 20 * 1024 * 1024, new GBMessageListener(columnNameSet));
            consumer.completeSubscribe();

            while (true) {
                if (stopped) {
                    consumer.shutdown();
//                    sessionFactory.shutdown();
                    break;
                } else {
                    try {
                        logger.info("waiting...");
                        Thread.sleep(5000);
                    } catch (Exception ex) {
                    }
                }
            }

        } catch (Exception ex) {
            logger.error("error happened when doing serilization to " + tableSe2DBRule.getTableName() + "gb data warehouse for " + ex.getMessage(), ex);
        }
    }

    class GBMessageListener implements MessageListener {

        Schema docSchema = tableSe2DBHandler.getDocSchema();
        DatumReader<GenericRecord> docReader = tableSe2DBHandler.getDocReader();
        TableSe2DBRule tableSe2DBRule = tableSe2DBHandler.tableSe2DBRule;
        List<TableSe2DBRule.Column> tableColumnSet = tableSe2DBRule.getColumnSet();
        int batchSize = tableSe2DBRule.getBatchSize();
        List<String> columnNameSet = null;
        int columnSize = tableColumnSet.size();
        GBLoadClient gbClient = null;
        int record2CommitNum = 0;
        Map<String, GBSession> gbSessionSet = new HashMap<String, GBSession>();
        ExecutorService threadPool = null;

        public GBMessageListener() {
            threadPool = Executors.newFixedThreadPool(10);
        }

        class GBSession {

            GBLoadClient gbClient = null;
            Statement stmt = null;
            int recordNum = 0;
            List<GenericRecord> docRecordSet = new ArrayList<GenericRecord>();

            public GBSession() {
                gbClient = new GBLoadClient();
                reset();
            }

            private void reset() {
                try {
                    gbClient.disconnect();
                } catch (Exception ex) {
                    logger.warn("error happened " + ex.getMessage(), ex);
                }

                try {
                    String[] connStrItems = tableSe2DBRule.getConnStr().split(":");
                    int rv = gbClient.connect(connStrItems[0], Integer.parseInt(connStrItems[1]), "", "");
                    if (rv != 0) {
                        gbClient.disconnect();
                    }
                } catch (Exception ex) {
                    try {
                        gbClient.disconnect();
                    } catch (Exception ex1) {
                    }
                }
            }

            public void initStatment() {
                if (stmt == null) {
                    try {
                        stmt = gbClient.startLoad("logdb", tableSe2DBRule.getTableName(), columnNameSet);
                    } catch (Exception ex) {
                        logger.warn("create stmt unsuccessfully for " + ex.getMessage(), ex);
                        stmt = null;
                    }
                }
            }

            public void loadData(List<GenericRecord> pRecordSet) throws Exception {
                docRecordSet.addAll(pRecordSet);
                recordNum += pRecordSet.size();
                if (recordNum >= batchSize) {
                    //build row
                    int tryTimes = 0;
                    for (; tryTimes < 10; tryTimes++) {
                        if (stmt != null) {
                            List<Row> rows = new ArrayList<Row>();
                            for (GenericRecord docRecord : docRecordSet) {
                                Row row = new Row(stmt);
                                try {
                                    row.beginRow();
                                    for (int i = 0; i < columnSize; i++) {
                                        Object value = docRecord.get(tableColumnSet.get(i).getColumnName());
                                        if (value == null || value.toString().isEmpty()) {
                                            row.appendNull();
                                        } else {
                                            row.appendValue(value.toString());
                                        }
                                    }
                                    row.endRow();
                                    rows.add(row);
                                } catch (Exception ex) {
                                    logger.warn("set record value unsuccessfully for " + ex.getMessage() + " and skip it", ex);
                                    continue;
                                }
                            }

                            if (rows.size() > 0) {
                                try {
                                    stmt.addRows(rows);
                                    long startTime = System.nanoTime();
                                    stmt.commit();
                                    long endTime = System.nanoTime();
                                    logger.info("write " + rows.size() + " records to table " + tableSe2DBRule.getTableName() + " successfully using " + (endTime - startTime) / (1024 * 1024) + " ms");
                                    docRecordSet.clear();
                                    recordNum = 0;
                                    break;
                                } catch (Exception ex) {
                                    logger.error("write " + rows.size() + " records to table " + tableSe2DBRule.getTableName() + " unsuccessfully ");
                                    try {
                                        stmt.rollback();
                                    } catch (Exception ex1) {
                                    }

                                    try {
                                        stmt.release();
                                    } catch (Exception ex1) {
                                    }
                                    stmt = null;
                                }
                            } else {
                                try {
                                    stmt.release();
                                } catch (Exception ex1) {
                                } finally {
                                    stmt = null;
                                    initStatment();
                                }
                            }
                        } else {
                            initStatment();
                        }
                    }

                    if (tryTimes >= 10) {
                        docRecordSet.clear();
                        recordNum = 0;
                        //dump to file                       
                    }
                }
            }
        }

        public GBMessageListener(List<String> pColumnNameSet) {
            columnNameSet = pColumnNameSet;
        }

        private GBSession getGBSession() {
            String key = Thread.currentThread().getId() + "";
            GBSession gbSession = null;
            synchronized (this) {
                gbSession = gbSessionSet.get(key);
            }

            if (gbSession == null) {
                gbSession = new GBSession();
                synchronized (this) {
                    gbSessionSet.put(key, gbSession);
                }
            }

            return gbSession;
        }

        //fixme:add timeout
        public void recieveMessages(Message msg) {
            try {
                logger.info(Thread.currentThread().getId() + " receives message " + msg.getData().length + " at " + System.nanoTime());

                //decode docs
                ByteArrayInputStream docsbis = new ByteArrayInputStream(msg.getData());//tuning
                BinaryDecoder docsbd = new DecoderFactory().binaryDecoder(docsbis, null);
                GenericArray docsset = null;
                try {
                    GenericRecord docsRecord = new GenericData.Record(RuntimeEnv.docsSchema);
                    RuntimeEnv.docsReader.read(docsRecord, docsbd);
                    docsset = (GenericData.Array<GenericRecord>) docsRecord.get("doc_set");
                    logger.info("docs size:" + docsset.size());
                    if (docsset.size() < 1) {
                        logger.warn("doc set is empty");
                        return;
                    }
                } catch (Exception ex) {
                    logger.error("parse docs unsuccessfully for " + ex.getMessage(), ex);
                    docsset = null;
                    return;
                } finally {
                    try {
                        docsbis.close();
                    } catch (Exception ex) {
                    }
                }

                //get gb session
                logger.info("getting gb session for " + tableSe2DBHandler.getTableName());
                GBSession gbSession = getGBSession();

                //build rows
                Iterator<ByteBuffer> itor = docsset.iterator();
                List<GenericRecord> recordSet = new ArrayList<GenericRecord>();
                while (itor.hasNext()) {
                    //read doc
                    ByteArrayInputStream docbis = new ByteArrayInputStream(((ByteBuffer) itor.next()).array());
                    BinaryDecoder docbd = new DecoderFactory().binaryDecoder(docbis, null);
                    GenericRecord docRecord = new GenericData.Record(docSchema);
                    try {
                        docReader.read(docRecord, docbd);
                    } catch (Exception ex) {
                        logger.warn("parse doc unsuccessfully for " + ex.getMessage() + " and skip it");
                        continue;
                    }
                    recordSet.add(docRecord);
                }
                gbSession.loadData(recordSet);
            } catch (Exception ex) {
                logger.fatal("unknown exception " + ex.getMessage(), ex);
            }
        }

        public Executor getExecutor() {
            // Thread pool to process messages,maybe null.            
            return threadPool;
        }
    }
}
