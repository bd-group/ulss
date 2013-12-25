/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ulss.se2db.handler;

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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
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

            consumer.subscribe(tableSe2DBRule.getMqName(), 20 * 1024 * 1024, new GBMessageListener());
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
        String pSQL = "";
        Map<String, GBSession> gbSessionSet = new HashMap<String, GBSession>();
        ExecutorService threadPool = null;

        class GBSession {

            Connection conn = null;
            PreparedStatement pStmt = null;
            int recordNum = 0;
            List<GenericRecord> docRecordSet = new ArrayList<GenericRecord>();

            public GBSession() {
                init();
            }

            private void init() {
                String sql = "INSERT INTO " + tableSe2DBRule.getTableName() + "(COLUMNS) VALUES(VALUES)";
                String columns = "";
                String values = "";
                for (int i = 0; i < columnSize; i++) {
                    if (i == 0) {
                        columns = tableColumnSet.get(i).getColumnName();
                        values = "?";
                    } else {
                        columns += "," + tableColumnSet.get(i).getColumnName();
                        values += ",?";
                    }
                }
                pSQL = sql.replace("COLUMNS", columns).replace("VALUES", values);
                logger.info("insert sql for table " + tableSe2DBRule.getTableName() + " is " + pSQL);
            }

            private void resetConnection() {
                if (conn != null) {
                    try {
                        conn.close();
                    } catch (Exception ex) {
                    } finally {
                        conn = null;
                    }
                }

                String[] connStrItems = tableSe2DBRule.getConnStr().split(":");
                if (connStrItems.length < 3) {
                    logger.error("connection string to gbase " + tableSe2DBRule.getConnStr() + " is wrong");
                    return;
                }

                try {
                    Class.forName("cn.gbase.jdbc.LoadDriver");  // 加载驱动,gbase加载接口的驱动名称为 cn.gbase.jdbc.LoadDriver
                } catch (Exception ex) {
                    logger.error("load gbase jdbc driver unsuccessfully for " + ex.getMessage(), ex);
                    return;
                }

                try {
                    conn = DriverManager.getConnection(connStrItems[0], connStrItems[1], connStrItems[2]);
                } catch (Exception ex) {
                    logger.warn("create connection to gbase " + tableSe2DBRule.getConnStr() + " unsuccessfully for " + ex.getMessage(), ex);
                    return;
                }
            }

            public void initStatment() {
                if (pStmt != null) {
                    try {
                        pStmt.clearBatch();
                    } catch (Exception ex1) {
                    }

                    try {
                        pStmt.close();
                    } catch (Exception ex1) {
                    }
                    pStmt = null;
                }

                resetConnection();
                for (int tryTimes = 0; tryTimes < 3; tryTimes++) {
                    logger.info(tryTimes + "st try to create pstmt to " + tableSe2DBRule.getTableName() + "...");
                    if (conn != null) {
                        try {
                            pStmt = conn.prepareStatement(pSQL);
                            break;
                        } catch (Exception ex) {
                            logger.warn("create pstmt unsuccessfully for " + ex.getMessage(), ex);
                            pStmt = null;
                            resetConnection();
                        }
                    } else {
                        logger.warn("connection to gbase is invalid,reset firstly");
                        resetConnection();
                    }
                }
            }

            public void loadData(List<GenericRecord> pRecordSet) throws Exception {
                if (pRecordSet.size() < 1) {
                    logger.warn("no data to load");
                    return;                    
                }
                docRecordSet.addAll(pRecordSet);
                recordNum += pRecordSet.size();
                if (recordNum >= batchSize) {
                    //build row
                    int validRecordNum = 0;
                    int tryTimes = 0;
                    for (; tryTimes < 10; tryTimes++) {
                        if (pStmt != null) {
                            for (GenericRecord docRecord : docRecordSet) {
                                try {
                                    for (int i = 0; i < columnSize; i++) {
                                        Object value = docRecord.get(tableColumnSet.get(i).getColumnName());
                                        if (value == null || value.toString().isEmpty()) {
                                            pStmt.setNull(tableColumnSet.get(i).getColumnIdx(), Types.NULL);
                                        } else {
                                            pStmt.setString(tableColumnSet.get(i).getColumnIdx(), value.toString());
                                        }
                                    }
                                    pStmt.addBatch();
                                    validRecordNum++;
                                } catch (Exception ex) {
                                    logger.warn("set record value unsuccessfully for " + ex.getMessage() + " and skip it", ex);
                                    pStmt.clearParameters();//support?
                                    continue;
                                }
                            }

                            if (validRecordNum > 0) {
                                try {
                                    long startTime = System.nanoTime();
                                    pStmt.executeBatch();
                                    conn.commit();
                                    long endTime = System.nanoTime();
                                    logger.info("write " + validRecordNum + "(expect:" + recordNum + ") records to table " + tableSe2DBRule.getTableName() + " successfully using " + (endTime - startTime) / (1024 * 1024) + " ms");
                                    pStmt.clearBatch();
                                    docRecordSet.clear();
                                    recordNum = 0;
                                    break;
                                } catch (Exception ex) {
                                    logger.error("write " + validRecordNum + "(expect:" + recordNum + ") records to table " + tableSe2DBRule.getTableName() + " unsuccessfully ", ex);
                                    initStatment();
                                }
                            } else {
                                initStatment();
                            }
                        } else {
                            initStatment();
                        }
                    }

                    if (tryTimes >= 10) {
                        docRecordSet.clear();
                        recordNum = 0;
                        initStatment();
                        //dump to file                       
                    }
                }
            }
        }

        private GBSession getGBSession() {
            String key = Thread.currentThread().getId() + "";
            GBSession gbSession = null;
            synchronized (this) {
                gbSession = gbSessionSet.get(key);
                if (gbSession == null) {
                    logger.info("gb session for thread " + key + " is null");
                    gbSession = new GBSession();
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
