<<<<<<< HEAD
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ulss.se2db.handler;

import com.oscar.cluster.jdbc3g.Jdbc3gConnection;
import com.oscar.cluster.jdbc3g.OscarImportDistribute;
import com.oscar.cluster.util.ImportMode;
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

/**
 *
 * @author alexmu
 */
public class SE2STDBWorker extends SE2DBWorker {

    private static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(SE2STDBWorker.class.getName());
    }

    public SE2STDBWorker(TableSe2DBHandler pTableSe2DBHandler) {
        super(pTableSe2DBHandler);
    }

    @Override
    public void run() {

        TableSe2DBRule tableSe2DBRule = tableSe2DBHandler.getTableSe2DBRule();
        try {
            logger.info("write to table " + tableSe2DBRule.getTableName());

            System.setProperty("notify.remoting.max_read_buffer_size", "10485760");
            MetaClientConfig metaClientConfig = new MetaClientConfig();
            final ZkUtils.ZKConfig zkConfig = new ZkUtils.ZKConfig();

            zkConfig.zkConnect = (String) RuntimeEnv.getParam(RuntimeEnv.ZK_CLUSTER);
            metaClientConfig.setZkConfig(zkConfig);

            final MessageSessionFactory sessionFactory = new MetaMessageSessionFactory(metaClientConfig);
            String consumerGroupName = "se2stdw_" + tableSe2DBRule.getTableName() + "_" + tableSe2DBRule.getMqName() + "";
            ConsumerConfig cc = new ConsumerConfig();
            cc.setGroup(consumerGroupName);
            cc.setFetchTimeoutInMills(30000);
            cc.setMaxDelayFetchTimeInMills(500);
            cc.setFetchRunnerCount(4);
            cc.setCommitOffsetPeriodInMills(100);
            cc.setConsumerId(System.nanoTime() + "");
            final MessageConsumer consumer = sessionFactory.createConsumer(cc);

            // fetch messages
            logger.info("starting consumer worker of mq " + tableSe2DBRule.getMqName() + " for " + tableSe2DBRule.getTableName() + " ....");

            consumer.subscribe(tableSe2DBRule.getMqName(), 20 * 1024 * 1024, new STMessageListener());
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

    class STMessageListener implements MessageListener {

        Schema docSchema = tableSe2DBHandler.getDocSchema();
        DatumReader<GenericRecord> docReader = tableSe2DBHandler.getDocReader();
        TableSe2DBRule tableSe2DBRule = tableSe2DBHandler.tableSe2DBRule;
        List<TableSe2DBRule.Column> tableColumnSet = tableSe2DBRule.getColumnSet();
        int batchSize = tableSe2DBRule.getBatchSize();
        List<String> columnNameSet = null;
        int columnSize = tableColumnSet.size();
        Map<String, STSession> stSessionSet = new HashMap<String, STSession>();
        ExecutorService threadPool = null;

        public STMessageListener() {
            threadPool = Executors.newFixedThreadPool(3);
        }

        class STSession {

            Connection conn = null;
            OscarImportDistribute handler = null;
            int recordNum = 0;
            List<GenericRecord> docRecordSet = new ArrayList<GenericRecord>();

            public STSession() {
                reset();
            }

            private void reset() {

                try {
                    if (conn != null) {
                        conn.close();
                    }
                } catch (Exception ex) {
                } finally {
                    conn = null;
                }

                try {
                    Class.forName("com.oscar.cluster.Driver");
                    conn = DriverManager.getConnection(tableSe2DBRule.getConnStr(), tableSe2DBRule.getUserName(), tableSe2DBRule.getPassword());
                    conn.setAutoCommit(false);
                    logger.info("create connection to st dw unsuccessfully");
                } catch (Exception ex) {
                    logger.warn("create connection to st dw unsuccessfully for " + ex.getMessage(), ex);
                    conn = null;
                }

                handler = null;
            }

            public void initHandler() {
                if (handler == null) {
                    try {
                        //创建大容量数据导入对象
                        handler = (OscarImportDistribute) ((Jdbc3gConnection) conn).createDistributeImportHandler(tableSe2DBRule.getTableName(), ImportMode.SINGLE);
                        handler.setBufferSize(30);
                    } catch (Exception ex) {
                        logger.warn("create handler to st dw unsuccessfully for " + ex.getMessage(), ex);
                        handler = null;
                    }
                }
            }

            public void loadData(List<GenericRecord> pRecordSet) {
                docRecordSet.addAll(pRecordSet);
                recordNum += pRecordSet.size();
                if (recordNum >= batchSize) {
                    int tryTimes = 0;
                    for (; tryTimes < 10; tryTimes++) {
                        if (handler != null) {
                            int validRecordNum = 0;
                            for (GenericRecord docRecord : docRecordSet) {
                                try {
                                    handler.beginRow();
                                    for (int i = 0; i < columnSize; i++) {
                                        Object value = docRecord.get(tableColumnSet.get(i).getColumnName());
                                        if (value == null || value.toString().isEmpty()) {
                                            handler.setObject(tableColumnSet.get(i).getColumnIdx(), "");
                                        } else {
                                            handler.setObject(tableColumnSet.get(i).getColumnIdx(), value);
                                        }
                                    }
                                    handler.endRow();
                                    validRecordNum++;
                                } catch (Exception ex) {
                                    logger.warn("set record value unsuccessfully for " + ex.getMessage() + " and skip it", ex);
                                    continue;
                                }
                            }

                            if (validRecordNum > 0) {
                                try {
                                    long startTime = System.nanoTime();
                                    handler.execute();
                                    conn.commit();
                                    long endTime = System.nanoTime();
                                    logger.info("write " + recordNum + " records to table " + tableSe2DBRule.getTableName() + " successfully using " + (endTime - startTime) / (1024 * 1024) + " us");

                                    handler.clearRow();
                                    docRecordSet.clear();
                                    recordNum = 0;
                                    break;
                                } catch (Exception ex) {
                                    logger.error("write " + recordNum + " records to table " + tableSe2DBRule.getTableName() + " unsuccessfully for " + ex.getMessage(), ex);
                                    try {
                                        handler.rollback();
                                    } catch (Exception ex1) {
                                    } finally {
                                        handler = null;
                                    }
                                }
                            } else {
                                try {
                                    handler.clearRow();
                                } catch (Exception ex) {
                                } finally {
                                    handler = null;
                                    initHandler();
                                }
                            }
                        } else {
                            initHandler();
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

        private STSession getSTSession() {
            String key = Thread.currentThread().getId() + "";
            STSession stSession = null;
            synchronized (this) {
                stSession = stSessionSet.get(key);
            }

            if (stSession == null) {
                stSession = new STSession();
                synchronized (this) {
                    stSessionSet.put(key, stSession);
                }
            }

            return stSession;
        }

        //fixme:add timeout
        public void recieveMessages(Message msg) {
            try {
                logger.info(Thread.currentThread().getId() + " receives message " + msg.getData().length + " at " + System.nanoTime());

                //decode docs
                ByteArrayInputStream docsbis = new ByteArrayInputStream(msg.getData());//tuning
                BinaryDecoder docsbd = new DecoderFactory().binaryDecoder(docsbis, null);
                GenericArray docsSet = null;
                try {
                    GenericRecord docsRecord = new GenericData.Record(RuntimeEnv.docsSchema);
                    RuntimeEnv.docsReader.read(docsRecord, docsbd);
                    docsSet = (GenericData.Array<GenericRecord>) docsRecord.get("doc_set");
                    logger.info("docs size:" + docsSet.size());
                    if (docsSet.size() < 1) {
                        logger.warn("doc set is empty");
                        return;
                    }
                } catch (Exception ex) {
                    logger.error("parse docs unsuccessfully for " + ex.getMessage(), ex);
                    docsSet = null;
                    return;
                } finally {
                    try {
                        docsbis.close();
                    } catch (Exception ex) {
                    }
                }

                //get st session
                logger.info("getting st session for " + tableSe2DBHandler.getTableName());
                STSession stSession = getSTSession();

                Iterator<ByteBuffer> itor = docsSet.iterator();
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

                //load data
                stSession.loadData(recordSet);
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
=======
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ulss.se2db.handler;

import com.oscar.cluster.jdbc3g.Jdbc3gConnection;
import com.oscar.cluster.jdbc3g.OscarImportDistribute;
import com.oscar.cluster.util.ImportMode;
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

/**
 *
 * @author alexmu
 */
public class SE2STDBWorker extends SE2DBWorker {

    private static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(SE2STDBWorker.class.getName());
    }

    public SE2STDBWorker(TableSe2DBHandler pTableSe2DBHandler) {
        super(pTableSe2DBHandler);
    }

    @Override
    public void run() {

        TableSe2DBRule tableSe2DBRule = tableSe2DBHandler.getTableSe2DBRule();
        try {
            logger.info("write to table " + tableSe2DBRule.getTableName());

            System.setProperty("notify.remoting.max_read_buffer_size", "10485760");
            MetaClientConfig metaClientConfig = new MetaClientConfig();
            final ZkUtils.ZKConfig zkConfig = new ZkUtils.ZKConfig();

            zkConfig.zkConnect = (String) RuntimeEnv.getParam(RuntimeEnv.ZK_CLUSTER);
            metaClientConfig.setZkConfig(zkConfig);

            final MessageSessionFactory sessionFactory = new MetaMessageSessionFactory(metaClientConfig);
            String consumerGroupName = "se2stdw_" + tableSe2DBRule.getTableName() + "_" + tableSe2DBRule.getMqName() + "";
            ConsumerConfig cc = new ConsumerConfig();
            cc.setGroup(consumerGroupName);
            cc.setFetchTimeoutInMills(30000);
            cc.setMaxDelayFetchTimeInMills(500);
            cc.setFetchRunnerCount(4);
            cc.setCommitOffsetPeriodInMills(100);
            cc.setConsumerId(System.nanoTime() + "");
            final MessageConsumer consumer = sessionFactory.createConsumer(cc);

            // fetch messages
            logger.info("starting consumer worker of mq " + tableSe2DBRule.getMqName() + " for " + tableSe2DBRule.getTableName() + " ....");

            consumer.subscribe(tableSe2DBRule.getMqName(), 20 * 1024 * 1024, new STMessageListener());
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

    class STMessageListener implements MessageListener {

        Schema docSchema = tableSe2DBHandler.getDocSchema();
        DatumReader<GenericRecord> docReader = tableSe2DBHandler.getDocReader();
        TableSe2DBRule tableSe2DBRule = tableSe2DBHandler.tableSe2DBRule;
        List<TableSe2DBRule.Column> tableColumnSet = tableSe2DBRule.getColumnSet();
        int batchSize = tableSe2DBRule.getBatchSize();
        List<String> columnNameSet = null;
        int columnSize = tableColumnSet.size();
        Map<String, STSession> stSessionSet = new HashMap<String, STSession>();
        ExecutorService threadPool = null;

        public STMessageListener() {
            threadPool = Executors.newFixedThreadPool(3);
        }

        class STSession {

            Connection conn = null;
            OscarImportDistribute handler = null;
            int recordNum = 0;
            List<GenericRecord> docRecordSet = new ArrayList<GenericRecord>();

            public STSession() {
                reset();
            }

            private void reset() {

                try {
                    if (conn != null) {
                        conn.close();
                    }
                } catch (Exception ex) {
                } finally {
                    conn = null;
                }

                try {
                    Class.forName("com.oscar.cluster.Driver");
                    conn = DriverManager.getConnection(tableSe2DBRule.getConnStr(), tableSe2DBRule.getUserName(), tableSe2DBRule.getPassword());
                    conn.setAutoCommit(false);
                    logger.info("create connection to st dw unsuccessfully");
                } catch (Exception ex) {
                    logger.warn("create connection to st dw unsuccessfully for " + ex.getMessage(), ex);
                    conn = null;
                }

                handler = null;
            }

            public void initHandler() {
                if (handler == null) {
                    try {
                        //创建大容量数据导入对象
                        handler = (OscarImportDistribute) ((Jdbc3gConnection) conn).createDistributeImportHandler(tableSe2DBRule.getTableName(), ImportMode.SINGLE);
                        handler.setBufferSize(30);
                    } catch (Exception ex) {
                        logger.warn("create handler to st dw unsuccessfully for " + ex.getMessage(), ex);
                        handler = null;
                    }
                }
            }

            public void loadData(List<GenericRecord> pRecordSet) {
                docRecordSet.addAll(pRecordSet);
                recordNum += pRecordSet.size();
                if (recordNum >= batchSize) {
                    int tryTimes = 0;
                    for (; tryTimes < 10; tryTimes++) {
                        if (handler != null) {
                            int validRecordNum = 0;
                            for (GenericRecord docRecord : docRecordSet) {
                                try {
                                    handler.beginRow();
                                    for (int i = 0; i < columnSize; i++) {
                                        Object value = docRecord.get(tableColumnSet.get(i).getColumnName());
                                        if (value == null || value.toString().isEmpty()) {
                                            handler.setObject(tableColumnSet.get(i).getColumnIdx(), "");
                                        } else {
                                            handler.setObject(tableColumnSet.get(i).getColumnIdx(), value);
                                        }
                                    }
                                    handler.endRow();
                                    validRecordNum++;
                                } catch (Exception ex) {
                                    logger.warn("set record value unsuccessfully for " + ex.getMessage() + " and skip it", ex);
                                    continue;
                                }
                            }

                            if (validRecordNum > 0) {
                                try {
                                    long startTime = System.nanoTime();
                                    handler.execute();
                                    conn.commit();
                                    long endTime = System.nanoTime();
                                    logger.info("write " + recordNum + " records to table " + tableSe2DBRule.getTableName() + " successfully using " + (endTime - startTime) / (1024 * 1024) + " us");

                                    handler.clearRow();
                                    docRecordSet.clear();
                                    recordNum = 0;
                                    break;
                                } catch (Exception ex) {
                                    logger.error("write " + recordNum + " records to table " + tableSe2DBRule.getTableName() + " unsuccessfully for " + ex.getMessage(), ex);
                                    try {
                                        handler.rollback();
                                    } catch (Exception ex1) {
                                    } finally {
                                        handler = null;
                                    }
                                }
                            } else {
                                try {
                                    handler.clearRow();
                                } catch (Exception ex) {
                                } finally {
                                    handler = null;
                                    initHandler();
                                }
                            }
                        } else {
                            initHandler();
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

        private STSession getSTSession() {
            String key = Thread.currentThread().getId() + "";
            STSession stSession = null;
            synchronized (this) {
                stSession = stSessionSet.get(key);
            }

            if (stSession == null) {
                stSession = new STSession();
                synchronized (this) {
                    stSessionSet.put(key, stSession);
                }
            }

            return stSession;
        }

        //fixme:add timeout
        public void recieveMessages(Message msg) {
            try {
                logger.info(Thread.currentThread().getId() + " receives message " + msg.getData().length + " at " + System.nanoTime());

                //decode docs
                ByteArrayInputStream docsbis = new ByteArrayInputStream(msg.getData());//tuning
                BinaryDecoder docsbd = new DecoderFactory().binaryDecoder(docsbis, null);
                GenericArray docsSet = null;
                try {
                    GenericRecord docsRecord = new GenericData.Record(RuntimeEnv.docsSchema);
                    RuntimeEnv.docsReader.read(docsRecord, docsbd);
                    docsSet = (GenericData.Array<GenericRecord>) docsRecord.get("doc_set");
                    logger.info("docs size:" + docsSet.size());
                    if (docsSet.size() < 1) {
                        logger.warn("doc set is empty");
                        return;
                    }
                } catch (Exception ex) {
                    logger.error("parse docs unsuccessfully for " + ex.getMessage(), ex);
                    docsSet = null;
                    return;
                } finally {
                    try {
                        docsbis.close();
                    } catch (Exception ex) {
                    }
                }

                //get st session
                logger.info("getting st session for " + tableSe2DBHandler.getTableName());
                STSession stSession = getSTSession();

                Iterator<ByteBuffer> itor = docsSet.iterator();
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

                //load data
                stSession.loadData(recordSet);
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
>>>>>>> 24e5a68860f09b3e497aadc003941dbdbb6750b8
