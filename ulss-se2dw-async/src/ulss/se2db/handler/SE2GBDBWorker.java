/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ulss.se2db.handler;

import cn.gbase.gb8a.gbloadapi.GBLoadClient;
import cn.gbase.gb8a.gbloadapi.GBLoadException;
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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
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
            cc.setFetchRunnerCount(4);
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
        List<String> columnNameSet = null;
        int columnSize = tableColumnSet.size();
        GBLoadClient gbClient = null;
        int record2CommitNum = 0;

        public GBMessageListener(List<String> pColumnNameSet) {
            columnNameSet = pColumnNameSet;
        }

        private Statement getStatement() {
            Statement stmt = null;
            if (gbClient == null) {
                synchronized (this) {
                    if (gbClient == null) {
                        try {
                            String[] connStrItems = tableSe2DBRule.getConnStr().split(":");
                            gbClient = new GBLoadClient();
                            int tmprv = gbClient.connect(connStrItems[0], Integer.parseInt(connStrItems[1]), "", "");
                            if (tmprv != 0) {
                                gbClient.disconnect();
                                gbClient = null;
                            }
                        } catch (Exception ex) {
                            logger.error("connect to gbase unsuccessfully for " + ex.getMessage(), ex);
                            gbClient = null;
                        }
                    }
                }
            }

            if (gbClient != null) {
                try {
                    stmt = gbClient.startLoad("logdb", tableSe2DBRule.getTableName(), columnNameSet);
                } catch (Exception ex) {
                    synchronized (this) {
                        try {
                            gbClient.disconnect();
                        } catch (Exception ex1) {
                            logger.error(ex1.getMessage());
                        }
                        gbClient = null;
                    }
                    logger.error("create session to gdbase unsuccessfully for " + ex.getMessage(), ex);
                }
            }

            return stmt;
        }

        //fixme:add timeout
        public void recieveMessages(Message msg) {
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

            long startTime = -1;
            long endTime = -1;
            int validRecordNum = 0;

            while (true) {
                //get stmt
                Statement stmt = getStatement();
                while (true) {
                    if (stmt == null) {
                        logger.warn("stmt is null and try to constrauct it");
                        stmt = getStatement();
                    } else {
                        logger.warn("construct handler successfully");
                        break;
                    }
                }

                //build rows
                List<Row> rows = new ArrayList<Row>();
                Iterator<ByteBuffer> itor = docsset.iterator();
                startTime = System.nanoTime();
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

                    //build row
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
                        validRecordNum++;
                    } catch (Exception ex) {
                        logger.warn("set record value unsuccessfully for " + ex.getMessage() + " and skip it", ex);
                        continue;
                    }
                }
                endTime = System.nanoTime();
                logger.info("build rows use " + (endTime - startTime) / (1024 * 1024) + "us with " + validRecordNum + " valid records");

                //insert into dw
                if (validRecordNum > 0) {
                    try {
                        stmt.addRows(rows);
                        startTime = System.nanoTime();
                        stmt.commit();
                        endTime = System.nanoTime();
                        logger.info("write " + validRecordNum + " records to table " + tableSe2DBRule.getTableName() + " successfully using " + (endTime - startTime) / (1024 * 1024) + " us");
                        break;
                    } catch (Exception ex) {
                        logger.error("write " + validRecordNum + " records to table " + tableSe2DBRule.getTableName() + " unsuccessfully ");
                        try {
                            stmt.rollback();
                        } catch (Exception ex1) {
                        }
                    } finally {
                        try {
                            stmt.release();
                        } catch (Exception ex1) {
                        }
                        stmt = null;
                    }
                } else {
                    logger.warn("no records to commit");
                    break;
                }
            }
        }

        public Executor getExecutor() {
            // Thread pool to process messages,maybe null.
//            ExecutorService threadPool = Executors.newFixedThreadPool(100);
            return null;
        }
    }
}
