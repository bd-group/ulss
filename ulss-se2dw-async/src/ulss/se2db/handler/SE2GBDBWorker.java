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
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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
            cc.setFetchTimeoutInMills(1000);
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
        GBLoadClient gbClient = new GBLoadClient();
        Statement stmt = null;
        int record2CommitNum = 0;

        public GBMessageListener(List<String> pColumnNameSet) {
            columnNameSet = pColumnNameSet;
            String[] connStrItems = tableSe2DBRule.getConnStr().split(":");
            int rv = gbClient.connect(connStrItems[0], Integer.parseInt(connStrItems[1]), null, null);
            if (rv != 0) {
                logger.error("connect to " + tableSe2DBRule.getConnStr() + " unsuccessfully");
            } else {
                logger.info("connect to " + tableSe2DBRule.getConnStr() + " successfully");
                stmt = gbClient.startLoad("logdb", tableSe2DBRule.getTableName(), columnNameSet, "");
            }
        }

        //fixme:add timeout
        public void recieveMessages(Message msg) {
            logger.info(Thread.currentThread().getId() + " receives message " + msg.getData().length + " at " + System.nanoTime());
            ByteArrayInputStream docsbis = new ByteArrayInputStream(msg.getData());//tuning
            BinaryDecoder docsbd = new DecoderFactory().binaryDecoder(docsbis, null);

            GenericRecord docsRecord = new GenericData.Record(RuntimeEnv.docsSchema);
            try {
                RuntimeEnv.docsReader.read(docsRecord, docsbd);

                GenericRecord docRecord = new GenericData.Record(docSchema);
                GenericArray docsset = (GenericData.Array<GenericRecord>) docsRecord.get("doc_set");
                logger.info("docs size:" + docsset.size());


                List<Row> rows = new ArrayList<Row>();
                Iterator<ByteBuffer> itor = docsset.iterator();
                long startTime = System.nanoTime();
                while (itor.hasNext()) {
                    ByteArrayInputStream docbis = new ByteArrayInputStream(((ByteBuffer) itor.next()).array());
                    BinaryDecoder docbd = new DecoderFactory().binaryDecoder(docbis, null);

                    try {
                        docReader.read(docRecord, docbd);
                    } catch (Exception ex) {
                        logger.warn("parse doc unsuccessfully for " + ex.getMessage() + " and skip it");
                        continue;
                    }
                    Row row = new Row();
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
                }
                long endTime = System.nanoTime();

                logger.info("build rows use " + (endTime - startTime) / (1024 * 1024));


                startTime = System.nanoTime();
                for (int tryTime = 0; tryTime < 10; tryTime++) {
                    logger.info("try " + tryTime + "st time...");

                    if (stmt == null) {
                        continue;
                    }

                    int rv = stmt.addRows(rows);
                    //fixme
                    if (rv != 0) {
                        gbClient.rollback();
                        logger.error("write " + rows.size() + " rows to table " + tableSe2DBRule.getTableName() + " unsuccessfully ");
                        continue;
                    } else {
                        logger.info("write " + rows.size() + " rows to table " + tableSe2DBRule.getTableName() + " successfully");
                        synchronized (this) {
                            record2CommitNum += rows.size();
                            if (record2CommitNum >= 10000) {
                                rv = gbClient.commit();
                                if (rv == 0) {
                                    logger.info("commit " + record2CommitNum + " rows to table " + tableSe2DBRule.getTableName() + " successfully");
                                    record2CommitNum = 0;
                                } else {
                                    //fixme
                                }
                            }
                        }
                        break;
                    }
                }
                endTime = System.nanoTime();
            } catch (Exception ex) {
                logger.warn("parse docs unsuccessfully for " + ex.getMessage(), ex);
            }
        }

        public Executor getExecutor() {
            // Thread pool to process messages,maybe null.
            ExecutorService threadPool = Executors.newFixedThreadPool(100);
            return threadPool;
        }
    }
}
