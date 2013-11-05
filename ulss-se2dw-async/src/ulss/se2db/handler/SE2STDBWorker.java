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
import com.taobao.metamorphosis.client.consumer.MessageIterator;
import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.utils.ZkUtils;
import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
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

//        TableSe2DBRule tableSe2DBRule = tableSe2DBHandler.getTableSe2DBRule();
//        Connection conn = null;
//        OscarImportDistribute handler = null;
//        try {
//
//            Class.forName("com.oscar.cluster.Driver");
////                jdbc url format:"jdbc:oscarcluster://" + host + ":" + port + "/" + dbName
//            conn = DriverManager.getConnection(tableSe2DBRule.getJdbcURL(), tableSe2DBRule.getUserName(), tableSe2DBRule.getPassword());
//            conn.setAutoCommit(true);
////                conn = DriverManager.getConnection(tableSe2DBRule.getJdbcURL(), tableSe2DBRule.getUserName(), tableSe2DBRule.getPassword());
////
////                //创建大容量数据导入对象
//            logger.info("write to table " + tableSe2DBRule.getTableName());
//            handler = (OscarImportDistribute) ((Jdbc3gConnection) conn).createDistributeImportHandler(tableSe2DBRule.getTableName(), ImportMode.SINGLE);
////
//            handler.setBufferSize(30);
//
//            System.setProperty("notify.remoting.max_read_buffer_size", "10485760");
//            MetaClientConfig metaClientConfig = new MetaClientConfig();
//            final ZkUtils.ZKConfig zkConfig = new ZkUtils.ZKConfig();
//
//            zkConfig.zkConnect = (String) RuntimeEnv.getParam(RuntimeEnv.ZK_CLUSTER);
//            metaClientConfig.setZkConfig(zkConfig);
//
//            final MessageSessionFactory sessionFactory = new MetaMessageSessionFactory(metaClientConfig);
//            String consumerGroupName = "se2stdw_" + tableSe2DBRule.getTableName() + "_" + tableSe2DBRule.getMqName();
//            ConsumerConfig cc = new ConsumerConfig();
//            cc.setGroup(consumerGroupName);
//            cc.setFetchTimeoutInMills(1000000);
//            cc.setMaxDelayFetchTimeInMills(500);
//            cc.setFetchRunnerCount(1);
//            final MessageConsumer consumer = sessionFactory.createConsumer(cc);
//
//
//            // start offset
//            long offset = 0;
//            MessageIterator it = null;
//
//            Schema docSchema = tableSe2DBHandler.getDocSchema();
//            DatumReader<GenericRecord> docReader = tableSe2DBHandler.getDocReader();
//            int record2CommitNum = 0;
//            // fetch messages
//            logger.info("starting consumer worker of mq " + tableSe2DBRule.getMqName() + " for " + tableSe2DBRule.getTableName() + " ....");
//            while (true) {
//                try {
//                    while ((it = consumer.get(tableSe2DBRule.getMqName(), new Partition(partitionName), offset, 20 * 1024 * 1024)) != null) {
//                        logger.info("received msgs...");
//                        while (it.hasNext()) {
//                            final Message msg = it.next();
//                            logger.info("Receive message " + msg.getData().length);
//
//                            ByteArrayInputStream docsbis = new ByteArrayInputStream(msg.getData());//tuning
//                            BinaryDecoder docsbd = new DecoderFactory().binaryDecoder(docsbis, null);
//
//                            GenericRecord docsRecord = new GenericData.Record(RuntimeEnv.docsSchema);
//                            try {
//                                RuntimeEnv.docsReader.read(docsRecord, docsbd);
//
//                                GenericRecord docRecord = new GenericData.Record(docSchema);
//                                GenericArray docsset = (GenericData.Array<GenericRecord>) docsRecord.get("doc_set");
//                                logger.info("docs size:" + docsset.size());
//
//                                while (true) {
//                                    Iterator<ByteBuffer> itor = docsset.iterator();
//                                    while (itor.hasNext()) {
//                                        ByteArrayInputStream docbis = new ByteArrayInputStream(((ByteBuffer) itor.next()).array());
//                                        BinaryDecoder docbd = new DecoderFactory().binaryDecoder(docbis, null);
//
//                                        docReader.read(docRecord, docbd);
//                                        //fixme call jdbc
//                                        List<Column> tableColumnSet = tableSe2DBRule.getColumnSet();
//                                        int columnSize = tableColumnSet.size();
//                                        try {
//                                            for (int i = 0; i < columnSize; i++) {
//                                                handler.setString(tableColumnSet.get(i).getColumnIdx(), docRecord.get(tableColumnSet.get(i).getColumnName()).toString());
//                                            }
//                                            handler.endRow();
//                                            record2CommitNum++;
//                                        } catch (Exception ex) {
//                                            logger.warn("set value unsuccessfully for " + ex.getMessage(), ex);
//                                        }
//                                    }
//
//                                    try {
//                                        //提交数据
//                                        handler.execute();
//                                        logger.info("commit " + record2CommitNum + " records to table " + tableSe2DBRule.getTableName() + " on " + tableSe2DBRule.getDbType() + " db successfully");
//                                        break;
//                                    } catch (Exception ex) {
//                                        logger.warn("commit " + record2CommitNum + " records to " + tableSe2DBRule.getTableName() + " on " + tableSe2DBRule.getDbType() + " unsuccessfully for " + ex.getMessage(), ex);
//                                    } finally {
//                                        try {
//                                            handler.close();
//                                            handler = null;
//                                        } catch (Exception ex) {
//                                        }
//
//                                        try {
//                                            handler = (OscarImportDistribute) ((Jdbc3gConnection) conn).createDistributeImportHandler(tableSe2DBRule.getTableName(), ImportMode.SINGLE);
//                                            handler.setBufferSize(30);
//                                        } catch (Exception ex) {
//                                        }
//                                    }
//                                }
//                            } catch (Exception ex) {
//                                logger.warn("parse docs unsuccessfully for " + ex.getMessage(), ex);
//                            }
//                        }
//
//
//
//
//
//                        record2CommitNum = 0;
//
//
//                        logger.info("stopped:" + stopped);
//                        if (stopped) {
//                            break;
//                        }
//                        // move offset forward
//                        offset += it.getOffset();
//                    }
//                } catch (Exception ex) {
//                    ex.printStackTrace();
//                } finally {
//                    if (record2CommitNum > 0) {
//                        try {
//                            //提交数据
//                            handler.execute();
//                            logger.info("commit " + record2CommitNum + " records to table " + tableSe2DBRule.getTableName() + " on " + tableSe2DBRule.getDbType() + " db successfully");
//                        } catch (Exception ex) {
//                            logger.warn("commit " + record2CommitNum + " records to " + tableSe2DBRule.getTableName() + " on " + tableSe2DBRule.getDbType() + " unsuccessfully for " + ex.getMessage(), ex);
//                        } finally {
//                            try {
//                                handler.close();
//                                handler = null;
//                            } catch (Exception ex) {
//                            }
//
//                            try {
//                                handler = (OscarImportDistribute) ((Jdbc3gConnection) conn).createDistributeImportHandler(tableSe2DBRule.getTableName(), ImportMode.SINGLE);
//                                handler.setBufferSize(30);
//                            } catch (Exception ex) {
//                            }
//                        }
//                        record2CommitNum = 0;
//                    }
//                }
//
//                if (stopped) {
//                    break;
//                } else {
//                    try {
//                        logger.info("waiting...");
//                        Thread.sleep(5000);
//                    } catch (Exception ex) {
//                    }
//                }
//            }
//            consumer.shutdown();
//            sessionFactory.shutdown();
//            logger.info("consumer " + consumerName + " exits successfully");
//        } catch (Exception ex) {
//            ex.printStackTrace();
//        } finally {
//            try {
//                if (handler != null) {
//                    handler.close();
//                }
//                if (conn != null) {
//                    conn.close();
//                }
//            } catch (SQLException exp) {
//                exp.printStackTrace();
//            }
//        }
    }
}
