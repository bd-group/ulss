/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.indexer.worker;

import cn.ac.iie.ulss.indexer.metastore.MetastoreWrapper;
import cn.ac.iie.ulss.indexer.runenvs.GlobalParas;
import cn.ac.iie.ulss.struct.DataSourceConfig;
import cn.ac.iie.ulss.struct.LuceneFileWriter;
import cn.ac.iie.ulss.utiltools.LuceneWriterUtil;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.message.MessageExt;
import devmap.DevMap;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.hadoop.hive.metastore.api.SFile;
import org.apache.hadoop.hive.metastore.api.SFileLocation;
import org.apache.hadoop.hive.metastore.model.MetaStoreConst;
import org.apache.lucene.index.IndexWriter;

/**
 *
 * @author work
 */
public class RocketdataHandler implements MessageListenerConcurrently {

    public static org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(RocketdataHandler.class.getName());
    private Protocol protocol = Protocol.parse(GlobalParas.schemaname2schemaContent.get(GlobalParas.docs));
    private Schema docsSchema = protocol.getType(GlobalParas.docs);
    private DatumReader<GenericRecord> docsReader = new GenericDatumReader<GenericRecord>(docsSchema);
    private ArrayBlockingQueue<GenericRecord> inbuffer;
    private ByteArrayInputStream docsbis = null;
    private BinaryDecoder docsbd = null;
    private AtomicLong logprintTime = null;

    public RocketdataHandler(ArrayBlockingQueue<GenericRecord> buf) {
        this.protocol = Protocol.parse(GlobalParas.schemaname2schemaContent.get(GlobalParas.docs));
        this.docsSchema = protocol.getType(GlobalParas.docs);
        this.docsReader = new GenericDatumReader<GenericRecord>(docsSchema);
        this.inbuffer = buf;
        this.logprintTime = new AtomicLong(0);
    }

    public static String getOriTopic(MessageExt msg) {
        for (String s : GlobalParas.consumeTopicsMap.keySet()) {
            if (msg.getTopic().contains(s)) {
                return s;
            }
        }
        return null;
    }

    /**
     * 默认msgs里只有一条消息，可以通过设置consumeMessageBatchMaxSize参数来批量接收消息
     */
    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
        try {
            //log.info("receive new messages size " + msgs.size());
            int docSetSize = 0;
            long bg = System.currentTimeMillis();
            ByteArrayInputStream docsbis = null;
            BinaryDecoder docsbd = null;
            GenericRecord docsRecord = null;
            IndexControler idxCtrl = null;
            GenericArray docSet = null;
            long file_id = -1;

            for (MessageExt msg : msgs) {
                docsbis = new ByteArrayInputStream(msg.getBody());
                docsbd = new DecoderFactory().binaryDecoder(docsbis, null);
                docsRecord = new GenericData.Record(docsSchema);
                try {
                    docsReader.read(docsRecord, docsbd);
                    docSet = (GenericData.Array<GenericRecord>) docsRecord.get(GlobalParas.docs_set);
                    docSetSize = docSet.size();
                    GlobalParas.getdataStatics.get(msg.getTopic()).addAndGet(docSetSize);

                    file_id = Long.parseLong(docsRecord.get(GlobalParas.sign).toString().split("[|]")[0]);
                    if (GlobalParas.isTestMode) {
                        /* fix me */
                        file_id = 100000;
                        if (GlobalParas.id2Createindex.get(file_id) != null) {
                            idxCtrl = GlobalParas.id2Createindex.get(file_id);
                            idxCtrl.inbuffer.put(docsRecord);
                        } else {
                            try {
                                Thread.sleep(2 * 1000);
                            } catch (Exception e) {
                            }
                            log.info("in test mode,there is no file to wrtite,just sleep");
                        }
                    } else {
                        idxCtrl = GlobalParas.id2Createindex.get(file_id);
                        if (idxCtrl != null) {  /*说明正在写入此文件*/
                            if (!idxCtrl.isDiskBad.get()) {  /*正在写入同时磁盘没坏*/
                                while (!idxCtrl.inbuffer.offer(docsRecord)) {
                                    Thread.sleep(600);
                                    log.info("put data to the buffer fail,will retry ...");
                                }
                                //idxCtrl.inbuffer.put(docsRecord);
                            } else { /*正在写入同时磁盘坏掉*/
                                log.info("put back some disk bad data for " + " " + file_id + "  " + msg.getTopic());
                                //将错误的数据或者无法写入的数据塞回消息队列，并写入一条错误数据信息,前端获取此消息后进行处理
                                String information = file_id + "|" + RocketdataHandler.getOriTopic(msg) + "|||" + GlobalParas.hostName;
                                if (GlobalParas.diskErrorFileId.containsKey(file_id)) {
                                    //如果已经将此坏掉的file_id缓存起来,就将数据写回即可，将消息写入队列，注意其实可以只写一次,
                                    //但是一旦重分布漏掉消息，重分部可能还会继续续发数据，持久化又会将数据退回就会造成数据永远无法写入
                                    if (System.currentTimeMillis() - GlobalParas.diskErrorFileId.get(file_id) >= GlobalParas.badfileinfoCheckGab * 1000) {
                                        RocketMQProducer.sendMessage(GlobalParas.error_file_mq, information.getBytes(), -1);
                                        GlobalParas.diskErrorFileId.put(file_id, System.currentTimeMillis());//更改错误message发送时的update时间
                                    }
                                    log.info("put back data for disk error " + file_id);
                                    RocketMQProducer.sendMessage(RocketdataHandler.getOriTopic(msg), msg.getBody(), docSetSize);
                                    continue;
                                } else {
                                    //如果没有将此坏掉的file_id缓存起来,就将消息写入队列，并将数据重新写入队列
                                    RocketMQProducer.sendMessage(GlobalParas.error_file_mq, information.getBytes(), -1);
                                    RocketMQProducer.sendMessage(RocketdataHandler.getOriTopic(msg), msg.getBody(), docSetSize);
                                    //note it
                                    GlobalParas.diskErrorFileId.put(file_id, System.currentTimeMillis());
                                    log.info("will set file bad for disk errors for " + file_id);
                                    idxCtrl.isFileStatusBad.set(true);
                                    MetastoreWrapper.setFileBad(file_id);
                                    log.info("set file bad for disk errors done for " + file_id);
                                    continue;
                                }
                            }
                        } else {  /*说明文件没有在写入，处于close或者revmove,或者其他状态*/
                            if (GlobalParas.diskErrorFileId.containsKey(file_id)) {
                                if (System.currentTimeMillis() - GlobalParas.diskErrorFileId.get(file_id) >= GlobalParas.badfileinfoCheckGab * 1000) {
                                    String information = file_id + "|" + RocketdataHandler.getOriTopic(msg) + "|||" + GlobalParas.hostName;
                                    RocketMQProducer.sendMessage(GlobalParas.error_file_mq, information.getBytes(), -1);
                                    GlobalParas.diskErrorFileId.put(file_id, System.currentTimeMillis());//更改错误message发送时的update时间
                                    log.info("put back data that is in disk error filelist " + information);
                                }
                                RocketMQProducer.sendMessage(RocketdataHandler.getOriTopic(msg), msg.getBody(), docSetSize);
                                continue;
                            } else if (GlobalParas.removedFileId.containsKey(file_id)) {
                                if (System.currentTimeMillis() - GlobalParas.removedFileId.get(file_id) >= GlobalParas.badfileinfoCheckGab * 1000) {
                                    String information = file_id + "|" + RocketdataHandler.getOriTopic(msg) + "|||" + GlobalParas.hostName;
                                    RocketMQProducer.sendMessage(GlobalParas.error_file_mq, information.getBytes(), -1);
                                    GlobalParas.removedFileId.put(file_id, System.currentTimeMillis());//更改错误message发送时的update时间
                                    log.info("put back data that is in removed filelist " + information);
                                }
                                RocketMQProducer.sendMessage(RocketdataHandler.getOriTopic(msg), msg.getBody(), docSetSize);
                                continue;
                            } else if (GlobalParas.metaORdeviceErrorFileId.containsKey(file_id)) {
                                if (System.currentTimeMillis() - GlobalParas.metaORdeviceErrorFileId.get(file_id) >= GlobalParas.badfileinfoCheckGab * 1000) {
                                    String information = file_id + "|" + RocketdataHandler.getOriTopic(msg) + "|||" + GlobalParas.hostName;
                                    RocketMQProducer.sendMessage(GlobalParas.error_file_mq, information.getBytes(), -1);
                                    GlobalParas.metaORdeviceErrorFileId.put(file_id, System.currentTimeMillis());//更改错误message发送时的update时间
                                    log.info("put back data that is in meta or wrong device error filelist " + information);
                                }
                                RocketMQProducer.sendMessage(RocketdataHandler.getOriTopic(msg), msg.getBody(), docSetSize);
                                continue;
                            }
                            bg = System.currentTimeMillis();

                            synchronized (this) {
                                try {
                                    if (GlobalParas.diskErrorFileId.containsKey(file_id)) {
                                        if (System.currentTimeMillis() - GlobalParas.diskErrorFileId.get(file_id) >= GlobalParas.badfileinfoCheckGab * 1000) {
                                            String information = file_id + "|" + RocketdataHandler.getOriTopic(msg) + "|||" + GlobalParas.hostName;
                                            RocketMQProducer.sendMessage(GlobalParas.error_file_mq, information.getBytes(), -1);
                                            GlobalParas.diskErrorFileId.put(file_id, System.currentTimeMillis());//更改错误message发送时的update时间
                                            log.info("put back data that is in disk error filelist " + information);
                                        }
                                        RocketMQProducer.sendMessage(RocketdataHandler.getOriTopic(msg), msg.getBody(), docSetSize);
                                        continue;
                                    } else if (GlobalParas.removedFileId.containsKey(file_id)) {
                                        if (System.currentTimeMillis() - GlobalParas.removedFileId.get(file_id) >= GlobalParas.badfileinfoCheckGab * 1000) {
                                            String information = file_id + "|" + RocketdataHandler.getOriTopic(msg) + "|||" + GlobalParas.hostName;
                                            RocketMQProducer.sendMessage(GlobalParas.error_file_mq, information.getBytes(), -1);
                                            GlobalParas.removedFileId.put(file_id, System.currentTimeMillis());//更改错误message发送时的update时间
                                            log.info("put back data that is in removed filelist " + information);
                                        }
                                        RocketMQProducer.sendMessage(RocketdataHandler.getOriTopic(msg), msg.getBody(), docSetSize);
                                        continue;
                                    } else if (GlobalParas.metaORdeviceErrorFileId.containsKey(file_id)) {
                                        if (System.currentTimeMillis() - GlobalParas.metaORdeviceErrorFileId.get(file_id) >= GlobalParas.badfileinfoCheckGab * 1000) {
                                            String information = file_id + "|" + RocketdataHandler.getOriTopic(msg) + "|||" + GlobalParas.hostName;
                                            RocketMQProducer.sendMessage(GlobalParas.error_file_mq, information.getBytes(), -1);
                                            GlobalParas.metaORdeviceErrorFileId.put(file_id, System.currentTimeMillis());//更改错误message发送时的update时间
                                            log.info("put back back data that is in device error filelist " + information);
                                        }
                                        RocketMQProducer.sendMessage(RocketdataHandler.getOriTopic(msg), msg.getBody(), docSetSize);
                                        continue;
                                    }

                                    if (!GlobalParas.id2Createindex.containsKey(file_id)) {/*为什么在这里还会判断,这是有原因的*/
                                        log.warn("the file has not been init in persi, but it is ok in metastore, get it from metastore: " + file_id);
                                        StringBuilder sb = new StringBuilder();
                                        SFile sf = MetastoreWrapper.getFile(file_id, sb);
                                        /**/
                                        if (sf == null) {
                                            String information = file_id + "|" + RocketdataHandler.getOriTopic(msg) + "|||" + GlobalParas.hostName;
                                            RocketMQProducer.sendMessage(GlobalParas.error_file_mq, information.getBytes(), -1);
                                            RocketMQProducer.sendMessage(RocketdataHandler.getOriTopic(msg), msg.getBody(), docSetSize);
                                            GlobalParas.metaORdeviceErrorFileId.put(file_id, System.currentTimeMillis());
                                            log.info("will set file bad for meta or device error(get file) for " + file_id);
                                            MetastoreWrapper.setFileBad(file_id);
                                            log.info("set file bad for meta or device error(get file) done for " + file_id);
                                            continue;
                                        }
                                        if (sf.getStore_status() != MetaStoreConst.MFileStoreStatus.INCREATE) {
                                            /*既然不是increate状态，那么就先查查是被删除了还是被损坏（磁盘满、磁盘只读等）,*然后检查是否是被删除（物理删除还是逻辑删除）*/
                                            /*如果文件是逻辑删除或者是物理删除在，此文件是不能使用的*/
                                            if (sf.getStore_status() == MetaStoreConst.MFileStoreStatus.RM_LOGICAL || sf.getStore_status() == MetaStoreConst.MFileStoreStatus.RM_PHYSICAL) {
                                                String values = "|" + sf.getValues().get(0).getValue() + "," + sf.getValues().get(1).getValue() + "|" + sf.getValues().get(2).getValue();
                                                String information = file_id + "|" + RocketdataHandler.getOriTopic(msg) + "|||" + GlobalParas.hostName;
                                                RocketMQProducer.sendMessage(GlobalParas.error_file_mq, information.getBytes(), -1);
                                                RocketMQProducer.sendMessage(RocketdataHandler.getOriTopic(msg), msg.getBody(), docSetSize);
                                                log.warn("the data is not in increate status,it is: " + information + " " + sf + ",the values are " + values);
                                                GlobalParas.removedFileId.put(file_id, System.currentTimeMillis());
                                                continue;
                                            } else {
                                                //如果文件是刚刚关闭正在复制或者文件已经有了副本，那么就可以reopen，由平台来处理余下的复制等
                                                sb = new StringBuilder();
                                                if (MetastoreWrapper.reopenFile(file_id)) {
                                                    sf = MetastoreWrapper.getFile(file_id, sb);
                                                    log.info("now reopen the file " + file_id + " done,the file is " + sf);
                                                } else {  /*reopen不成功的话就将消息和数据退回,就不再使用此文件*/
                                                    String information = file_id + "|" + RocketdataHandler.getOriTopic(msg) + "|||" + GlobalParas.hostName;
                                                    RocketMQProducer.sendMessage(GlobalParas.error_file_mq, information.getBytes(), -1);
                                                    RocketMQProducer.sendMessage(RocketdataHandler.getOriTopic(msg), msg.getBody(), docSetSize);
                                                    GlobalParas.metaORdeviceErrorFileId.put(file_id, System.currentTimeMillis());
                                                    log.info("will set file bad for meta or device error(reopen file) for " + information + ",before reopen: " + sf);
                                                    MetastoreWrapper.setFileBad(file_id);
                                                    log.info("set file bad for meta or device error(reopen file) done for " + information + ",before reopen: " + sf);
                                                    continue;
                                                }
                                            }
                                        } else {
                                            log.info("get one exist file ok,the file is " + sf.toString());
                                        }

                                        List<SFile> luceneFileList = new ArrayList<SFile>();
                                        String db = sf.getDbName();
                                        String tb = sf.getTableName();
                                        luceneFileList.add(sf);
                                        DevMap dm = new DevMap();
                                        String fullPath = "";
                                        SFileLocation location = null;
                                        try {
                                            for (SFileLocation tmpLocation : sf.getLocations()) {
                                                if (GlobalParas.hostName.equalsIgnoreCase(tmpLocation.getNode_name())) {
                                                    location = tmpLocation;
                                                    break;
                                                }
                                            }
                                            fullPath = dm.getPath(location.getDevid(), location.getLocation());  //paras: device id,path
                                        } catch (Exception e) {
                                            /*将设备名解析成路径时报错，这有可能是对应额fid本应该到节点1对应的队列上却到了节点2对应的队列上，此时肯定是重分布出现了严重的问题，将数据推回*/
                                            log.error(e, e);
                                            String information = file_id + "|" + RocketdataHandler.getOriTopic(msg) + "|||" + GlobalParas.hostName;
                                            RocketMQProducer.sendMessage(GlobalParas.error_file_mq, information.getBytes(), -1);
                                            RocketMQProducer.sendMessage(RocketdataHandler.getOriTopic(msg), msg.getBody(), docSetSize);

                                            GlobalParas.metaORdeviceErrorFileId.put(file_id, System.currentTimeMillis());
                                            log.info("will set file bad for device error(get file) for " + file_id + " " + sf.toString());
                                            MetastoreWrapper.setFileBad(file_id);
                                            log.info("set bad done for device error(get file) for " + file_id + " " + sf.toString());
                                            continue;
                                        }
                                        log.info("will get the lucene file from disk,the fullpath is " + fullPath);

                                        String stTime = "";
                                        String edTime = "";
                                        for (int j = 0; j < 2; j++) {
                                            long time = Long.parseLong(sf.getValues().get(j).getValue().trim());
                                            if (j == 0) {
                                                stTime = String.valueOf(time);
                                            } else {
                                                edTime = String.valueOf(time);
                                            }
                                        }
                                        log.info("now set the time flag for file " + file_id + " to " + stTime + "_" + edTime);
                                        DataSourceConfig ds = new DataSourceConfig(db, tb, stTime);
                                        ConcurrentHashMap<Integer, IndexWriter> hashWriterMap = new ConcurrentHashMap<Integer, IndexWriter>();
                                        ConcurrentHashMap<Integer, List<SFile>> luceneSFMap = new ConcurrentHashMap<Integer, List<SFile>>();
                                        try {
                                            IndexWriter FSWriter = LuceneWriterUtil.getSimpleLuceneWriter(fullPath, false);
                                            FSWriter.close();
                                            FSWriter = LuceneWriterUtil.getSimpleLuceneWriter(fullPath, false);
                                            log.info("get one lucene physicical file ok ->" + file_id + ";" + Thread.currentThread().getName() + "-" + Thread.currentThread().getId());
                                            hashWriterMap.put(0, FSWriter);
                                            luceneSFMap.put(0, luceneFileList);
                                            LuceneFileWriter writer = new LuceneFileWriter(hashWriterMap, luceneSFMap);

                                            ArrayBlockingQueue buf = new ArrayBlockingQueue(GlobalParas.readbufSize);
                                            IndexControler c = new IndexControler(ds, buf, writer, file_id);
                                            c.threadPool = Workerpool.hlwrzpool;
                                            Thread ct = new Thread(c);
                                            ct.setName(ds.getDbName() + "_" + ds.getTbName() + "_" + file_id);
                                            ct.start();

                                            GlobalParas.id2Createindex.put(file_id, c);
                                            log.info("apply one file from metastore use  " + (System.currentTimeMillis() - bg) + " ms for " + file_id);
                                        } catch (Exception e) {
                                            log.error(e, e);
                                            //就是从初始化lucene文件以后的代码开始如果出错了，要么就是从元数据中获取了文件，但是物理文件无法初始化或者对应的控制线程无法初始化
                                            //整个文件也就无法使用的,GlobalParas.id2Createindex.get(file_id).inbuffer.put(docsRecord)也不应该执行,数据退回（至于file_id该不该写入disk bad）
                                            //error中后面再说吧
                                            String information = file_id + "|" + RocketdataHandler.getOriTopic(msg) + "|||" + GlobalParas.hostName;
                                            RocketMQProducer.sendMessage(GlobalParas.error_file_mq, information.getBytes(), -1);
                                            RocketMQProducer.sendMessage(RocketdataHandler.getOriTopic(msg), msg.getBody(), docSetSize);

                                            GlobalParas.metaORdeviceErrorFileId.put(file_id, System.currentTimeMillis());
                                            log.info("will set file bad for meta or device error(get file) for " + information + " " + e);
                                            MetastoreWrapper.setFileBad(file_id);
                                            log.info("set file bad for meta or device error(get file) done for " + information + " " + e);
                                            continue;
                                        }
                                        log.info("check or new file for file id " + file_id + " use " + (System.currentTimeMillis() - bg) + " ms");
                                    }
                                    while (!GlobalParas.id2Createindex.get(file_id).inbuffer.offer(docsRecord)) {
                                        Thread.sleep(300);
                                        log.info("put data to the buffer fail,will retry ...");
                                    }
                                    //GlobalParas.id2Createindex.get(file_id).inbuffer.put(docsRecord);
                                } catch (Exception e) {
                                    log.error(e, e);
                                } finally {
                                    continue;
                                }
                            }
                        }
                    }
                } catch (Exception ex) {
                    log.error(ex, ex);
                }
            }
        } catch (Exception e) {
            log.error(e, e);
        }
        try {
            if (System.currentTimeMillis() - this.logprintTime.get() >= 3000) {
                this.logprintTime.set(System.currentTimeMillis());
                for (String s : GlobalParas.getdataStatics.keySet()) {
                    log.info("new receive data in total is: " + s + " " + GlobalParas.getdataStatics.get(s).get());
                }
            }
        } catch (Exception e) {
            log.error(e, e);
        }
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }
}
