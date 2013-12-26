/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.metastore;

import cn.ac.iie.ulss.indexer.Indexer;
import devmap.DevMap;
import iie.metastore.MetaStoreClient;
import iie.mm.client.ClientAPI;
import iie.mm.client.ClientConf;
import java.io.File;
import java.util.List;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Node;
import org.apache.hadoop.hive.metastore.api.SFile;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.model.MetaStoreConst;
import org.apache.hadoop.hive.metastore.tools.PartitionFactory;
import org.apache.hadoop.hive.metastore.tools.PartitionFactory.PartitionInfo;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.thrift.TException;

public class MetaStoreTest {

    public static Logger log = Logger.getLogger(MetaStoreTest.class.getName());

    public static void main(String[] argss) throws Exception {
        PropertyConfigurator.configure("log4j.properties");
        MetaStoreClient cli = null;
        try {
            cli = new MetaStoreClient("192.168.1.14", 10101);//10101
        } catch (MetaException ex) {
            log.error(ex, ex);
        }
        IMetaStoreClient icli = cli.client;

        //List<Database> dbs = cli.client.get_local_attribution();


        System.out.println(cli.client.get_local_attribution().getParameters().get("mm.url"));
//        for (Database db : dbs) {
//            String dbName = db.getName();
//            String mmurl = db.getParameters().get("mm.url");
//            if (mmurl != null && !"".equals(mmurl)) {
//                System.out.println(dbName + " " + mmurl);
//            } else {
//                log.info("the mm.url for " + dbName + " is null ");
//            }
//        }
        SFile f = icli.get_file_by_id(2623);
        System.out.println(f);
        System.out.println(f.getValues().get(0).getValue());
        System.out.println(f.getValues().get(1));
        System.out.println(f.getValues().get(2) + "\n\n\n");
        //System.out.println(f.getStore_status() == MetaStoreConst.MFileStoreStatus.CLOSED);
        //System.out.println(f);

        Table bf_dxx = null;
        try {
            System.out.println(icli.getAllDatabases());
            bf_dxx = icli.getTable("db1", "t_dx_rz_qydx");
            System.out.println(icli.getTableNodeGroups("db1", "t_dx_rz_qydx"));
            System.out.println("the size is ..." + cli.client.listTableFiles("db1", "t_dx_rz_qydx", 0, 100000).get(100));
            List<Index> idxList = cli.client.listIndexes("db1", "t_dx_rz_qydx", (short) -1);
            for (Index ix : idxList) {
                System.out.println(ix.getIndexName());
            }


            //System.out.println(idxList);
            //SFile f = icli.get_file_by_id(96);

            //System.out.println(f);
            //icli.filterTableFiles(null, null, null);

            DevMap dm = new DevMap();
            String fullPath = dm.getPath("222e1000155b7615b", "/data/db1/wb/1538047325");
            log.info("new one lucene file,the fullpath is " + fullPath);

            System.out.println(f.getFid());
            System.out.println(f.getValues());
            System.out.println(f.getLength());
            System.out.println(f.getLocations().get(0).getLocation());
            System.out.println("the node name is " + f.getLocations().get(0).getNode_name());
            System.out.println(f.getLocations().get(0).getDigest());

            //。。load_status    record_num 、allrecordnumb、 副本数  、splitvalue、长度 、digest(先不算)

//            DevMap dm = new DevMap();
//            String path = dm.getPath(f.getLocations().get(0).getLocation());
//            path += "/bloomfilter.bf";
//            File file = new File(path);
//            File parent = file.getParentFile();
//            if (parent != null && !parent.exists()) {
//                parent.mkdirs();
//            }

            //"cmd"-- - "start|fid|
            //msCli.client.close_file(sf);

            SFile f1 = new SFile();
            f1.setDbName(null);
            f1.setFid(0);
            f1.setLocations(null);// ??数据是有多个副本的，那么每个副本都有一个location的

            System.out.println(bf_dxx.getNodeGroups());

            //((Node) (bf_dxx.getNodeGroups().get(0).getNodes().toArray())[0]).getIps();


            System.out.println(icli.listNodes().get(0).getIps());

            List<FieldSchema> splitKeys = bf_dxx.getFileSplitKeys();
            System.out.println(splitKeys);

            String split_name_l1 = ""; //String split_name_l1 =  pis.get(0).getP_col()??getP_type().getName();
            String split_name_l2 = "";
            String part_type_l1 = "";
            String part_type_l2 = "";
            int l1_part_num = 0;
            int l2_part_num = 0;

            List<PartitionInfo> pis = PartitionInfo.getPartitionInfo(splitKeys);

            if (pis.size() == 1) {  //一级分区
                PartitionFactory.PartitionInfo pinfo = pis.get(0);
                split_name_l1 = pinfo.getP_col();  //使用哪一列进行分区
                part_type_l1 = pinfo.getP_type().getName();   //这级分区的方式，是hash还是interval ?
                l1_part_num = pinfo.getP_num();   //分区有多少个，如果是hash的话n个分区，那么特征值就是0,1,2 。。。 n-1

                System.out.println("part level is: " + pinfo.getP_level());
                System.out.println("part cols is: " + split_name_l1);
                System.out.println("partition type is: " + part_type_l1);
                System.out.println("partition num is: " + l1_part_num);
            } else if (pis.size() == 2) {
                for (PartitionFactory.PartitionInfo pinfo : pis) {
                    if (pinfo.getP_level() == 1) {         //分区是第几级？一级还是二级(现在支持一级interval、hash和一级interv、二级hash 三种分区方式)
                        split_name_l1 = pinfo.getP_col();  //使用哪一列进行分区
                        part_type_l1 = pinfo.getP_type().getName(); //这级分区的方式，是hash还是interval ?
                        l1_part_num = pinfo.getP_num();   //分区有多少个，如果是hash的话n个分区，那么特征值就是0,1,2 。。。 n-1

                        System.out.println("part level is: " + pinfo.getP_level());
                        System.out.println("part cols is: " + split_name_l1);
                        System.out.println("partition type is: " + part_type_l1);
                        System.out.println("partition num is: " + l1_part_num);
                        if ("interval".equalsIgnoreCase(part_type_l1)) {
                            if (pis.get(0).getArgs().size() < 2) {
                                throw new RuntimeException("get the table's partition unit and interval error");
                            } else {
                                //Y year,M mponth,W week,D day,H hour，M minute。 现在只支持H D W， 因为月和年的时间并不是完全确定的，因此无法进行精确的分区，暂时不支持；分钟级的单位太小，暂时也不支持
                                List<String> paras = pinfo.getArgs();
                                String unit = paras.get(0);
                                String interval = paras.get(1);
                                long partitionInverval = 0l;
                                if ("'MI'".equalsIgnoreCase(unit)) {   //以分钟为单位
                                    partitionInverval = Long.parseLong(interval) * 60;
                                } else if ("'H'".equalsIgnoreCase(unit)) {    //以小时为单位
                                    partitionInverval = Long.parseLong(interval) * 60 * 60;
                                } else if ("'D'".equalsIgnoreCase(unit)) { //以天为单位
                                    partitionInverval = Long.parseLong(interval) * 60 * 60 * 24;
                                } else {
                                    throw new RuntimeException("now the partition unit is not support, it only supports --- D day,H hour,MI minute");
                                }
                            }
                            System.out.println("partition unit is: " + pinfo.getArgs().get(0));
                            System.out.println("partition vals is: " + pinfo.getArgs().get(1));
                        }
                    }
                }
                System.out.println("\n\n\n");
                for (PartitionFactory.PartitionInfo pinfo : pis) {
                    if (pinfo.getP_level() == 2) {
                        split_name_l2 = pinfo.getP_col();
                        part_type_l2 = pinfo.getP_type().getName();
                        l2_part_num = pinfo.getP_num();
                        System.out.println("part level is: " + pinfo.getP_level());
                        System.out.println("partition type is: " + part_type_l2);
                        System.out.println("part cols is: " + split_name_l2);
                        System.out.println("partition num is: " + l2_part_num);
                    }
                }
            }
        } catch (MetaException ex) {
            log.error(ex, ex);
        } catch (TException ex) {
            log.error(ex, ex);
        }
    }
}

/*
 *
 Table(tableName:wb, dbName:db1, schemaName:wb, owner:root, createTime:1381380018, lastAccessTime:0, retention:0, sd:StorageDescriptor(cols:[FieldSchema(name:id, type:int, comment:null, version:0), FieldSchema(name:user_id, type:string, comment:null, version:0), FieldSchema(name:rootuser_id, type:string, comment:null, version:0), FieldSchema(name:mid, type:string, comment:null, version:0), FieldSchema(name:rootid, type:string, comment:null, version:0), FieldSchema(name:rel_time, type:timestamp, comment:@time, version:0), FieldSchema(name:intput_time, type:string, comment:null, version:0), FieldSchema(name:send_ip, type:string, comment:@ip, version:0), FieldSchema(name:send_srcport, type:int, comment:null, version:0), FieldSchema(name:province_name, type:string, comment:null, version:0), FieldSchema(name:city_name, type:string, comment:null, version:0), FieldSchema(name:isp_name, type:string, comment:null, version:0), FieldSchema(name:net_type, type:string, comment:null, version:0), FieldSchema(name:client_type, type:int, comment:null, version:0), FieldSchema(name:mobile_type, type:int, comment:null, version:0), FieldSchema(name:message_type, type:int, comment:null, version:0), FieldSchema(name:audit_status, type:int, comment:null, version:0), FieldSchema(name:user_fansnum, type:int, comment:null, version:0), FieldSchema(name:user_friendsnum, type:int, comment:null, version:0), FieldSchema(name:mid_commentnum, type:int, comment:null, version:0), FieldSchema(name:mid_retweetnum, type:int, comment:null, version:0), FieldSchema(name:rootid_commentnum, type:int, comment:null, version:0), FieldSchema(name:rootid_retweetnum, type:int, comment:null, version:0), FieldSchema(name:client_remark, type:string, comment:null, version:0), FieldSchema(name:m_text, type:string, comment:@text, version:0), FieldSchema(name:is_long, type:int, comment:null, version:0), FieldSchema(name:pic_url, type:string, comment:null, version:0), FieldSchema(name:pic_content, type:string, comment:null, version:0), FieldSchema(name:audio_url, type:string, comment:null, version:0), FieldSchema(name:audio_content, type:string, comment:null, version:0), FieldSchema(name:video_url, type:string, comment:null, version:0), FieldSchema(name:video_content, type:string, comment:null, version:0), FieldSchema(name:sp_type, type:int, comment:null, version:0)], location:hdfs://192.168.1.13:54310/user/hive/warehouse/db1.db/wb, inputFormat:org.apache.hadoop.mapred.SequenceFileInputFormat, outputFormat:org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat, compressed:false, numBuckets:-1, serdeInfo:SerDeInfo(name:null, serializationLib:org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe, parameters:{serialization.format=1}), bucketCols:[], sortCols:[], parameters:{}, skewedInfo:SkewedInfo(skewedColNames:[], skewedColValues:[], skewedColValueLocationMaps:{}), storedAsSubDirectories:false), partitionKeys:[], parameters:{transient_lastDdlTime=1381380018}, viewOriginalText:null, viewExpandedText:null, tableType:MANAGED_TABLE, fileSplitKeys:[FieldSchema(name:rel_time, type:interval, comment:{"p_col":"rel_time","p_level":1,"p_order":0,"p_num":0,"p_version":0,"p_type":"interval","args":["'MI'","3"]}, version:0), FieldSchema(name:isp_name, type:hash, comment:{"p_col":"isp_name","p_level":2,"p_order":0,"p_num":4,"p_version":0,"p_type":"hash","args":["4"]}, version:0)])
 [FieldSchema(name:rel_time, type:interval, comment:{"p_col":"rel_time","p_level":1,"p_order":0,"p_num":0,"p_version":0,"p_type":"interval","args":["'MI'","3"]}, version:0), FieldSchema(name:isp_name, type:hash, comment:{"p_col":"isp_name","p_level":2,"p_order":0,"p_num":4,"p_version":0,"p_type":"hash","args":["4"]}, version:0)]
 part level is: 1
 part cols is: rel_time
 partition type is: interval
 partition num is: 0
 partition unit is: 'MI'
 partition vals is: 3


 part level is: 2
 partition type is: hash
 part cols is: isp_name
 partition num is: 4
 *
 */
