/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package test;

import cn.ac.iie.ulss.dataredistribution.commons.GlobalVariables;
import cn.ac.iie.ulss.dataredistribution.commons.RuntimeEnv;
import cn.ac.iie.ulss.dataredistribution.tools.MetaStoreClientPool;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FileOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.SFile;
import org.apache.thrift.TException;

/**
 *
 * @author evan
 */
public class getFileFromID {

    public static void main(String[] args) throws FileOperationException, MetaException, TException {
        HiveConf hc = new HiveConf();
        hc.set("hive.metastore.uris", "thrift://10.228.69.5:10101");
        MetaStoreClientPool newmscp = new MetaStoreClientPool(1, hc);
        RuntimeEnv.addParam(GlobalVariables.METASTORE_CLIENT_POOL, newmscp);
        MetaStoreClientPool mscp = newmscp;
        MetaStoreClientPool.MetaStoreClient cli = mscp.getClient();
        IMetaStoreClient icli = cli.getHiveClient();
        SFile sf = icli.get_file_by_id(408355);
        sf = icli.get_file_by_id(410676);
        System.out.println(sf);
    }
}
