package cn.ac.iie.ulss.indexer.metastore;

import cn.ac.iie.ulss.dao.SimpleDaoImpl;
import cn.ac.iie.ulss.indexer.runenvs.GlobalParas;
import cn.ac.iie.ulss.indexer.worker.Indexer;
import java.util.Date;
import java.util.List;
import org.apache.log4j.Logger;

public class DBMetastore {

    public static Logger log = Logger.getLogger(DBMetastore.class.getName());
    private SimpleDaoImpl simpleDao;

    public DBMetastore() {
        simpleDao = SimpleDaoImpl.getDaoInstance();
    }

    //@TODO check whether the  MtPartitionIndex is updated in realtime and cascade update.
    /**
     * time_records format: start_time(seconds):hours:houroffset,min_s,min_e;...
     *
     * @param start_time_real
     * @param duration_real
     * @param records
     * @return
     */
    public static String getTimeIndexData(Date start_time_real, long duration_real, String records) {
        StringBuilder sb = new StringBuilder();
        sb.append(start_time_real.getTime() / 1000).append(":");
        sb.append(duration_real).append(":");
        sb.append(records);
        return sb.toString();
    }

    public List<List<String>> getAllSchema() {
        String sql = "select dataschema.schema_name,dataschema.schema_content from dataschema";
        //String sql = "select dataschema.schema_name,dataschema.schema_content,dataschema_mq.mq from dataschema left outer join dataschema_mq on dataschema.schema_name=dataschema_mq.schema_name";
        List<List<String>> resL = null;
        while (true) {
            try {
                log.info("get all schema is " + sql);
                resL = simpleDao.queryForList(sql);
                if (resL != null && resL.size() > 0) {
                    return resL;
                }
                Thread.sleep(3000);
            } catch (Exception ex) {
                log.error(ex, ex);
            }
        }
    }

    public List<List<String>> getSchema2table() {
        String sql = "select MQ_TABLE.table_name,DATASCHEMA_MQ.SCHEMA_NAME  from MQ_TABLE  left outer join DATASCHEMA_MQ   on DATASCHEMA_MQ.mq=MQ_TABLE.mq";
        List<List<String>> resL = null;
        while (true) {
            try {
                log.info("get schema is " + sql);
                resL = simpleDao.queryForList(sql);
                if (resL != null && resL.size() > 0) {
                    return resL;
                }
                Thread.sleep(3000);
            } catch (Exception ex) {
                log.error(ex, ex);
            }
        }
    }

    public String getDocSchema() {
        String sql = "select dataschema.schema_name, dataschema.schema_content from dataschema where schema_name=" + "'" + GlobalParas.docs + "'";
        List<List<String>> resL = null;
        while (true) {
            try {
                log.info("get docs schema is " + sql);
                resL = simpleDao.queryForList(sql);
                if (resL != null && resL.size() > 0) {
                    return resL.get(0).get(1);
                }
                Thread.sleep(3000);
            } catch (Exception ex) {
                log.error(ex, ex);
            }
        }
    }

    public String getStatVolumeSchema() {
        String sql = "select dataschema.schema_name, dataschema.schema_content from dataschema where schema_name=" + "'data_volume_stat'";
        List<List<String>> resL = null;
        while (true) {
            try {
                log.info("get data_volume_stat schema is " + sql);
                resL = simpleDao.queryForList(sql);
                if (resL != null && resL.size() > 0) {
                    return resL.get(0).get(1);
                }
                Thread.sleep(3000);
            } catch (Exception ex) {
                log.error(ex, ex);
            }
        }
    }

    public static void main(String[] args) {
        DBMetastore imd = new DBMetastore();
        System.out.println(imd.getAllSchema());
    }
}