package cn.ac.iie.ulss.metastore;

import cn.ac.iie.ulss.dao.SimpleDaoImpl;
import cn.ac.iie.ulss.indexer.Indexer;
import java.util.Date;
import java.util.List;
import org.apache.log4j.Logger;

public class DBMeta {

    public static Logger log = Logger.getLogger(DBMeta.class.getName());
    private SimpleDaoImpl simpleDao;

    public DBMeta() {
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
        log.info("get all schema is " + sql);
        List<List<String>> resL = simpleDao.queryForList(sql);
        if (resL.size() <= 0) {
            return null;
        }
        return resL;
    }

    public List<List<String>> getSchema2table() {
        //String sql = "select schema_name,table_name from DATASCHEMA_TABLE";
        String sql = "select MQ_TABLE.table_name,DATASCHEMA_MQ.SCHEMA_NAME  from MQ_TABLE  left outer join DATASCHEMA_MQ   on DATASCHEMA_MQ.mq=MQ_TABLE.mq";
        log.info("get schema is " + sql);
        List<List<String>> resL = simpleDao.queryForList(sql);
        if (resL.size() <= 0) {
            return null;
        }
        return resL;
    }

    public String getDocSchema() {
        String sql = "select dataschema.schema_name, dataschema.schema_content from dataschema where schema_name=" + "'" + Indexer.docs_schema_name + "'";
        log.info("get schema is " + sql);
        List<List<String>> resL = simpleDao.queryForList(sql);
        if (resL.size() <= 0) {
            return null;
        }
        return resL.get(0).get(1);
    }

    public static void main(String[] args) {
        DBMeta imd = new DBMeta();
        System.out.println(imd.getDocSchema());
    }
}
