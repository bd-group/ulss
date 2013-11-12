package cn.ac.iie.ulss.metastore;

import cn.ac.iie.ulss.dao.SimpleDaoImpl;
import cn.ac.iie.ulss.indexer.Indexer;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Logger;
import cn.ac.iie.ulss.util.Constants;
import java.sql.ResultSet;
import java.util.Date;
import java.util.HashSet;
import org.apache.avro.Protocol;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;

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

    public void updateMetaqOffset(String dbName, String tbName, String partName, long offset, String timeLable) {
        long id = simpleDao.getNextMetaqID();
        String sql = "insert into METAQ_OFFSET_RECORD"
                + "(ID,DATABASE_NAME,TABLE_NAME,HOSTNAME,PARTITION_NAME,OFFSET,UPDATETIME,TIME_LABLE,IS_REDO) values("
                + id + ",'" + dbName + "','" + tbName + "','" + Indexer.hostName + "','" + partName + "'," + offset + ",sysdate,'" + timeLable + "'," + 0
                + ")";
        log.info(sql);
        simpleDao.excuteSQL(sql);
    }

    public long getMetaqOffset(String dbName, String tbName, String partName, String timeLable) {
        String sql = "select offset from  METAQ_OFFSET_RECORD where "
                + "DATABASE_NAME='" + dbName + "' and TABLE_NAME='" + tbName
                + "' and PARTITION_NAME='" + partName + "' order by id desc";
        log.info(sql);
        List<List<Long>> resL = simpleDao.queryForList(sql);
        if (resL.size() <= 0) {
            return 0;
        }
        return resL.get(0).get(0);
    }

    public List<List<String>> getAllSchema() {
        String sql = "select dataschema.schema_name,dataschema.schema_content,dataschema_mq.mq from dataschema left outer join dataschema_mq on dataschema.schema_name=dataschema_mq.schema_name";
        log.info("get all schema is " + sql);
        List<List<String>> resL = simpleDao.queryForList(sql);
        if (resL.size() <= 0) {
            return null;
        }
        return resL;
    }

    public List<List<String>> getSchema2table() {
        String sql = "select schema_name,table_name from DATASCHEMA_TABLE";
        log.info("get schema is " + sql);
        List<List<String>> resL = simpleDao.queryForList(sql);
        if (resL.size() <= 0) {
            return null;
        }
        return resL;
    }

    public String getDocSchema() {
        String sql = "select dataschema.schema_name, dataschema.schema_content from   dataschema where schema_name=" + "'" + Indexer.docs_schema_name + "'";
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
