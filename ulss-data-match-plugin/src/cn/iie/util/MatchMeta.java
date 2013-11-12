package cn.iie.util;

import cn.iie.dao.*;
import java.util.List;
import org.apache.log4j.Logger;

public class MatchMeta {

    public static Logger log = Logger.getLogger(MatchMeta.class.getName());
    private SimpleDaoImpl simpleDao;

    public MatchMeta() {
        simpleDao = SimpleDaoImpl.getDaoInstance();
    }

    public List<List<String>> getSchema(String metaQName) {
        String sql = "select dataschema.schema_name as schema_name,dataschema.schema_content as schema_content from dataschema,dataschema_mq where dataschema_mq.mq='" + metaQName + "' and dataschema.schema_name=dataschema_mq.schema_name";
        log.info("get schema is " + sql);
        List<List<String>> resL = simpleDao.queryForList(sql);
        if (resL.size() <= 0) {
            return null;
        }
        return resL;
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

    public String getDocSchema() {
        String sql = "select dataschema.schema_name, dataschema.schema_content from   dataschema where schema_name='docs'";
        log.info("get schema is " + sql);
        List<List<String>> resL = simpleDao.queryForList(sql);
        if (resL.size() <= 0) {
            return null;
        }
        return resL.get(0).get(1);
    }

    public List getRules(String bustype) {
        String sql = "select rule_content,cols,mcols from datamatch_rules where state='1' and bustype='" + bustype + "'";
        log.info("get match rule is " + sql);
        List<String> resL = simpleDao.queryForList(sql);
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

    public static void main(String[] args) {
        MatchMeta imd = new MatchMeta();
        System.out.println(imd.getDocSchema());
    }
}
