package cn.ac.iie.ulss.util;

import cn.ac.iie.ulss.dao.SimpleDaoImpl;
import cn.ac.iie.ulss.match.worker.Matcher;
import java.util.List;
import java.util.logging.Level;
import org.apache.log4j.Logger;

public class MatchDBMeta {

    public static Logger log = Logger.getLogger(MatchDBMeta.class.getName());
    private SimpleDaoImpl simpleDao;

    public MatchDBMeta() {
        simpleDao = SimpleDaoImpl.getDaoInstance();
    }

    public List<List<String>> getAllSchema() {
        String sql = "select dataschema.schema_name,dataschema.schema_content from dataschema";
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

    public List<List<String>> getMq2Schema() {
        String sql = "select DATASCHEMA_MQ.mq,DATASCHEMA_MQ.schema_name,region from DATASCHEMA_MQ";
        List<List<String>> resL = null;
        while (true) {
            try {
                log.info("get mq2Schema is " + sql);
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
        String sql = "select dataschema.schema_name, dataschema.schema_content from dataschema where schema_name=" + "'" + Matcher.docsName + "'";

        List<List<String>> resL = null;
        while (true) {
            try {
                log.info("get schema is " + sql);
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

    public List<List<String>> getSchema(String metaQName) {
        String sql = "select dataschema.schema_name ,dataschema.schema_content  from dataschema,dataschema_mq where dataschema_mq.mq='" + metaQName + "' and dataschema.schema_name=dataschema_mq.schema_name";

        List<List<String>> resL = null;
        while (true) {
            log.info("get schema is " + sql);
            try {
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

    public List<List<String>> getSchema(String region, String schemaName) {
        String sql = "";
        if (region == null || "".equalsIgnoreCase(region)) {
            sql = "select dataschema.schema_name,dataschema.schema_content from dataschema,dataschema_mq where dataschema_mq.SCHEMA_NAME='" + schemaName + "' and region is null " + " and dataschema.schema_name=dataschema_mq.schema_name";
        } else {
            sql = "select dataschema.schema_name,dataschema.schema_content from dataschema,dataschema_mq where dataschema_mq.SCHEMA_NAME='" + schemaName + "' and region='" + region + "' and dataschema.schema_name=dataschema_mq.schema_name";
        }

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

    public String getMq(String region, String schemaName) {
        String sql = "";
        if (region == null || "".equalsIgnoreCase(region)) {
            sql = "select DATASCHEMA_MQ.MQ from DATASCHEMA_MQ where dataschema_mq.SCHEMA_NAME='" + schemaName + "' and region is null ";
        } else {
            sql = "select DATASCHEMA_MQ.MQ from DATASCHEMA_MQ where dataschema_mq.SCHEMA_NAME='" + schemaName + "' and region='" + region + "'";
        }

        List<List<String>> resL = null;
        while (true) {
            log.info("get mq name is " + sql);
            try {
                resL = simpleDao.queryForList(sql);
                if (resL != null && resL.size() > 0) {
                    return resL.get(0).get(0);
                }
                Thread.sleep(3000);
            } catch (Exception ex) {
                log.error(ex, ex);
            }
        }
    }

    public List getRules(String bustype) {
        String sql = "select rule_content,cols,mcols from datamatch_rules where state='1' and bustype='" + bustype + "'";

        List<String> resL = null;
        while (true) {
            log.info("get match rule is " + sql);
            try {
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

    public static void main(String[] args) {
        MatchDBMeta imd = new MatchDBMeta();
        //System.out.println("" + "fsdf");
        //System.out.println(imd.getAllSchema());
        //System.out.println(imd.getSchema("", "t_dx_rz_ccdx"));
        System.out.println(imd.getSchema("sichuan", "dx_cdr"));
        System.out.println(imd.getMq("nc", "t_ybrz"));
    }
}
