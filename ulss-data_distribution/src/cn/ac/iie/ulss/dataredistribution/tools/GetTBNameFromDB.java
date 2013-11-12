/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.dataredistribution.tools;

import cn.ac.iie.ulss.dataredistribution.commons.GlobalVariables;
import cn.ac.iie.ulss.dataredistribution.commons.RuntimeEnv;
import cn.ac.iie.ulss.dataredistribution.dao.SimpleDaoImpl;
import cn.ac.iie.ulss.dataredistribution.handler.GetRuleFromDBThread;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author evan
 */
public class GetTBNameFromDB {

    static Logger logger = null;
    private static SimpleDaoImpl simpleDao;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(GetRuleFromDBThread.class.getName());
    }

    /**
     *
     * get TBName from the oracle
     */
    public static void getTBNameFromDB() {
        Map<String, String> topicToTBName = (Map<String, String>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_TBNAME);
        Map<String, String> TBNameToTopic = (Map<String, String>) RuntimeEnv.getParam(GlobalVariables.TBNAME_TO_TOPIC);

        String dbCluster = (String) RuntimeEnv.getParam(RuntimeEnv.DB_CLUSTER);
        simpleDao = SimpleDaoImpl.getDaoInstance(dbCluster);
        logger.info("getting schema from metadb...");
        String sql = "select DATASCHEMA_MQ.MQ,DATASCHEMA_TABLE.TABLE_NAME from DATASCHEMA_MQ,DATASCHEMA_TABLE WHERE DATASCHEMA_MQ.SCHEMA_NAME=DATASCHEMA_TABLE.SCHEMA_NAME";
        List<List<String>> rs = simpleDao.queryForList(sql);
        for (List<String> r1 : rs) {
            topicToTBName.put(r1.get(0), r1.get(1));
            TBNameToTopic.put(r1.get(1), r1.get(0));
        }

        logger.info("get TBName from metadb successfully");
    }
}
