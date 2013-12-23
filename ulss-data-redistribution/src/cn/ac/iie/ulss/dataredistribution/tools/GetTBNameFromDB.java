/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.dataredistribution.tools;

import cn.ac.iie.ulss.dataredistribution.commons.GlobalVariables;
import cn.ac.iie.ulss.dataredistribution.commons.RuntimeEnv;
import cn.ac.iie.ulss.dataredistribution.dao.SimpleDaoImpl;
import cn.ac.iie.ulss.dataredistribution.handler.GetRuleFromDB;
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
        logger = Logger.getLogger(GetRuleFromDB.class.getName());
    }

    /**
     *
     * get TBName from the oracle
     */
    public static void getTBNameFromDB() {
        Map<String, String> topicToTBName = (Map<String, String>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_TBNAME);
        Map<String, String> metaToTopic = (Map<String, String>) RuntimeEnv.getParam(GlobalVariables.META_TO_TOPIC);

        String dbCluster = (String) RuntimeEnv.getParam(RuntimeEnv.DB_CLUSTER);
        simpleDao = SimpleDaoImpl.getDaoInstance(dbCluster);
        logger.info("getting MQ to TABLE from oracledb...");
        String sql = "select MQ,TABLE_NAME from MQ_TABLE";
        List<List<String>> rs = simpleDao.queryForList(sql);
        for (List<String> r1 : rs) {
            topicToTBName.put(r1.get(0), r1.get(1));
            logger.info(r1.get(0) + " " + r1.get(1));
        }

        logger.info("get MQ to TABLE from oracledb successfully");
        
        logger.info("getting metaToTopic from oracledb...");
        String sql2 = "select dataschema_mq.region,dataschema_mq.mq,mq_table.table_name from dataschema_mq,mq_table where dataschema_mq.mq=mq_table.mq";
        List<List<String>> rs2 = simpleDao.queryForList(sql2);
        
        for (List<String> r1 : rs2) {
            metaToTopic.put(r1.get(0)+r1.get(2), r1.get(1));
            logger.info(r1.get(0)+r1.get(2) + " " + r1.get(1));
        }

        logger.info("get metaToTopic from oracledb successfully");
    }
}
