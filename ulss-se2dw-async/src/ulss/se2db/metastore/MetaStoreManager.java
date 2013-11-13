/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ulss.se2db.metastore;

import cn.ac.ict.ncic.util.dao.DaoPool;
import cn.ac.ict.ncic.util.dao.util.ClusterInfoOP;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import ulss.se2db.config.Configuration;
import ulss.se2db.handler.TableSe2DBHandler;

/**
 *
 * @author ulss
 */
public class MetaStoreManager {

    private static MetaStoreManager metaStoreManager = null;
    private MetaStoreStub metaStore = null;
    private Map<String, TableSe2DBRule> tableSe2DBRuleSet = new HashMap<String, TableSe2DBRule>();
    private Map<String, TableSe2DBHandler> tableChangeHandler = new HashMap<String, TableSe2DBHandler>();
    Configuration conf = null;
    public static final String METASTORE_DB_NAME = "metaStoreDB";
    private static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(MetaStoreManager.class.getName());
    }

    private MetaStoreManager(Configuration pConf) {
        conf = pConf;
    }

    public static MetaStoreManager getMetaStoreManger(Configuration pConf) {

        if (metaStoreManager == null) {
            try {
                metaStoreManager = new MetaStoreManager(pConf);
                metaStoreManager.init();
            } catch (Exception ex) {
                metaStoreManager = null;
                logger.error("initializing metaStoreManager is failed for " + ex, ex);
            }
        }
        return metaStoreManager;
    }

    private void init() throws Exception {
        //to-do:init metastore client firstly
        //fixme
//        initializeMetaStoreThriftClient();
        initializeMetaStoreDBDao();
    }

    private void initializeMetaStoreDBDao() throws Exception {
        String metaStoreDBConnectString = conf.getString("metaStoreDB", "");
        if (metaStoreDBConnectString.isEmpty()) {
            throw new Exception("parameter metaStoreDB does not exist or is not defined");
        }

        try {
            DaoPool.putDao(ClusterInfoOP.getDBClusters(metaStoreDBConnectString));//need check
        } catch (Exception ex) {
            throw new Exception("initializing metastore db client is failed for " + ex.getMessage(), ex);
        }

        syncTableSe2DBRuleSet();
    }

    private void initializeMetaStoreThriftClient() throws Exception {
        String metaStoreThriftServerConnectString = conf.getString("metaStoreThriftServer", "");
        if (metaStoreThriftServerConnectString.isEmpty()) {
            throw new Exception("parameter metaStoreThriftServer does not exist or is not defined");
        }

        try {
            metaStore = MetaStoreStub.getMetaStoreStub(metaStoreThriftServerConnectString);
        } catch (Exception ex) {
            throw new Exception("initializing metastore thrift server client is failed for " + ex.getMessage(), ex);
        }
    }

    private Map<String, TableSe2DBRule> syncTableSe2DBRuleSet() {

        Map<String, TableSe2DBRule> newestTableSe2DBRuleSet = new HashMap<String, TableSe2DBRule>();
        int tryConnectTimes = 0;
        while (tryConnectTimes++ < 3) {

            ResultSet rs = null;
            try {
                rs = DaoPool.getDao(METASTORE_DB_NAME).executeQuery("select rule_name,rule_content from data_dispose_rules where rule_type=3");
                while (rs.next()) {
                    String ruleName = rs.getString("rule_name").trim();
                    String ruleContent = rs.getString("rule_content").trim();
                    logger.info("tp rule name:" + ruleName + ";rule content:" + ruleContent);
                    String[] ruleContentItems = ruleContent.split("\\|");
                    //mqName|dbType|connStrL|userName|passwd|tableName|columnSetStr
                    String mqName = ruleContentItems[0];
                    String dbType = ruleContentItems[1];
                    String connStr = ruleContentItems[2];
                    String userName = ruleContentItems[3];
                    String passwd = ruleContentItems[4];
                    String tableName = ruleContentItems[5];
                    String columnSetStr = ruleContentItems[6].toLowerCase();
                    TableSe2DBRule tableSe2DBRule = new TableSe2DBRule(ruleName, mqName, dbType, connStr, userName, passwd, tableName);
                    tableSe2DBRule.parseColumSet(columnSetStr);
                    newestTableSe2DBRuleSet.put(ruleName, tableSe2DBRule);
                }
                break;
            } catch (Exception ex) {
                logger.error("synchronizing se2db rule is failed for " + ex.getMessage(), ex);
                newestTableSe2DBRuleSet.clear();
                continue;
            } finally {
                Connection tmpConn = null;
                try {
                    tmpConn = rs.getStatement().getConnection();
                } catch (Exception ex) {
                }
                try {
                    rs.close();
                } catch (Exception ex) {
                }
                try {
                    tmpConn.close();
                } catch (Exception ex) {
                }
            }
        }

        return newestTableSe2DBRuleSet;

    }

    public Map<String, List> getTableSe2DBRuleSet() {
        Map<String, TableSe2DBRule> newestTableSe2DBRuleSet = syncTableSe2DBRuleSet();
        List<TableSe2DBRule> newTableSe2DBRuleList = new ArrayList<TableSe2DBRule>();
        List<TableSe2DBRule> updatedTableSe2DBRuleList = new ArrayList<TableSe2DBRule>();
        List<TableSe2DBRule> deletedTableSe2DBRuleList = new ArrayList<TableSe2DBRule>();

        //get deleted rules
        Iterator existTableSe2DBRuleItor = tableSe2DBRuleSet.entrySet().iterator();
        while (existTableSe2DBRuleItor.hasNext()) {
            TableSe2DBRule existTableSe2DBRule = ((Entry<String, TableSe2DBRule>) existTableSe2DBRuleItor.next()).getValue();
            if (!newestTableSe2DBRuleSet.containsKey(existTableSe2DBRule.ruleName)) {
                existTableSe2DBRuleItor.remove();
                deletedTableSe2DBRuleList.add(existTableSe2DBRule);
            }
        }

        //get new rules and updated rules
        Iterator newestTableSe2DBRuleItor = newestTableSe2DBRuleSet.entrySet().iterator();
        while (newestTableSe2DBRuleItor.hasNext()) {
            TableSe2DBRule newestTableSe2DBRule = ((Entry<String, TableSe2DBRule>) newestTableSe2DBRuleItor.next()).getValue();
            TableSe2DBRule tmpTableSe2DBRule = tableSe2DBRuleSet.get(newestTableSe2DBRule.ruleName);
            if (tmpTableSe2DBRule == null) {
                tableSe2DBRuleSet.put(newestTableSe2DBRule.ruleName, newestTableSe2DBRule);
                newTableSe2DBRuleList.add(newestTableSe2DBRule);
            } else {
                if (!newestTableSe2DBRule.mqName.equals(tmpTableSe2DBRule.mqName)) {
                    tableSe2DBRuleSet.put(newestTableSe2DBRule.ruleName, newestTableSe2DBRule);
                    updatedTableSe2DBRuleList.add(newestTableSe2DBRule);
                }
            }
        }

        Map<String, List> tablePersistanceRuleOPSet = new HashMap<String, List>();
        tablePersistanceRuleOPSet.put("new", newTableSe2DBRuleList);
        tablePersistanceRuleOPSet.put("updated", updatedTableSe2DBRuleList);
        tablePersistanceRuleOPSet.put("deleted", deletedTableSe2DBRuleList);
        return tablePersistanceRuleOPSet;
    }

    public TableSe2DBRule getTableSe2DBRule(String pTableName) {
        return tableSe2DBRuleSet.get(pTableName);
    }

    public void registerHandler(TableSe2DBHandler pTableSe2DBHandler) {
        tableChangeHandler.put(pTableSe2DBHandler.getRuleName(), pTableSe2DBHandler);
    }

    public void removeHandler(TableSe2DBHandler pTableSe2DBHandler) {
        tableChangeHandler.remove(pTableSe2DBHandler.getRuleName());
    }

    private void act2TableChange() {
    }
}
