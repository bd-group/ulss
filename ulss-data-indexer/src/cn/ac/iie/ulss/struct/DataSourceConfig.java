/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.struct;

/**
 *
 * @author liucuili
 */
public class DataSourceConfig {

    String zkUrl;
    String dbName;
    String tbName;
    String timeLable;
    //String topic;
    //String[] metaqParts;

    public DataSourceConfig() {
    }

    public DataSourceConfig(String zk, String db, String tb, String tl, String tp, String[] mps) {
        this.zkUrl = zk;
        this.dbName = db;
        this.tbName = tb;
        this.timeLable = tl;
        //this.topic = tp;
        //this.metaqParts = mps;
    }

    public DataSourceConfig(String zk, String db, String tb, String tl) {
        this.zkUrl = zk;
        this.dbName = db;
        this.tbName = tb;
        this.timeLable = tl;
    }

    public String getZkUrl() {
        return zkUrl;
    }

    public void setZkUrl(String zkUrl) {
        this.zkUrl = zkUrl;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getTbName() {
        return tbName;
    }

    public void setTbName(String tbName) {
        this.tbName = tbName;
    }

    public String getTimeLable() {
        return timeLable;
    }

    public void setTimeLable(String timeLable) {
        this.timeLable = timeLable;
    }

//    public String getTopic() {
//        return topic;
//    }
//
//    public void setTopic(String topic) {
//        this.topic = topic;
//    }
//
//    public String[] getMetaqParts() {
//        return metaqParts;
//    }
//
//    public void setMetaqParts(String[] metaqParts) {
//        this.metaqParts = metaqParts;
//    }
}
