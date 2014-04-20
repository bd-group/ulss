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

    String dbName;
    String tbName;
    String timeLable;

    public DataSourceConfig() {
    }

    public DataSourceConfig(String db, String tb, String tl) {
        this.dbName = db;
        this.tbName = tb;
        this.timeLable = tl;
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
}
