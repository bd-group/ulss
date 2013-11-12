/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.dao;

import cn.ac.iie.ulss.dao.RsMetaData;
import java.util.List;

import cn.ac.iie.ulss.struct.RowSetHolder;

/**
 *
 * @author lxh
 */
public class RowResultSet {

    private RsMetaData metaData;
    private RowSetHolder rowSet;

    public RowResultSet() {
    }

    public RowResultSet(int colNum) {
        metaData = new RsMetaData(colNum);
    }

    /**
     * 直接获得所有行的List
     *
     * @return
     */
    public List getAllRowList() {
        return rowSet.getAllRows();
    }

    public RowSetHolder getRowSet() {
        return rowSet;
    }

    public void setRowSet(RowSetHolder rowSet) {
        this.rowSet = rowSet;
    }

    public RsMetaData getMetaData() {
        return metaData;
    }

    public void setMetaData(RsMetaData metaData) {
        this.metaData = metaData;
    }
}
