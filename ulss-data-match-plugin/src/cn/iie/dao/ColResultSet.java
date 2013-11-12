/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.iie.dao;

import java.util.List;
import cn.iie.struct.ColumnSetHolder;

/**
 *
 * @author lxh
 */
public class ColResultSet {

    private RsMetaData metaData;
    private ColumnSetHolder colSet;

    public ColResultSet() {
    }

//    public ColResultSet(int colNum) {
//        metaData = new RsMetaData(colNum);
//    }
    /**
     * 直接获得所有行的List
     *
     * @return
     */
    public List getAllColList() {
        return colSet.getAllCols();
    }

    public ColumnSetHolder getColSet() {
        return colSet;
    }

    public void setColSet(ColumnSetHolder colSet) {
        this.colSet = colSet;
    }

    public RsMetaData getMetaData() {
        return metaData;
    }

    public void setMetaData(RsMetaData metaData) {
        this.metaData = metaData;
    }
}
