/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.statistics.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;

/**
 *
 * @author lxh
 */
public class RsMetaData {

    private int colNum = 0;
    private List<String> colNames;
    private Map<String, Integer> colNameToPos;
    private List<String> colTypeNames;
    private List<Integer> colTypes;
    static private Logger logger = Logger.getLogger(RsMetaData.class);

    public RsMetaData(int colNum) {
        setColNum(colNum);
    }

    public int getColNum() {
        return colNum;
    }

    public void setColNum(int colNum) {
        this.colNum = colNum;
        this.colNames = new ArrayList<String>(colNum);
        this.colTypes = new ArrayList<Integer>(colNum);
        this.colTypeNames = new ArrayList<String>(colNum);
        for (int i = 0; i < colNum; i++) {
            this.colNames.add("");
            this.colTypeNames.add("");
            this.colTypes.add(0);
        }
        colNameToPos = new HashMap<String, Integer>();
    }

    public List getColNames() {
        return colNames;
    }

    public void setColNames(List<String> colNames) {
        this.colNames = colNames;
        colNameToPos = new HashMap();
        for (int i = 0; i < this.colNames.size(); i++) {
            colNameToPos.put(this.colNames.get(i), i);
        }
    }

    // index start with 0.
    public String getColName(int index) {
        if (index < 0 || index >= this.colNames.size()) {

            logger.warn("Index Out of Bounds!");
            return "";
        }
        return this.colNames.get(index);
    }

    // index start with 0.
    public void setColName(String name, int index) {
        if (index < 0 || index >= this.colNames.size()) {

            logger.warn("Index Out of Bounds!");
            return;
        }

        this.colNames.set(index, name);

        colNameToPos.put(name, index);

    }

    public List getColTypes() {
        return colTypes;
    }

    public void setColTypes(List<Integer> colTypes) {
        this.colTypes = colTypes;
    }

    // index start with 0.
    public int getColType(int index) {
        if (index < 0 || index >= this.colTypes.size()) {

            logger.warn("Index Out of Bounds!");
            return 0;
        }
        return this.colTypes.get(index);

    }
    // index start with 0.

    public void setColType(int colType, int index) {
        if (index < 0 || index >= this.colTypes.size()) {

            logger.warn("Index Out of Bounds!");
            return;
        }
        this.colTypes.set(index, colType);
    }

    public List getColTypeNames() {
        return colTypeNames;
    }

    public void setColTypeNames(List<String> colTypesNames) {
        this.colTypeNames = colTypesNames;
    }

    // index start with 0.
    public String getColTypeName(int index) {
        if (index < 0 || index >= this.colTypeNames.size()) {

            logger.warn("Index Out of Bounds!");
            return "";
        }
        return this.colTypeNames.get(index);
    }
    // index start with 0.    

    public void setColTypeName(String colTypeName, int index) {
        if (index < 0 || index >= this.colTypeNames.size()) {
            logger.warn("Index Out of Bounds!");
            return;
        }
        this.colTypeNames.set(index, colTypeName);
    }

    public int getPosByName(String colName) {
        return this.colNameToPos.get(colName);
    }
}
