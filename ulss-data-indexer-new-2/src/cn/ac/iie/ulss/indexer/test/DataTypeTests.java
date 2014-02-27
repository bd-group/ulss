/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.indexer.test;

import cn.ac.iie.ulss.indexer.constant.DataTypeEnum;

/**
 *
 * @author liucuili
 */
public class DataTypeTests {

    public void caseDataType(String type) {
        switch (DataTypeEnum.getDataType(type)) {
            case INT:
                System.out.println("this is a int");
                break;
            case LONG:
                System.out.println("this is a int");
                break;
            case ARRAYINT:
                System.out.println("this is a arrayint");
                break;
            default:
                System.out.println("this is a default");
                break;
        }
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        DataTypeTests tests = new DataTypeTests();
        tests.caseDataType("array<int>".replace("<", "").replace(">", ""));
    }
}
