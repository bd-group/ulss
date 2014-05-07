/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.struct;

import java.util.ArrayList;
import java.util.List;

/**
 * 二维列表
 *
 * @author lxh
 */
public class D2ListHolder {

    //二维List,第一维即可以表示行List,也可以表示列List;第二维是Object元素。
    private List<List> d2list = new ArrayList<List>();

    //================== 集合操作 =====================//
    /**
     * 清空整个List
     */
    public void clearAll() {
        d2list.clear();
    }

    /**
     * 保留所有List，清空List中的元素。
     */
    protected void clearElements() {
        for (int i = 0; i < d2list.size(); i++) {
            List list = d2list.get(i);
            list.clear();
        }
    }

    /**
     * 如果二维列表没有size个List,则增加到指定个数的List
     *
     * @param size
     */
    protected void reserve(int size) {
        int len = d2list.size();
        for (int i = len; i < size; i++) {
            List list = new ArrayList<Object>();
            d2list.add(list);
        }
    }

    /**
     * 得到整个二维List
     *
     * @return D2ListHolder所表示的二维List
     */
    protected List<List> getD2List() {
        return d2list;
    }

    protected void setD2List(List<List> lists) {
        d2list = lists;
    }

    //================== List操作 =====================//
    /**
     * 在尾部添加一个List元素，并将该List返回
     *
     * @return 新增的List
     */
    protected List addList() {
        List list = new ArrayList<Object>();
        d2list.add(list);
        return d2list.get(d2list.size() - 1);
    }

    /**
     * 在列表的尾部添加指定元素（在其索引中加 1）。
     *
     * @param list
     */
    protected void addList(List list) {
        d2list.add(list);
    }

    /**
     * 在列表的指定位置插入指定元素。将当前处于该位置的元素（如果有的话）和所有后续元素向右移动（在其索引中加 1）。
     *
     * @param index
     * @param list
     */
    protected void addList(int index, List list) {
        d2list.add(index, list);
    }

    protected void setList(int index, List list) {
        d2list.set(index, list);
    }

    protected List getList(int index) {
        return d2list.get(index);
    }

    //================== 元素操作 =====================//
    /**
     * 在第indexOfList个List的尾部添加元素
     *
     * @param indexOfList，List的索引，从0开始计数
     * @param obj
     */
    protected void addElement(int indexOfList, Object obj) {
        if (d2list.size() <= indexOfList) {
            reserve(indexOfList + 1);
        }
        List list = d2list.get(indexOfList);
        list.add(obj);
    }

    /**
     * 修改第idxOfList个List的第idxOfEelement个元素的值。
     *
     * @param idxOfList，List的索引，从0开始计数
     * @param idxOfEelement，指定List的指定元素索引，从0开始计数
     * @param obj 待更新的值
     */
    protected void setElement(int idxOfList, int idxOfEelement, Object obj) {
        List list = d2list.get(idxOfList);
        list.set(idxOfEelement, obj);
    }

    /**
     * 得到第idxOfList个List的第idxOfEelement个元素的值
     *
     * @param idxOfList，List的索引，从0开始计数
     * @param idxOfEelement，指定List的指定元素索引，从0开始计数
     * @return
     */
    protected Object getElement(int idxOfList, int idxOfEelement) {
        List list = d2list.get(idxOfList);
        return list.get(idxOfEelement);
    }

    //================== 元数据操作 =====================//
    /**
     * 得到集合中的List个数
     *
     * @return 集合中的List个数
     */
    protected int getListNum() {
        return d2list.size();
    }

    /**
     * 得到第一个List的元素个数。
     *
     * @return 返回第一个List的元素个数
     */
    protected int getSizeOfFirstList() {
        int size = 0;
        if (d2list.size() > 0) {
            List list = d2list.get(0);
            size = list.size();
        }
        return size;
    }
}
