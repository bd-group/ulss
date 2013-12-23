package cn.ac.iie.ulss.dataredistribution.dao;

import java.util.ArrayList;     //非线程安全
import java.util.List;

public class ColumnSetHolder extends D2ListHolder {

    /**
     * 构造函数：用于初始化“记录集”容器。
     *
     * @return
     */
    public ColumnSetHolder() {
    }

    /**
     * 构造函数：初始化固定个数的列向量 ColumnSetHolder所特有
     *
     * @param num，列向量的个数
     */
    public ColumnSetHolder(int col_num) {
        this.reserve(col_num);
    }

    /**
     * 设置列个数，并初始化列向量。如果原来有数据，则将原数据销毁 ColumnSetHolder所特有
     */
    public void setColumnNum(int columns) {
        this.clearAll();
        this.reserve(columns);
    }

    /**
     * 保留所有列向量List，清空列向量中的数据 ColumnSetHolder所特有
     */
    public void clearColumnEelments() {
        this.clearElements();
    }

    public void setColSet(List<List> lists) {
        this.setD2List(lists);
    }

    /**
     * 得到所有列
     *
     * @return List<List<Object>> 所有记录
     */
    public List<List> getAllCols() {
        return this.getD2List();
    }

    //================== Column List操作 =====================//
    /**
     * 追加列向量,并将新增List返回
     *
     * @return 新增List
     */
    public List addNewColumn() {
        return this.addList();
    }

    /**
     * 向列表的尾部添加指定的元素
     *
     * @param col
     */
    public void addColumn(List col) {
        this.addList(col);
    }

    /**
     * 在列表的指定位置插入指定元素（可选操作）。将当前处于该位置的元素（如果有的话）和所有后续元素向右移动（在其索引中加 1）。
     * ColumnSetHolder所特有
     *
     * @param index
     * @param col
     */
    public void addColumn(int index, List col) {
        this.addList(index, col);
    }

    /**
     * 替换列向量
     *
     * @param index 列的位置索引，从0开始计数
     * @return
     */
    public void setColumnList(int index, List colAry) {
        this.setList(index, colAry);
    }

    /**
     * 得到列向量
     *
     * @param index 列的位置索引，从0开始计数
     * @return
     */
    public List getColumnList(int index) {
        return this.getList(index);
    }

    //================== 元素操作 =====================//
    /**
     * 在指定列的List尾部追加列值
     *
     * @param col_idx，列的位置索引，从0开始计数
     * @param obj
     */
    public void addObjectOfColumn(int col_idx, Object obj) {
        this.addElement(col_idx, obj);
    }

    /**
     * 将指定行，列的元素值替换为obj；调用时，应保证该位置原来有元素
     *
     * @param row 行索引，从0开始计数
     * @param col 列索引，从0开始计数
     * @return
     */
    public void setObject(int row, int col, Object obj) {
        this.setElement(col, row, obj);
    }

    /**
     * 得到指定行，列的元素值
     *
     * @param row 行索引，从0开始计数
     * @param col 列索引，从0开始计数
     * @return
     */
    public Object getObject(int row, int col) {
        return this.getElement(col, row);
    }

    //================== 转换为行 =====================//
    /**
     * 得到第row行的记录
     *
     * @param row 行的位置索引，从0开始计数
     * @return
     */
    public List getRow(int row) {
        List record = new ArrayList();
        for (int col = 0; col < getColCount(); col++) {
            record.add(this.getObject(row, col));
        }
        return record;
    }

    /**
     * 得到行集；此函数通过将列集做转置得到行集。 不建议如此使用，如果需要行集结果，可直接使用行集对象。
     *
     * @return 行集List
     */
    public List<List> getAllRows() {
        List<List> records = new ArrayList<List>();
        int rows = getRowCount();
        for (int i = 0; i < rows; i++) {
            records.add(getRow(i));
        }
        return records;
    }

    //================== 元数据操作 =====================//
    /**
     * 得到行数
     *
     * @return 行数
     */
    public int getRowCount() {
        return this.getSizeOfFirstList();
    }

    /**
     * 得到列数
     *
     * @return
     */
    public int getColCount() {
        return this.getListNum();
    }
}
