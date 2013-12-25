package cn.ac.iie.ulss.statistics.dao;

import java.util.ArrayList;
import java.util.List;

public class RowSetHolder extends D2ListHolder {

    /**
     * 构造函数：用于初始化“记录集”容器。
     *
     * @return
     */
    public RowSetHolder() {
    }

    public void setRowSet(List<List> lists) {
        this.setD2List(lists);
    }

    /**
     * 得到所有行
     *
     * @return List<List<Object>> 所有记录
     */
    public List<List> getAllRows() {
        return this.getD2List();
    }

    //================== Row List操作 =====================//
    /**
     * 创建一个空的新行。
     *
     * @param record
     * @return 空的新行，size=0
     */
    public List addNewRow() {
        return this.addList();
    }

    /**
     * 在行集尾部追加记录
     *
     * @param record
     */
    public void addRow(List record) {
        this.addList(record);
    }

    /**
     * 返回指定行的记录
     *
     * @param rowindex 行索引，从0开始计数
     * @return List<Object> 一条记录
     */
    public List getRow(int rowIndex) {
        return this.getList(rowIndex);
    }

    //================== 元素操作 =====================//
    /**
     * 在指定行的List尾部追加值
     *
     * @param row_idx，行的位置索引，从0开始计数
     * @param obj 待追加的值
     */
    public void addObjectOfRow(int row_idx, Object obj) {
        this.addElement(row_idx, obj);
    }

    /**
     * 将指定行，列的元素值替换为obj；调用时，应保证该位置原来有元素
     *
     * @param row 行索引，从0开始计数
     * @param col 列索引，从0开始计数
     * @return
     */
    public void setObject(int row, int col, Object obj) {
        this.setElement(row, col, obj);
    }

    /**
     * 得到指定行，列的元素值
     *
     * @param row 行索引，从0开始计数
     * @param col 列索引，从0开始计数
     * @return
     */
    public Object getObject(int row, int col) {
        return this.getElement(row, col);
    }

    //================== 转换为列 =======================//
    /**
     * 得到第col列的List
     *
     * @param col 列的位置索引，从0开始计数
     * @return
     */
    public List getColumn(int col) {
        List column = new ArrayList();
        for (int row = 0; row < getRowCount(); row++) {
            column.add(this.getObject(row, col));
        }
        return column;
    }

    /**
     * 得到行集；此函数通过将列集做转置得到行集。 不建议如此使用，如果需要行集结果，可直接使用行集对象。
     *
     * @return 行集List
     */
    public List<List> getAllCols() {
        List<List> columns = new ArrayList<List>();
        int cols = getColCount();
        for (int i = 0; i < cols; i++) {
            columns.add(getColumn(i));
        }
        return columns;
    }

    //================== 元数据操作 =====================//
    public int getRowCount() {
        return this.getListNum();
    }

    public int getColCount() {
        return this.getSizeOfFirstList();
    }
}
