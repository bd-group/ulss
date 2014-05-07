/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.dao;

import java.util.List;
import org.springframework.dao.DataAccessException;

/**
 *
 * @author fenglei
 */
public interface SimpleDao {

    public void excuteSQL(String sql);

    public RowResultSet queryForRowSet(String sql);

    public List queryForList(String sql) throws DataAccessException;

    public List queryForColumnList(String sql);

    public void batchExecuteSQL(String sql, final List<List> args) throws DataAccessException;

    public void insertDate(String sql, Object[] args);

    public Long getNextSeID(Boolean isDefer);

    public Long getDeferIndexerNum();

    public void completeDeferIndexer(Long num);

    public Long getNextDeployIndexID();
}
