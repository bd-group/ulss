/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.dataredistribution.consistenthashing;

/**
 *
 * @author evan
 */
public interface NodeLocator {
    public RNode getPrimary(final String k);
    public int getNodesNum();
}
