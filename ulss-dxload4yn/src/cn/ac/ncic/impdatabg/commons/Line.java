/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.ac.ncic.impdatabg.commons;

/**
 *
 * @author oracle
 */
public class Line {

    public String line;
    public long currchars;

    public Line(String line,long currchars)
    {
        this.line = line;
        this.currchars = currchars;
    }
}
