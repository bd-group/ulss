/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.dataredistribution.consistenthashing;

/**
 *
 * @author evan yang
 */
public class RNode {

    private String name = null;
    private int hashNum = 0;
    private String keywords = null;
    private String partType = null;
    private int type = -1;

    public RNode(String name) {
        this.name = name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }
    
    public void setHashNum(int hashNum) {
        this.hashNum = hashNum;
    }

    public int getHashNum() {
        return this.hashNum;
    }
    
    public void setKeywords(String keywords) {
        this.keywords = keywords;
    }

    public String getKeywords() {
        return this.keywords;
    }
    
    public void setPartType(String partType) {
        this.partType = partType;
    }

    public String getPartType() {
        return this.partType;
    }
    
    public void setType(int type) {
        this.type = type;
    }

    public int getType() {
        return this.type;
    }
}