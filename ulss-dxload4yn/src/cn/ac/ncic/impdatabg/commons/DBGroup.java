/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.ncic.impdatabg.commons;

/**
 *
 * @author oracle
 */
public class DBGroup {

    String url;
    String user;
    String passwd;

    public DBGroup(String purl, String  puser, String ppasswd) {
        this.url = purl;
        this.user = puser;
        this.passwd = ppasswd;
    }

    public String getInfo(){
        return this.url+":"+this.user+":"+this.passwd;
    }

    public String getPasswd() {
        return passwd;
    }

    public String getUrl() {
        return url;
    }

    public String getUser() {
        return user;
    }

}
