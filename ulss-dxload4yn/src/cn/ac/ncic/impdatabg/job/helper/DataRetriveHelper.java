/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.ncic.impdatabg.job.helper;

/**
 *
 * @author oracle
 */
public class DataRetriveHelper {
    
    int dataFieldNum = -1;
    int dxnrIdx = -1; //start from 1

    public DataRetriveHelper(int _dataFieldNum,int _dxnrIdx) {
        this.dataFieldNum = _dataFieldNum;
        this.dxnrIdx = _dxnrIdx;
    }
    
    //get all fields
    public String[] getField(String _content) throws Exception {

        String[] target = new String[dataFieldNum];
        char[] ar = _content.toCharArray();
        int contentLength = _content.length();
        int headcount = 0;
        int tailcount = 0;
        int i = 0;
        int j = 0;

        for (i = 0; i
                < contentLength; i++) {
            if (ar[i] == '"') {
                i++;
                StringBuilder sb = new StringBuilder();
                while (ar[i] != '"') {
                    sb.append(ar[i]);
                    i++;
                }

                target[headcount] = (sb.toString());
                headcount++;

                if (headcount == dxnrIdx-1) {
                    break;
                }

            }
        }

        if (headcount < dxnrIdx-1) {
            throw new Exception();
        }

        for (j = contentLength - 1; j
                >= 0; j--) {
            //can skip $EOF$
            if (ar[j] == '"') {
                j--;
                StringBuilder sb = new StringBuilder();
                while (ar[j] != '"') {
                    sb.append(ar[j]);
                    j--;

                }

                tailcount++;
                target[dataFieldNum - tailcount] = (reverse(sb.toString()));
                if (tailcount == (dataFieldNum-dxnrIdx)) {
                    break;
                }

            }
        }

        if (tailcount < (dataFieldNum-dxnrIdx)) {
            throw new Exception();
        }

        target[dxnrIdx-1] = _content.substring(i + 3, j - 2);

        return target;
    }

    protected String reverse(String input) {
        int length = input.length();
        StringBuilder sb = new StringBuilder(length);
        for (int i = length - 1; i
                >= 0; i--) {
            sb.append(input.charAt(i));
        }
        return sb.toString();
    }
}
