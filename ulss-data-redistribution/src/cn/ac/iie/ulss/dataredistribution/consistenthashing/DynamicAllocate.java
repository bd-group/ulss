package cn.ac.iie.ulss.dataredistribution.consistenthashing;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author evan yang
 * date
 */
public class DynamicAllocate {

    public List<RNode> nodes = new ArrayList<RNode>();
    public MD5NodeLocator MD5Nodelocator = null;
    public HashCodeNodeLocator hashCodeNodelocator = null;
    public int nodeCopies = 160;

    public void setNodes(List<RNode> aurl) {
        if (!aurl.isEmpty()) {
            for (RNode node : aurl) {
                nodes.add(node);
            }
        }
    }

    public MD5NodeLocator getMD5NodeLocator() {
        MD5Nodelocator = new MD5NodeLocator(nodes, MD5HashAlgorithm.KETAMA_HASH, nodeCopies);
        return MD5Nodelocator;
    }

    public HashCodeNodeLocator getHashCodeNodeLocator() {
        hashCodeNodelocator = new HashCodeNodeLocator(nodes);
        return hashCodeNodelocator;
    }
}
