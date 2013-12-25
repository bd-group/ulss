/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.dataredistribution.consistenthashing;

import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 *
 * @author evan yang
 */
public final class MD5NodeLocator implements NodeLocator{

    private TreeMap<Long, RNode> ketamaNodes;
    private MD5HashAlgorithm hashAlg;
    private int numReps = 160;
    private List<RNode> nodes = null;
    public MD5NodeLocator(List<RNode> nodes, MD5HashAlgorithm alg, int nodeCopies) {
        this.nodes = nodes;
        hashAlg = alg;
        ketamaNodes = new TreeMap<Long, RNode>();

        if (nodeCopies > 0) {
            numReps = nodeCopies;
        }

        for (RNode node : nodes) {
            for (int i = 0; i < numReps / 4; i++) {
                byte[] digest = hashAlg.computeMd5(node.getName() + i);
                for (int h = 0; h < 4; h++) {
                    long m = hashAlg.hash(digest, h);
                    ketamaNodes.put(m, node);
                }
            }
        }
    }

    @Override
    public RNode getPrimary(final String k) {
        byte[] digest = hashAlg.computeMd5(k);
        RNode rv = getNodeForKey(hashAlg.hash(digest, 0));
        return rv;
    }

    RNode getNodeForKey(long hash) {
        final RNode rv;
        Long key = hash;
        if (!ketamaNodes.containsKey(key)) {
            SortedMap<Long, RNode> tailMap = ketamaNodes.tailMap(key);
            if (tailMap.isEmpty()) {
                key = ketamaNodes.firstKey();
            } else {
                key = tailMap.firstKey();
            }
        }

        rv = ketamaNodes.get(key);
        return rv;
    }
    
    public int getNodesNum(){
        return nodes.size();
    }
}