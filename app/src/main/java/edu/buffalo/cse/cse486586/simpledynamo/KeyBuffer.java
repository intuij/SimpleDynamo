package edu.buffalo.cse.cse486586.simpledynamo;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by weiyijiang on 4/19/17.
 */

// Data class, store all buffered k-v pairs for a failure node
public class KeyBuffer {
    // Buffered for which node
    private String nodeId;
    // Each entry is "KEY,"VALUE"
    private List<String> pairs;

    public KeyBuffer() {
        this.pairs = new ArrayList<String>();
        this.nodeId = "";
    }

    public void addPair(String key, String value) {
        String s = key + "," + value;
        this.pairs.add(s);
    }

    public void removePair(String key) {
        for (String str : this.pairs) {
            String[] ss = str.split(",");
            if (key.equals(ss[0])) {
                this.pairs.remove(str);
                return;
            }
        }
    }

    public void clean() {
        this.nodeId = "";
        this.pairs.clear();
    }

    public void setNodeId(String failureNode) {
        this.nodeId = failureNode;
    }

    public String getNodeId() {
        return nodeId;
    }

    public List<String> getPairs() {
        return pairs;
    }
}
