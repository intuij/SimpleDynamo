package edu.buffalo.cse.cse486586.simpledynamo;

import java.util.TreeSet;

/**
 * Created by weiyijiang on 4/15/17.
 */

public class DynamoNode {
    // id of each AVD, eg. "5554"
    private String emuPort;
    // Hash value of each id
    private String hashCode;
    // Successors of this node
    private TreeSet<DynamoNode> succ;

    public DynamoNode(String emuPort, String hashCode) {
        this.emuPort = emuPort;
        this.hashCode = hashCode;
    }

    public String getEmuPort() {
        return emuPort;
    }

    public void setEmuPort(String emuPort) {
        this.emuPort = emuPort;
    }

    public String getHashCode() {
        return hashCode;
    }

    public void setHashCode(String hashCode) {
        this.hashCode = hashCode;
    }

    public TreeSet<DynamoNode> getSucc() {
        return succ;
    }

    public void setSucc(TreeSet<DynamoNode> succ) {
        this.succ = succ;
    }
}
