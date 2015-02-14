package it.unitn.ds;

/**
 * Represents different states the node can be in
 */
public enum NodeState {

    /**
     * Node is currently in the ring and operational
     */
    CONNECTED,

    /**
     * Node is currently NOT in the ring, therefore NOT operational
     */
    DISCONNECTED,

    /**
     * Node is currently in the ring, but NOT operational
     */
    CRASHED,
}
