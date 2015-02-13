package it.unitn.ds;

/**
 * Static arbitrary values for replication
 * <p/>
 * Must maintain the formula [ W + R > N ] to avoid read/write conflicts
 */
public interface Replication {

    /**
     * Write quorum
     */
    int W = 2;

    /**
     * Read quorum
     */
    int R = 2;

    /**
     * Count of nodes used for replication
     */
    int N = 3;
}
