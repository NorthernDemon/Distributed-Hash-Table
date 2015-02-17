package it.unitn.ds;

/**
 * Static arbitrary values for replication
 * <p/>
 * Must maintain the formula [ W + R > N ] to avoid read/write conflicts
 *
 * @see it.unitn.ds.entity.Item
 * @see it.unitn.ds.entity.Node
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
     * Count of successor nodes used for replication, including itself
     */
    int N = 3;
}
