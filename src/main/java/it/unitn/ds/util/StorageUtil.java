package it.unitn.ds.util;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import it.unitn.ds.server.Item;
import it.unitn.ds.server.Node;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Iterator;

/**
 * Convenient class to work with Node's internal list of items and maintain CSV file
 */
public abstract class StorageUtil {

    private static final Logger logger = LogManager.getLogger();

    private static final String SEPARATOR = ",";

    /**
     * Creates/Updates list of new items into CSV file in format: {key},{value},{version}
     *
     * @param node responsible for item
     */
    public static Collection<Item> write(Node node) {
        Collection<Item> items = node.getItems().values();
        try (PrintWriter writer = new PrintWriter(getFileName(node.getId()), "UTF-8")) {
            for (Item item : items) {
                writer.write(item.getKey() + SEPARATOR + item.getValue() + SEPARATOR + item.getVersion() + "\n");
                logger.debug("Storage of node=" + node.getId() + " wrote an item=" + item);
            }
        } catch (Exception e) {
            logger.error("Failed to write items of the node=" + node, e);
        }
        return items;
    }

    /**
     * Returns an item by key from CSV file of the provided node
     *
     * @param nodeId responsible for item
     * @param key    of the item
     * @return an item fetched by key from given node
     */
    @Nullable
    public static Item read(int nodeId, int key) {
        try {
            for (String line : Files.readAllLines(Paths.get((getFileName(nodeId))), Charsets.UTF_8)) {
                if (line.startsWith(key + SEPARATOR)) {
                    Iterator<String> it = Splitter.on(SEPARATOR).split(line).iterator();
                    Item item = new Item(Integer.parseInt(it.next()), it.next(), Integer.parseInt(it.next()));
                    logger.debug("Storage of node=" + nodeId + " read an item=" + item);
                    return item;
                }
            }
        } catch (Exception e) {
            logger.error("Failed to read item with key=" + key + " for nodeId=" + nodeId, e);
        }
        return null;
    }

    /**
     * Removes file storage of the given node
     *
     * @param nodeId id of the node to remove its CSV file
     */
    public static void removeFile(int nodeId) {
        try {
            Path path = Paths.get(getFileName(nodeId));
            if (Files.exists(path)) {
                Files.delete(path);
            }
        } catch (Exception e) {
            logger.error("Failed to remove file for nodeId=" + nodeId, e);
        }
    }

    private static String getFileName(int nodeId) {
        return "storage/Node-" + nodeId + ".csv";
    }
}
