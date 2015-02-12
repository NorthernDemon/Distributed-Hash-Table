package it.unitn.ds.util;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import it.unitn.ds.entity.Item;
import it.unitn.ds.entity.Node;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

    private static final String STORAGE_FOLDER = "storage";

    /**
     * Creates/Updates list of new items into CSV file in format: {key},{value},{version},{nodeId}
     *
     * @param node responsible for item
     */
    public static void write(Node node) {
        try (PrintWriter writer = new PrintWriter(getFileName(node.getId()), "UTF-8")) {
            writeItem(writer, node.getItems().values());
            writeItem(writer, node.getReplicas().values());
        } catch (Exception e) {
            logger.error("Failed to write items of the node=" + node, e);
        }
    }

    private static void writeItem(PrintWriter writer, Collection<Item> items) {
        for (Item item : items) {
            writer.write(item.getKey() + SEPARATOR + item.getValue() + SEPARATOR + item.getVersion() + SEPARATOR + item.getNodeId() + "\n");
            logger.debug("Storage wrote an item=" + item);
        }
    }

    /**
     * Returns an item by key from CSV file of the provided node in format: {key},{value},{version},{nodeId}
     *
     * @param nodeId responsible for item
     * @param key    of the item
     * @return an item fetched by key from given node
     */
    public static Item read(int nodeId, int key) {
        try {
            for (String line : Files.readAllLines(Paths.get((getFileName(nodeId))), Charsets.UTF_8)) {
                if (line.startsWith(key + SEPARATOR)) {
                    Iterator<String> it = Splitter.on(SEPARATOR).split(line).iterator();
                    Item item = new Item(Integer.parseInt(it.next()), it.next(), Integer.parseInt(it.next()), Integer.parseInt(it.next()));
                    logger.debug("Storage of node=" + nodeId + " read an item=" + item);
                    return item;
                }
            }
        } catch (Exception e) {
            logger.error("Failed to read item with key=" + key + " for nodeId=" + nodeId, e);
        }
        return new Item();
    }

    /**
     * Creates storage folder
     */
    public static void init() {
        try {
            Path path = Paths.get(STORAGE_FOLDER);
            if (!Files.exists(path)) {
                Files.createDirectory(path);
            }
        } catch (Exception e) {
            logger.error("Failed to create storage directory", e);
        }
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
        return STORAGE_FOLDER + "/Node-" + nodeId + ".csv";
    }
}
