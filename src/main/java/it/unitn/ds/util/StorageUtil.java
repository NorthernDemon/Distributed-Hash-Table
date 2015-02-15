package it.unitn.ds.util;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import it.unitn.ds.entity.Item;
import it.unitn.ds.entity.Node;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Convenient class to work with Node's internal list of items
 * <p/>
 * Maintains CSV file (under STORAGE_FOLDER directory) in format: {key},{value},{version}
 */
public abstract class StorageUtil {

    private static final Logger logger = LogManager.getLogger();

    private static final String SEPARATOR = ",";

    private static final String STORAGE_FOLDER = "storage";

    /**
     * Creates/Updates list of nodes items and replicas into CSV file
     *
     * @param node to write
     */
    public static void write(Node node) {
        try (PrintWriter writer = new PrintWriter(getFileName(node.getId()), "UTF-8")) {
            writeItems(writer, node.getItems().values());
            writeItems(writer, node.getReplicas().values());
        } catch (Exception e) {
            logger.error("Failed to write items from node=" + node, e);
        }
    }

    private static void writeItems(PrintWriter writer, Collection<Item> items) {
        for (Item item : items) {
            writer.write(item.getKey() + SEPARATOR + item.getValue() + SEPARATOR + item.getVersion() + System.getProperty("line.separator"));
            logger.debug("Storage wrote an item=" + item);
        }
    }

    /**
     * Returns all items and replicas from node's CSV file
     *
     * @param nodeId of the node
     * @return all items and replicas of node's storage
     */
    public static List<Item> readAll(int nodeId) {
        List<Item> items = new ArrayList<>();
        try {
            for (String line : Files.readAllLines(Paths.get((getFileName(nodeId))), Charsets.UTF_8)) {
                Iterator<String> it = Splitter.on(SEPARATOR).split(line).iterator();
                items.add(new Item(Integer.parseInt(it.next()), it.next(), Integer.parseInt(it.next())));
            }
        } catch (Exception e) {
            logger.error("Failed to read items from nodeId=" + nodeId, e);
        }
        logger.debug("Storage of node=" + nodeId + " read all item=" + Arrays.toString(items.toArray()));
        return items;
    }

    /**
     * Returns an item from node's CSV file
     *
     * @param nodeId  of the node
     * @param itemKey of the item
     * @return an item from node's CSV file
     */
    @Nullable
    public static Item read(int nodeId, int itemKey) {
        try {
            for (String line : Files.readAllLines(Paths.get((getFileName(nodeId))), Charsets.UTF_8)) {
                if (line.startsWith(itemKey + SEPARATOR)) {
                    Iterator<String> it = Splitter.on(SEPARATOR).split(line).iterator();
                    Item item = new Item(Integer.parseInt(it.next()), it.next(), Integer.parseInt(it.next()));
                    logger.debug("Storage of node=" + nodeId + " read an item=" + item);
                    return item;
                }
            }
        } catch (Exception e) {
            logger.error("Failed to read item with itemKey=" + itemKey + " from nodeId=" + nodeId, e);
        }
        return null;
    }

    /**
     * Creates storage folder to keep node's CSV files in
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
     * Removes node's CSV file
     *
     * @param nodeId of the node
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
