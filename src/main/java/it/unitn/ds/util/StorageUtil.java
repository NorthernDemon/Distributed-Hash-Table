package it.unitn.ds.util;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.io.Files;
import it.unitn.ds.server.Item;
import it.unitn.ds.server.Node;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.PrintWriter;
import java.util.*;

/**
 * Convenient class to work with Node's internal list of items and maintain CSV file
 */
public abstract class StorageUtil {

    private static final Logger logger = LogManager.getLogger();

    public static final String SEPARATOR = ",";

    /**
     * Creates/Updates new item into memory of given node and
     * to CSV file in format: {key},{value},{version}
     *
     * @param node    responsible for item
     * @param newItem item to be written
     */
    @Nullable
    public static Item write(Node node, Item newItem) {
        List<Item> items = write(node, new ArrayList<>(Arrays.asList(newItem)));
        if (!items.isEmpty()) {
            return items.iterator().next();
        } else {
            return null;
        }
    }

    /**
     * Creates/Updates list of new items into memory of given node and
     * to CSV file in format: {key},{value},{version}
     *
     * @param node     responsible for item
     * @param newItems list of item to be written
     */
    public static List<Item> write(Node node, List<Item> newItems) {
        try (PrintWriter writer = new PrintWriter(getFileName(node.getId()), "UTF-8")) {
            Collection<Item> items = updateNodeItems(node, newItems).values();
            for (Item item : items) {
                logger.debug("Storage write item=" + item);
                writer.write(item.getKey() + SEPARATOR + item.getValue() + SEPARATOR + item.getVersion() + "\n");
            }
            return newItems;
        } catch (Exception e) {
            logger.error("Failed to write items=" + Arrays.toString(newItems.toArray()) + " for node=" + node, e);
        }
        return new ArrayList<>();
    }

    private static Map<Integer, Item> updateNodeItems(Node node, List<Item> newItems) {
        for (Item item : newItems) {
            node.getItems().put(item.getKey(), item);
        }
        return node.getItems();
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
            for (String line : Files.readLines(new File(getFileName(nodeId)), Charsets.UTF_8)) {
                if (line.startsWith(key + SEPARATOR)) {
                    Iterator<String> it = Splitter.on(SEPARATOR).split(line).iterator();
                    Item item = new Item(Integer.parseInt(it.next()), it.next(), Integer.parseInt(it.next()));
                    logger.debug("Storage read item=" + item);
                    return item;
                }
            }
        } catch (Exception e) {
            logger.error("Failed to read item with key=" + key + " for nodeId=" + nodeId, e);
        }
        return null;
    }

    private static String getFileName(int nodeId) {
        return "storage/Node-" + nodeId + ".csv";
    }
}
