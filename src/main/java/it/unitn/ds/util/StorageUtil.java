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
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

/**
 * Convenient class to work with Node's internal list of items and maintain CSV file
 */
public abstract class StorageUtil {

    private static final Logger logger = LogManager.getLogger();

    private static final String SEPARATOR = ",";

    public static void main(String[] args) {
        Node node = new Node(1);
        Map<Integer, Item> i = new HashMap<>();
        i.put(11, new Item(11, "I am a Cow11", 1));
        i.put(12, new Item(12, "I am a Cow12", 2));
        i.put(13, new Item(13, "I am a Cow13", 3));
        node.setItems(i);
        write(node, new Item(10, "I am a Cow", 1));
        write(node, new Item(13, "I am a Cow13 updated", 4));
        logger.info(Arrays.toString(node.getItems().values().toArray()));
        logger.info(read(node, 11));
        logger.info(read(node, 12));
        logger.info(read(node, 13));
        logger.info(read(node, 10));
        logger.info(read(node, 9));
    }

    /**
     * Creates/Updates new item into memory of given node and
     * to CSV file in format: {key},{value},{version}
     *
     * @param node    responsible for item
     * @param newItem item to be written
     */
    public static void write(Node node, Item newItem) {
        write(node, new ArrayList<>(Arrays.asList(newItem)));
    }

    /**
     * Creates/Updates list of new items into memory of given node and
     * to CSV file in format: {key},{value},{version}
     *
     * @param node     responsible for item
     * @param newItems list of item to be written
     */
    public static void write(Node node, List<Item> newItems) {
        try (PrintWriter writer = new PrintWriter(getFileName(node), "UTF-8")) {
            for (Item item : updateNodeItems(node, newItems).values()) {
                logger.debug("Storage write item=" + item);
                writer.write(item.getKey() + SEPARATOR + item.getValue() + SEPARATOR + item.getVersion() + "\n");
            }
        } catch (Exception e) {
            logger.error("Failed to write items=" + Arrays.toString(newItems.toArray()) + " for node=" + node, e);
        }
    }

    private static Map<Integer, Item> updateNodeItems(Node node, List<Item> newItems) {
        Map<Integer, Item> items = node.getItems();
        for (Item item : newItems) {
            items.put(item.getKey(), item);
        }
        return items;
    }

    /**
     * Returns an item by key from CSV file of the provided node
     *
     * @param node responsible for item
     * @param key  of the item
     * @return an item fetched by key from given node
     */
    @Nullable
    public static Item read(Node node, int key) {
        try {
            for (String line : Files.readLines(new File(getFileName(node)), Charsets.UTF_8)) {
                if (line.startsWith(key + SEPARATOR)) {
                    Iterator<String> it = Splitter.on(SEPARATOR).split(line).iterator();
                    Item item = new Item(Integer.parseInt(it.next()), it.next(), Integer.parseInt(it.next()));
                    logger.debug("Storage read item=" + item);
                    return item;
                }
            }
        } catch (Exception e) {
            logger.error("Failed to read item with key=" + key + " for node=" + node, e);
        }
        return null;
    }

    private static String getFileName(Node node) {
        return "Node-" + node.getId() + ".csv";
    }
}
