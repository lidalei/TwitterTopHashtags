package master2016;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Sophie on 12/14/16.
 */
public class WriteTopKHashtagsBolt extends BaseRichBolt {

    private OutputCollector collector = null;

    private HashMap<String, String> langTokenDict = null;
    private String outputFolder = null;

    private HashMap<String, BufferedWriter> resWriters = null;
    private final String resFileNameSuffix = "_21.log";

    public WriteTopKHashtagsBolt(HashMap<String, String> langTokenDict, String outputFolder) {
        this.langTokenDict = langTokenDict;
        this.outputFolder = outputFolder;
    }

    /**
     * Called when a task for this component is initialized within a worker on the cluster.
     * It provides the bolt with the environment in which the bolt executes.
     * <p>
     * This includes the:
     *
     * @param stormConf The Storm configuration for this bolt. This is the configuration provided to the topology merged in with cluster configuration on this machine.
     * @param context   This object can be used to get information about this task's place within the topology, including the task id and component id of this task, input and output information, etc.
     * @param collector The collector is used to emit tuples from this bolt. Tuples can be emitted at any time, including the prepare and cleanup methods. The collector is thread-safe and should be saved as an instance variable of this bolt object.
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;

        int initialCapacity = langTokenDict.size() * 2;

        // initialize result writers
        resWriters = new HashMap<>(initialCapacity);

        try{

            // create if the output folder does not exist
            Path outputFolderPath = FileSystems.getDefault().getPath(outputFolder);
            if(!Files.isDirectory(outputFolderPath)) {
                Files.createDirectory(outputFolderPath);
            }

            // create several files
            for(String lang : langTokenDict.keySet()) {
                Path resFilePath = FileSystems.getDefault().getPath(outputFolder, lang + resFileNameSuffix);
                resWriters.put(lang, Files.newBufferedWriter(resFilePath, StandardCharsets.UTF_8));
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.out.println("Fatal error: " + e.getMessage() + ". To terminate the program.");
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Fatal error: " + e.getMessage() + ". To terminate the program.");
        }
    }

    /**
     * Process a single tuple of input. The Tuple object contains metadata on it
     * about which component/stream/task it came from. The values of the Tuple can
     * be accessed using Tuple#getValue. The IBolt does not have to process the Tuple
     * immediately. It is perfectly fine to hang onto a tuple and process it later
     * (for instance, to do an aggregation or join).
     * <p>
     * Tuples should be emitted using the OutputCollector provided through the prepare method.
     * It is required that all input tuples are acked or failed at some point using the OutputCollector.
     * Otherwise, Storm will be unable to determine when tuples coming off the spouts
     * have been completed.
     * <p>
     * For the common case of acking an input tuple at the end of the execute method,
     * see IBasicBolt which automates this.
     *
     * @param input The input tuple to be processed.
     */
    @Override
    public void execute(Tuple input) {
        String language = input.getStringByField(TwitterTopKBolt.LANGUAGE_NAME);

        String topkHashtags = input.getStringByField(TwitterTopKBolt.TWEET_TOPK_HASHTAGS_NAME);


        try {
            resWriters.get(language).write(topkHashtags);
            resWriters.get(language).newLine();
        } catch (IOException e) {
            e.printStackTrace();

            System.out.println("Fatal error: " + e.getMessage() + ". To terminate the program.");
        }
    }

    /**
     * Declare the output schema for all the streams of this topology.
     *
     * @param declarer this is used to declare output stream ids, output fields, and whether or not each output stream is a direct stream
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}

    @Override
    public void cleanup() {
        // close all file resources
        try {
            for(BufferedWriter resWriter : resWriters.values()) {
                resWriter.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
