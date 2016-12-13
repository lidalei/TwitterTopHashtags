package master2016;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Created by Sophie on 11/24/16.
 */
public class TwitterTopKBolt extends BaseRichBolt {

    // TODO, each bolt deals with some languages instead of all languages
    // TODO, create new files only when necessary

    private HashMap<String, String> langTokenDict = null;
    private String outputFolder = null;
    private int k = 3;

    private HashMap<String, BufferedWriter> resWriters = null;
    private final String resFileNameSuffix = "_21.log";

    // an object used to store and output top k hashtags
    private HashMap<String, StreamTopK> streamTopKCounters = null;
    // based on the routing strategy, each language will have a counter, <lang, counter>
    private HashMap<String, Integer> condWindowsCounters = null;
    // flags of condWindows, true represents within a conditional window, false to start a new windows
    private HashMap<String, Boolean> condWindowsFlags = null;

    public TwitterTopKBolt(HashMap<String, String> langTokenDict, String outputFolder, int k) {
        this.langTokenDict = langTokenDict;
        this.outputFolder = outputFolder;
        if(k <= 0) {
            this.k = 3;
        }
        else {
            this.k = k;
        }
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        int initialCapacity = langTokenDict.size() * 2;

        // initialize condWindowsCounters and condWindowsFlags
        streamTopKCounters = new HashMap<>(initialCapacity);
        condWindowsCounters = new HashMap<>(initialCapacity);
        condWindowsFlags = new HashMap<>(initialCapacity);

        for(String lang : langTokenDict.keySet()) {
            streamTopKCounters.put(lang, new StreamTopK(k));
            condWindowsCounters.put(lang, 0);
            condWindowsFlags.put(lang, false);
        }

        // initialize result writers
        resWriters = new HashMap<>(initialCapacity);

        try{
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

    @Override
    public void execute(Tuple tuple) {
        String language = tuple.getStringByField(KafkaSpout.LANGUAGE_NAME);

        // not a language to be computed
        if(!langTokenDict.containsKey(language)) {
            return;
        }

        String hashtags = tuple.getStringByField(KafkaSpout.HASHTAGS_NAME);

        // there is no hashtag
        if(hashtags == null) {
            return;
        }

        // deal with multiple hashtags
        for (String hashtag : hashtags.split(":")) {
//            System.out.println("language " + language);
//            System.out.println("hashtag " + hashtag);

            // starting or ending window hashtag
            if(hashtag.equals(langTokenDict.get(language))) {
                // false, to start a new window
                if(!condWindowsFlags.get(language)) {
                    condWindowsFlags.put(language, true);
                    condWindowsCounters.put(language, condWindowsCounters.get(language) + 1);

                    // clear the window
                    streamTopKCounters.get(language).clear();
                }
                else {
                    // true, end of a window, output results
                    condWindowsFlags.put(language, false);

                    List<Entry<String, Integer>> topKHashtags = streamTopKCounters.get(language).topk();

                    // TODO, comment after finishing development
                    System.out.println("Counter: " + condWindowsCounters.get(language));
                    System.out.println("hashtags: ");


                    StringBuilder strBuilder = new StringBuilder(50);
                    strBuilder.append(condWindowsCounters.get(language)).append("," + language);

                    for (Entry<String, Integer> o : topKHashtags) {
                        // TODO, comment after finishing development
                        System.out.println(o.toString());

                        strBuilder.append("," + o.getKey() + "," + o.getValue());
                    }

                    int paddingSize = k - topKHashtags.size();

                    if(paddingSize > 0) {
                        for(int i = 0; i < paddingSize; ++i) {
                            strBuilder.append(",null,0");
                        }
                    }

                    try {
                        resWriters.get(language).write(strBuilder.toString());
                        resWriters.get(language).newLine();
                    } catch (IOException e) {
                        e.printStackTrace();

                        System.out.println("Fatal error: " + e.getMessage() + ". To terminate the program.");
                    }

                }
            }
            else {
                // only add this hahstag when the window is active
                if(condWindowsFlags.get(language)) {
                    streamTopKCounters.get(language).add(hashtag, 1);
                }
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // there is no bolt to output to
    }


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
