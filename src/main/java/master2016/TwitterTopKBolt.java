package master2016;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Created by Sophie on 11/24/16.
 */
public class TwitterTopKBolt extends BaseRichBolt {

    private HashMap<String, String> langTokenDict = null;
    private String outputFolder = null;
    private int k = 3;

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
        // initialize condWindowsCounters and condWindowsFlags
        streamTopKCounters = new HashMap<>(langTokenDict.size() * 2);
        condWindowsCounters = new HashMap<>(langTokenDict.size() * 2);
        condWindowsFlags = new HashMap<>(langTokenDict.size() * 2);

        for(String lang : langTokenDict.keySet()) {
            streamTopKCounters.put(lang, new StreamTopK(k));
            condWindowsCounters.put(lang, 0);
            condWindowsFlags.put(lang, false);
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
                }
                else {
                    // true, end of a window, output results, TODO, less than k tags
                    condWindowsFlags.put(language, false);

                    System.out.println("Counter: " + condWindowsCounters.get(language));
                    System.out.println("hashtags: ");
                    for (Entry<String, Integer> o : streamTopKCounters.get(language).topk()) {
                        System.out.println(o.toString());
                    }

                    streamTopKCounters.get(language).clear();
                }
            }
            else {
                // to compute top3
                streamTopKCounters.get(language).add(hashtag, 1);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // there is no bolt to output to
    }
}
