package master2016;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * Created by Sophie on 11/24/16.
 */
public class TwitterTop3Bolt extends BaseRichBolt {

    private HashMap<String, String> langTokenDict = null;

    private String outputFolder = null;

    // <lang, hashtags>
    private HashMap<String, LinkedList<String>> top3Hashtags = null;

    // based on the routing strategy, each language will have a counter, <lang, counter>
    private HashMap<String, Integer> condWindowsCounters = null;

    // flags of condWindows, true represents within a conditional window, false to start a new windows
    private HashMap<String, Boolean> condWindowsFlags = null;

    public TwitterTop3Bolt(HashMap<String, String> langTokenDict, String outputFolder) {
        this.langTokenDict = langTokenDict;
        this.outputFolder = outputFolder;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        // initialize top3Hashtags and condWindowsCounters
        top3Hashtags = new HashMap<>(langTokenDict.size() * 2);
        condWindowsCounters = new HashMap<>(langTokenDict.size() * 2);
        condWindowsFlags = new HashMap<>(langTokenDict.size() * 2);

        for(String lang : langTokenDict.keySet()) {
            top3Hashtags.put(lang, new LinkedList<String>());
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

            System.out.println("language " + language);
            System.out.println("hashtag " + hashtag);

            // starting or ending window hashtag
            if(hashtag.equals(langTokenDict.get(language))) {
                // false, to start a new window
                if(!condWindowsFlags.get(language)) {
                    condWindowsFlags.put(language, true);
                    condWindowsCounters.put(language, condWindowsCounters.get(language) + 1);
                }
                else {
                    // true, end of a window, output results, TODO
                    condWindowsFlags.put(language, false);

                    System.out.println("Counter: " + condWindowsCounters.get(language));
                    System.out.println("hashtags: " + top3Hashtags.get(language).toString());

                    top3Hashtags.get(language).clear();
                }
            }
            else {
                // to compute top3
                top3Hashtags.get(language).add(hashtag);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // there is no bolt to output to
    }
}
