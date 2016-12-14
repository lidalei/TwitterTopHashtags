package master2016;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Created by Sophie on 11/24/16.
 */
public class TwitterTopKBolt extends BaseRichBolt {

    public final static String TWEET_TOPK_HASHTAGS_STREAM = "tweet_topk_hashtags_stream";
    public final static String LANGUAGE_NAME = "language";
    public final static String TWEET_TOPK_HASHTAGS_NAME = "tweet_topk_hashtags";

    private OutputCollector collector = null;

    private HashMap<String, String> langTokenDict = null;
    private int k = 3;

    // an object used to store and output top k hashtags
    private HashMap<String, StreamTopK> streamTopKCounters = null;
    // based on the routing strategy, each language will have a counter, <lang, counter>
    private HashMap<String, Integer> condWindowsCounters = null;
    // flags of condWindows, true represents within a conditional window, false to start a new windows
    private HashMap<String, Boolean> condWindowsFlags = null;

    public TwitterTopKBolt(HashMap<String, String> langTokenDict, int k) {
        this.langTokenDict = langTokenDict;
        if(k <= 0) {
            this.k = 3;
        }
        else {
            this.k = k;
        }
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;

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
                        strBuilder.append("," + o.getKey() + "," + o.getValue());
                    }

                    int paddingSize = k - topKHashtags.size();
                    if(paddingSize > 0) {
                        for(int i = 0; i < paddingSize; ++i) {
                            strBuilder.append(",null,0");
                        }
                    }

                    // TODO, emit topk hashtags
                    collector.emit(TWEET_TOPK_HASHTAGS_STREAM, new Values(language, strBuilder.toString()));

                    // TODO, comment after finishing development
                    System.out.println(language + "," + strBuilder.toString());

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
        outputFieldsDeclarer.declareStream(TWEET_TOPK_HASHTAGS_STREAM, new Fields(LANGUAGE_NAME, TWEET_TOPK_HASHTAGS_NAME));
    }


    @Override
    public void cleanup() {}
}
