package master2016;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
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

    private HashMap<String, LinkedList<String>> top3Hashtags = null;

    // based on the routing strategy, each language will have a counter
    private HashMap<String, Integer> condWindowsCounters = null;

    public TwitterTop3Bolt(HashMap<String, String> langTokenDict) {
        this.langTokenDict = langTokenDict;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        // initialize top3Hashtags and condWindowsCounters
        top3Hashtags = new HashMap<>(langTokenDict.size() * 2);
        condWindowsCounters = new HashMap<>(langTokenDict.size() * 2);
    }

    @Override
    public void execute(Tuple tuple) {
        // TODO
        String language = tuple.getStringByField(KafkaSpout.LANGUAGE_NAME);
        String hashtag = tuple.getStringByField(KafkaSpout.HASHTAG_NAME);


        System.out.println("language " + language);
        System.out.println("hashtag " + hashtag);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // there is no bolt to output to
    }
}
