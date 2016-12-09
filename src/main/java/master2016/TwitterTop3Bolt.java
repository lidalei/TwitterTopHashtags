package master2016;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created by Sophie on 11/24/16.
 */
public class TwitterTop3Bolt extends BaseRichBolt {

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        // nothing to prepare
    }

    @Override
    public void execute(Tuple tuple) {
        // TODO
        System.out.println("language " + tuple.getStringByField(KafkaSpout.LANGUAGE_NAME));
        System.out.println("hashtag " + tuple.getStringByField(KafkaSpout.HASHTAG_NAME));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // there is no bolt to output to
    }
}
