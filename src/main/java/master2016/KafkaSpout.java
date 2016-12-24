package master2016;

import master2016.twitterApp.StartTwitterApp;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.*;

/**
 * Created by Sophie on 11/24/16.
 */
public class KafkaSpout extends BaseRichSpout {

    public final static String LANGUAGE_NAME = "lang";
    public final static String HASHTAGS_NAME = "hashtag";
    public final static String TWITTER_STREAM_NAME = "tweets";

    private SpoutOutputCollector collector = null;

    private KafkaConsumer<String, String> consumer = null;

    private HashMap<String, String> langTokenDict = null;
    // separated by ,
    private String kafkaBrokerList = null;

    private String groupID = null;

    public KafkaSpout(HashMap<String, String> langTokenDict, String kafkaBrokerList, String groupID) {
        this.langTokenDict = langTokenDict;
        this.kafkaBrokerList = kafkaBrokerList;
        this.groupID = groupID;
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        // initialize a consumer
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokerList);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupID);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(props);
        // receive records from topic
        consumer.subscribe(Arrays.asList(StartTwitterApp.TOPIC_NAME));
    }

    @Override
    public void nextTuple() {

        ConsumerRecords<String, String> records = consumer.poll(100);
        for (ConsumerRecord<String, String> record : records) {

            String lang = record.key();
            // only emit a tuple when the language is useful
            if(langTokenDict.containsKey(lang)) {
                Values val = new Values(lang, record.value());
                collector.emit(TWITTER_STREAM_NAME, val);

                // TODO, comment after finishing development
//                System.out.println("Emit: " + val.toString());
            }
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(TWITTER_STREAM_NAME, new Fields(LANGUAGE_NAME, HASHTAGS_NAME));
    }

    @Override
    public void close() {
        consumer.close();
    }
}
