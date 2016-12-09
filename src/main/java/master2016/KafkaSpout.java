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

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

/**
 * Created by Sophie on 11/24/16.
 */
public class KafkaSpout extends BaseRichSpout {

    public final static String LANGUAGE_NAME = "lang";
    public final static String HASHTAG_NAME = "hashtag";
    public final static String TWITTER_STREAM_NAME = "twitter";

    private SpoutOutputCollector collector = null;

    KafkaConsumer<String, String> consumer = null;

    // separated by ,
    private String kafkaBrokerList = null;

    private String groupID = null;

    public KafkaSpout(String kafkaBrokerList, String groupID) {
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
        consumer.subscribe(Arrays.asList(StartTwitterApp.TOPIC_NAME));
    }

    private Values parseLangHashtag(String langHashtag) {
        String[] langHashtagList = langHashtag.split(",");
        String[] languagePair = langHashtagList[0].split(":");

        // when language is empty, set it as "null"
        String language = null;
        if(languagePair.length >= 2) {
            language = languagePair[1];
        }
        else {
            language = "null";
        }

        // TODO, multiple hashtags
        String[] hashtagPair = langHashtagList[1].split(":");

        // when hashtag is empty, set it as null
        String hashtag = null;
        if(hashtagPair.length >= 2) {
            hashtag = hashtagPair[1];
        }

        return new Values(language, hashtag);
    }

    @Override
    public void nextTuple() {
        ConsumerRecords<String, String> records = consumer.poll(100);
        for (ConsumerRecord<String, String> record : records) {
//            System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());

            Values val = parseLangHashtag(record.value());
            collector.emit(TWITTER_STREAM_NAME, val);

            System.out.println("Emit: " + val.toString());
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(TWITTER_STREAM_NAME, new Fields(LANGUAGE_NAME, HASHTAG_NAME));
    }
}
