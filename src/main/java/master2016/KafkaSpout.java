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
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;

/**
 * Created by Sophie on 11/24/16.
 */
public class KafkaSpout extends BaseRichSpout {

    // test the code, TODO, delete after finishing development
    private boolean testMode = false;

    public final static String LANGUAGE_NAME = "lang";
    public final static String HASHTAGS_NAME = "hashtag";
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

    // TODO, delete after finishing development
    public KafkaSpout(String kafkaBrokerList, String groupID, boolean testMode) {
        this.kafkaBrokerList = kafkaBrokerList;
        this.groupID = groupID;
        this.testMode = testMode;
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

        // multiple hashtags, return all hashtags split by :
        String hashtagsPair = langHashtagList[1];

        // when hashtags are empty, set it as null
        String hashtags = null;

        int firstOccuIndex = hashtagsPair.indexOf(":");
        // hashtags are not empty
        if(firstOccuIndex != hashtagsPair.length() - 1) {
            hashtags = hashtagsPair.substring(firstOccuIndex + 1);
        }

        return new Values(language, hashtags);
    }

    @Override
    public void nextTuple() {

        // TODO, delete after finishing development
         if(testMode) {

             // start es hashtag
             Values val = parseLangHashtag("lang:en,hashtags:machine learning");
             collector.emit(TWITTER_STREAM_NAME, val);
             System.out.println("Emit: " + val.toString());

             val = parseLangHashtag("lang:en,hashtags:machine learning");
             collector.emit(TWITTER_STREAM_NAME, val);
             System.out.println("Emit: " + val.toString());

             val = parseLangHashtag("lang:en,hashtags:machine learning");
             collector.emit(TWITTER_STREAM_NAME, val);
             System.out.println("Emit: " + val.toString());

             val = parseLangHashtag("lang:en,hashtags:abc");
             collector.emit(TWITTER_STREAM_NAME, val);
             System.out.println("Emit: " + val.toString());

             val = parseLangHashtag("lang:en,hashtags:abc");
             collector.emit(TWITTER_STREAM_NAME, val);
             System.out.println("Emit: " + val.toString());

             val = parseLangHashtag("lang:en,hashtags:is");
             collector.emit(TWITTER_STREAM_NAME, val);
             System.out.println("Emit: " + val.toString());

             val = parseLangHashtag("lang:en,hashtags:a dog");
             collector.emit(TWITTER_STREAM_NAME, val);
             System.out.println("Emit: " + val.toString());

             val = parseLangHashtag("lang:en,hashtags:a dog");
             collector.emit(TWITTER_STREAM_NAME, val);
             System.out.println("Emit: " + val.toString());

             // start es hashtag
             val = parseLangHashtag("lang:es,hashtags:madrid");
             collector.emit(TWITTER_STREAM_NAME, val);
             System.out.println("Emit: " + val.toString());

             val = parseLangHashtag("lang:en,hashtags:hahaha");
             collector.emit(TWITTER_STREAM_NAME, val);
             System.out.println("Emit: " + val.toString());

             val = parseLangHashtag("lang:en,hashtags:a dog");
             collector.emit(TWITTER_STREAM_NAME, val);
             System.out.println("Emit: " + val.toString());

             // end en hashtag
             val = parseLangHashtag("lang:en,hashtags:machine learning");
             collector.emit(TWITTER_STREAM_NAME, val);
             System.out.println("Emit: " + val.toString());

             val = parseLangHashtag("lang:es,hashtags:is");
             collector.emit(TWITTER_STREAM_NAME, val);
             System.out.println("Emit: " + val.toString());

             val = parseLangHashtag("lang:es,hashtags:a");
             collector.emit(TWITTER_STREAM_NAME, val);
             System.out.println("Emit: " + val.toString());

             val = parseLangHashtag("lang:es,hashtags:beautiful");
             collector.emit(TWITTER_STREAM_NAME, val);
             System.out.println("Emit: " + val.toString());

             val = parseLangHashtag("lang:es,hashtags:city");
             collector.emit(TWITTER_STREAM_NAME, val);
             System.out.println("Emit: " + val.toString());

             // end es hashtag
             val = parseLangHashtag("lang:es,hashtags:madrid");
             collector.emit(TWITTER_STREAM_NAME, val);
             System.out.println("Emit: " + val.toString());

             return;
         }


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
        outputFieldsDeclarer.declareStream(TWITTER_STREAM_NAME, new Fields(LANGUAGE_NAME, HASHTAGS_NAME));
    }
}
