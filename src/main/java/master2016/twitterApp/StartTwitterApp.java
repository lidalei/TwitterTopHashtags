/*
* Thanks https://github.com/saurzcode/twitter-stream/blob/master/src/main/java/com/saurzcode/twitter/TwitterKafkaProducer.java.
*
* */

package master2016.twitterApp;

import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
//import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Created by Sophie on 11/17/16.
 */
public class StartTwitterApp {

    public final static String TOPIC_NAME = "DataManagement";

    private void getTweetsAndStoreToKafka(HashMap<String, String> twitterAPIParams) {

        System.out.println("twitterAPIParas: " + twitterAPIParams.toString());


        if(twitterAPIParams.get("mode").equals("1")) { // read from file

            // TODO
        }
        else { // get twitters from twitter

            BlockingQueue<String> twitterQueue = new LinkedBlockingQueue<>();

            // Producer properties
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, twitterAPIParams.get("kafkaBrokerURL"));
            props.put(ProducerConfig.ACKS_CONFIG, "all");
            props.put(ProducerConfig.RETRIES_CONFIG, 0);
            props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
            props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
            props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

            Producer<String, String> producer = new KafkaProducer<>(props);

            // connect to twitter
            StatusesSampleEndpoint endpoint = new StatusesSampleEndpoint();
            endpoint.filterLevel(Constants.FilterLevel.None);

            Authentication auth = new OAuth1(twitterAPIParams.get("twitterAPIKey"), twitterAPIParams.get("APISecret"),
                    twitterAPIParams.get("tokenValue"), twitterAPIParams.get("tokenSecret"));

            // Create a new BasicClient. By default gzip is enabled.
            Client client = new ClientBuilder().hosts(Constants.STREAM_HOST)
                    .endpoint(endpoint).authentication(auth)
                    .processor(new StringDelimitedProcessor(twitterQueue)).build();

            // Establish a connection
            client.connect();

            TwitterParser twitterParser = new TwitterParser();

            // send twitter to kafka
            for (int i = 0; i < 100000; ++i) {
                ProducerRecord<String, String> twitter = null;
                try {
                    String languageHashTag = twitterParser.parse(twitterQueue.take());

                    twitter = new ProducerRecord<>(TOPIC_NAME, languageHashTag);
                    System.out.println(twitter);

                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                producer.send(twitter);
            }

            // stop store twitters in kafka
            producer.close();
            // disconnect twitter
            client.stop();
        }
    }


    public static void main(String[] args) {
        if(args.length < 7) {
            System.out.println("Not enough parameters. There should be seven parameters.");
            return;
        }

        System.out.println("Parameters:");
        HashMap<String, String> twitterAPIParams = new HashMap<>();

        // parse mode, 1 means reading from file, 2 from Twitter API
        int mode = Integer.valueOf(args[0]);
        if(mode != 1 && mode != 2) {
            System.out.println("Incorrect first parameter: mode. Please try agin!");
            return;
        }

        twitterAPIParams.put("mode", Integer.toString(mode));
        System.out.println("mode: " + mode);

        // parse Twitter API key
        String twitterAPIKey = args[1];
        if(twitterAPIKey.equals("null")) {
            twitterAPIKey = "l3NSaKh0ELLHbgnNYoDzII8GR";
        }

        twitterAPIParams.put("twitterAPIKey", twitterAPIKey);
        System.out.println("Twitter API key: " + twitterAPIKey);

        // parse secret associated with the Twitter app consumer
        String APISecret = args[2];
        if(APISecret.equals("null")) {
            APISecret = "NS0ePZrk5vMRfwX1x3YobJEaWb7KgVeWUO0WL1Xl1MNmYAh9c7";
        }

        twitterAPIParams.put("APISecret", APISecret);
        System.out.println("Secret key: " + APISecret);

        // parse access token associated with the Twitter app
        String tokenValue = args[3];
        if(tokenValue.equals("null")) {
            tokenValue = "783660909268963329-UqRnX33mwnMmSJodGsyqq2WsC4OIAEo";
        }

        twitterAPIParams.put("tokenValue", tokenValue);
        System.out.println("Access token: " + tokenValue);

        // parse access token secret
        String tokenSecret = args[4];
        if(tokenSecret.equals("null")) {
            tokenSecret = "VfKpxmoQKiIbGy8sTpcC9mJKueXzuQdVT3fLJBAUZdLxX";
        }

        twitterAPIParams.put("tokenSecret", tokenSecret);
        System.out.println("Access token secret: " + tokenSecret);

        // parse Kafka Broker URL: String in the format IP:port corresponding with the Kafka Broker
        String kafkaBrokerURL = args[5];

        if(kafkaBrokerURL.equals("null")) {
            kafkaBrokerURL = "localhost:9092";
        }

        twitterAPIParams.put("kafkaBrokerURL", kafkaBrokerURL);
        System.out.println("Kafka Broker URL: " + kafkaBrokerURL);

        // parse Filename, path to the file with the tweets
        // the path is relative to the filesystem of the node that will be used to run the Twitter app
        String fileName = args[6];

        twitterAPIParams.put("fileName", fileName);
        System.out.println("File name: " + fileName);

        StartTwitterApp selfInstance = new StartTwitterApp();
        selfInstance.getTweetsAndStoreToKafka(twitterAPIParams);

    }


}
