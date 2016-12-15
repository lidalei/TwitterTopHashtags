/*
* Thanks https://github.com/saurzcode/twitter-stream/blob/master/src/main/java/com/saurzcode/twitter/TwitterKafkaProducer.java.
*
* */

package master2016.twitterApp;


import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
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

    private BufferedReader tweetsReader = null;

    private final static JsonParser jsonParser = new JsonParser();


    public StartTwitterApp() {}

    // switch to this function from TweetParser
    private String tweetParse(String jsonString) {
        // System.out.println(jsonString);

        JsonElement jsonElement = jsonParser.parse(jsonString);
        JsonObject jsonObject = jsonElement.getAsJsonObject();

        // the tweet was deleted if there is no language element
//            System.out.println(jsonObject.get("lang"));
        JsonElement langJsonObject = jsonObject.get("lang");

        if(langJsonObject == null) {
            return null;
        }

        String language = langJsonObject.getAsString();

        String hashTags = ",";
        JsonArray hashTagArr = jsonObject.getAsJsonObject("entities").getAsJsonArray("hashtags");

        if(hashTagArr.size() >= 1) {
            for(JsonElement e : hashTagArr) {
                // transform hashtag to lower case format
                hashTags += e.getAsJsonObject().get("text").getAsString() + ":";
            }

            String languageHashTags = language + hashTags;
            // remove the last : before return
            return  languageHashTags.substring(0, languageHashTags.length() - 1);
        }
        else {
            // there is no hashtag, directly drop the tweet
            return null;
        }
    }


    private void getTweetsAndStoreToKafka(HashMap<String, String> twitterAPIParams) {

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

        System.out.println("twitterAPIParas: " + twitterAPIParams.toString());

        if(twitterAPIParams.get("mode").equals("1")) { // read from file

            try {
                // To deal with FileNotFoundException
                tweetsReader = new BufferedReader(new InputStreamReader(new FileInputStream(twitterAPIParams.get("fileName")), StandardCharsets.UTF_8));

                // each line is a twitter
                String tweet = null;

                // To deal with IOException
                while((tweet = tweetsReader.readLine()) != null) {
                    // send twitter to kafka
                    ProducerRecord<String, String> twitterRecord = null;

                    String languageHashtags = tweetParse(tweet);

                    // this tweet was deleted or has no hashtag
                    if(languageHashtags == null) {
                        continue;
                    }

                    String[] langHashtagsArr = languageHashtags.split(",");
                    twitterRecord = new ProducerRecord<>(TOPIC_NAME, langHashtagsArr[0], langHashtagsArr[1]);
                    // TODO, comment after finishing development
                    System.out.println(twitterRecord);

                    producer.send(twitterRecord);
                }

                tweetsReader.close();

            }
            catch (FileNotFoundException e) {
                e.printStackTrace();

                System.out.println("Create BufferedReader from file error. Exception: " + e.getMessage());

                // fatal error, terminate program
                return;
            }
            catch (IOException e) {
                e.printStackTrace();

                System.out.println("Read tweets from file error. Exception: " + e.getMessage());

                // fatal error, terminate program
                return;
            }
            finally {
                // stop store twitters in kafka
                producer.close();
            }
        }
        else { // get twitters from twitter

            BlockingQueue<String> twitterQueue = new LinkedBlockingQueue<>();

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

            try {
                // send twitter to kafka forever
                for (;;) {
                    ProducerRecord<String, String> twitterRecord = null;
                    String languageHashtags = tweetParse(twitterQueue.take());

                    // this tweet was deleted or has no hashtag
                    if(languageHashtags == null) {
                        continue;
                    }

                    // key is the language, value is the hashtags split by :
                    String[] langHashtagsArr = languageHashtags.split(",");
                    twitterRecord = new ProducerRecord<>(TOPIC_NAME, langHashtagsArr[0], langHashtagsArr[1]);
                    System.out.println(twitterRecord);

                    producer.send(twitterRecord);
                }
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
            finally {
                // stop store twitters in kafka
                producer.close();
                // disconnect twitter
                client.stop();
            }
        }
    }


    public static void main(String[] args) {

        String parameterNames = "mode, apiKey, apiSecret, tokenValue, tokenSecret, Kafka Broker URL, Filename";

        if(args.length < 7) {
            System.out.println("Not enough parameters. There should be seven parameters: " + parameterNames + ".");
            return;
        }

        // TODO, comment after finishing development
        System.out.println("Parameters:");

        HashMap<String, String> twitterAPIParams = new HashMap<>(14);

        // parse mode, 1 means reading from file, 2 from Twitter API
        int mode = Integer.valueOf(args[0]);
        if(mode != 1 && mode != 2) {
            System.out.println("Incorrect first parameter: mode. Please try again!");
            return;
        }

        twitterAPIParams.put("mode", Integer.toString(mode));
        // TODO, comment after finishing development
        System.out.println("mode: " + mode);

        // parse Twitter API key
        String twitterAPIKey = args[1];
        if(twitterAPIKey.equals("null")) {
            twitterAPIKey = "l3NSaKh0ELLHbgnNYoDzII8GR";
        }

        twitterAPIParams.put("twitterAPIKey", twitterAPIKey);
        // TODO, comment after finishing development
        System.out.println("Twitter API key: " + twitterAPIKey);

        // parse secret associated with the Twitter app consumer
        String APISecret = args[2];
        if(APISecret.equals("null")) {
            APISecret = "NS0ePZrk5vMRfwX1x3YobJEaWb7KgVeWUO0WL1Xl1MNmYAh9c7";
        }

        twitterAPIParams.put("APISecret", APISecret);
        // TODO, comment after finishing development
        System.out.println("Twitter API Secret key: " + APISecret);

        // parse access token associated with the Twitter app
        String tokenValue = args[3];
        if(tokenValue.equals("null")) {
            tokenValue = "783660909268963329-UqRnX33mwnMmSJodGsyqq2WsC4OIAEo";
        }

        twitterAPIParams.put("tokenValue", tokenValue);
        // TODO, comment after finishing development
        System.out.println("Access token: " + tokenValue);

        // parse access token secret
        String tokenSecret = args[4];
        if(tokenSecret.equals("null")) {
            tokenSecret = "VfKpxmoQKiIbGy8sTpcC9mJKueXzuQdVT3fLJBAUZdLxX";
        }

        twitterAPIParams.put("tokenSecret", tokenSecret);
        // TODO, comment after finishing development
        System.out.println("Access token secret: " + tokenSecret);

        // parse Kafka Broker URL: String in the format IP:port corresponding with the Kafka Broker
        String kafkaBrokerURL = args[5];

        if(kafkaBrokerURL.equals("null")) {
            kafkaBrokerURL = "localhost:9092";
        }

        twitterAPIParams.put("kafkaBrokerURL", kafkaBrokerURL);
        // TODO, comment after finishing development
        System.out.println("Kafka Broker URL: " + kafkaBrokerURL);

        // parse Filename, path to the file with the tweets
        // the path is relative to the filesystem of the node that will be used to run the Twitter app
        String fileName = args[6];

        twitterAPIParams.put("fileName", fileName);
        // TODO, comment after finishing development
        System.out.println("File name: " + fileName);

        StartTwitterApp selfInstance = new StartTwitterApp();
        selfInstance.getTweetsAndStoreToKafka(twitterAPIParams);

    }
}
