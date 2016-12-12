package master2016;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import java.util.HashMap;

/**
 * Created by Sophie on 11/17/16.
 */
public class Top3App {

    public static void main(String[] args) {

        /*
        if(args.length < 4) {
            System.out.println("Not enough parameters. There should be four parameters.");
            return;
        }


        System.out.println("Parameters:");

        // parse langList
        String langTokenListStr = args[0];
        String[] langTokenList = langTokenListStr.split(",");
        HashMap<String, String> langTokenDict = new HashMap<>(langTokenList.length * 2);
        for(String e : langTokenList) {
            String[] langToken = e.split(":");
            langTokenDict.put(langToken[0], langToken[1].toLowerCase());
        }

        System.out.println("langList: " + langTokenDict.toString());

        // parse Kafka Broker URL
        String kafkaBrokerURL = args[1];

        System.out.println("Kafka Broker URL: " + kafkaBrokerURL);

        // parse topology name
        String topologyName = args[2];

        System.out.println("topologyName: " + topologyName);

        // parse outputFolder
        String outputFolder = args[3];

        System.out.println("Output folder: " + outputFolder);
        */


        // for debug use only. // TODO, change to previous piece of code
        HashMap<String, String> langTokenDict = new HashMap<>(4);
        langTokenDict.put("en", "house");
        langTokenDict.put("es", "ordenador");

        String kafkaBrokerURL = "localhost:9092";
        String topologyName = "Topology";
        String outputFolder = "/Users/Sophie/";


        final String groupID = "YesWeCan";

        // build topology
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout("KafkaSpout", new KafkaSpout(kafkaBrokerURL, groupID));
        topologyBuilder.setBolt("Top3Bolt", new TwitterTopKBolt(langTokenDict, outputFolder, 3))
                .fieldsGrouping("KafkaSpout", KafkaSpout.TWITTER_STREAM_NAME, new Fields(KafkaSpout.LANGUAGE_NAME));

        // local model
        LocalCluster locClu = new LocalCluster();
        locClu.submitTopology(topologyName, new Config(), topologyBuilder.createTopology());

        Utils.sleep(100000);

        locClu.killTopology(topologyName);
        locClu.shutdown();

    }

}
