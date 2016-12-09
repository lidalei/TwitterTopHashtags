package master2016;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
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
        HashMap<String, String> langTokenDict = new HashMap<>(langTokenList.length);
        for(String e : langTokenList) {
            String[] langToken = e.split(":");
            langTokenDict.put(langToken[0], langToken[1]);
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

        String kafkaBrokerURL = "localhost:9092";
        String topologyName = "Topology";

        // build topology
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("KafkaSpout", new KafkaSpout(kafkaBrokerURL, "YesWeCan"));
        topologyBuilder.setBolt("Top3Bolt", new TwitterTop3Bolt()).localOrShuffleGrouping("KafkaSpout", KafkaSpout.TWITTER_STREAM_NAME);

        // local model
        LocalCluster locClu = new LocalCluster();
        locClu.submitTopology(topologyName, new Config(), topologyBuilder.createTopology());

        Utils.sleep(10000);

        locClu.killTopology(topologyName);
        locClu.shutdown();



    }

}
