package topology;

import bolt.TwitterStatusBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import spout.TwitterSpout;

public class TwitterStatusTopology {

    public static void main(String[] st) throws InterruptedException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("tweeter-collector", new TwitterSpout());
        builder.setBolt("text-extractor", new TwitterStatusBolt()).shuffleGrouping("tweets-collector");

        LocalCluster cluster = new LocalCluster();
        Config config = new Config();
        config.setDebug(true);

        cluster.submitTopology("twitter-text", config, builder.createTopology());
        Thread.sleep(60*1000);
        cluster.shutdown();
    }
}
