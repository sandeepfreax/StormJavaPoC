package topology;

import bolt.RandomFailureBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import spout.IntegerFailureSpout;

public class FailureTopology {

    public static void main(String[] st) throws InterruptedException {

        //Topoology definition
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("Integer-Spout", new IntegerFailureSpout());
        builder.setBolt("Random-Failure-Bolt", new RandomFailureBolt()).shuffleGrouping("Integer-Spout");

        //Configuration
        Config conf = new Config();
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology("Random-Fail-Topology", conf, builder.createTopology());
            Thread.sleep(60*1000);
        } finally {
            cluster.shutdown();
        }
    }
}
