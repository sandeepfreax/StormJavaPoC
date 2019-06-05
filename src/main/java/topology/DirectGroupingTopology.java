package topology;

import bolt.WriteIntToBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import spout.DirectGroupingSpout;
import spout.IntegerGeneratorSpout;

public class DirectGroupingTopology {
    public static void main(String[] st) throws InterruptedException {
        //Topology definition
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("Integer-Generator-Spout", new DirectGroupingSpout());
        builder.setBolt("Int-To-Bucket-Bolt", new WriteIntToBolt(), 2).directGrouping("Integer-Generator-Spout");

        //Configuration
        Config config = new Config();
        config.setDebug(true);
        config.put("dirToWrite", "D:\\StormOutputFiles\\");

        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology("Int-Bucket-Writer-Topology", config, builder.createTopology());
            Thread.sleep(1000*20);
        } finally {
            cluster.shutdown();
        }
    }
}
