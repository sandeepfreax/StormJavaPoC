package topology;

import bolt.FilterFieldBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import spout.FileReaderColSpout;

public class FilterFieldsTopology {
    public static void main(String[] st) throws InterruptedException {
        //Topology definition
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("File-Reader-Spout", new FileReaderColSpout());
        builder.setBolt("Filter-Bolt", new FilterFieldBolt()).shuffleGrouping("File-Reader-Spout");

        //Configuration
        Config config = new Config();
        config.setDebug(true);
        config.put("fileToRead", "C:\\bigdata\\eventsDataModified.txt");

        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology("File-Reader-Topology", config, builder.createTopology());
            Thread.sleep(1000*20);
        } finally {
            cluster.shutdown();
        }
    }
}
