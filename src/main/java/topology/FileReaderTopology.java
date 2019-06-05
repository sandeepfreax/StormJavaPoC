package topology;

import bolt.FileReaderAfterBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import spout.FileReaderSpout;

public class FileReaderTopology {
    public static void main(String[] st) throws InterruptedException {
        //Topology definition
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("File-Reader-Spout", new FileReaderSpout());
        builder.setBolt("Simple-Bolt", new FileReaderAfterBolt()).shuffleGrouping("File-Reader-Spout");

        //Configuration
        Config config = new Config();
        config.setDebug(true);
        config.put("fileToRead", "C:\\WLAN.log");

        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology("File-Reader-Topology", config, builder.createTopology());
            Thread.sleep(1000*20);
        } finally {
            cluster.shutdown();
        }
    }
}
