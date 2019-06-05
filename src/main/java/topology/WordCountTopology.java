package topology;

import bolt.WordCounterBolt;
import bolt.WordNormalizerBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import spout.FileReaderSpout;

public class WordCountTopology {
    public static void main(String[] args) throws InterruptedException {

        //Topology definition
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader", new FileReaderSpout());
        builder.setBolt("word-normalizer", new WordNormalizerBolt())
                .shuffleGrouping("word-reader");
        builder.setBolt("word-counter", new WordCounterBolt(),2)
                .fieldsGrouping("word-normalizer", new Fields("word"));

        //Configuration
        Config config = new Config();
        config.setDebug(true);
        config.put("fileToRead", "C:\\bigdata\\eventsDataModified.txt");
        config.put("dirToWrite", "C:\\bigdata\\stormOutput\\");

        //Topology run
        LocalCluster cluster = new LocalCluster();

        try{
            cluster.submitTopology("WordCounter-Topology", config, builder.createTopology());
            Thread.sleep(30*1000);
        }
        finally {
            cluster.shutdown();
        }
    }
}
