package topology;

import bolt.PlusTenPythonBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import spout.IntegerSpoutPython;

public class MultilangTopology {

    public static void main(String[] args) throws InterruptedException {

        //Topology definition
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("My-First-Spout", new IntegerSpoutPython());
        builder.setBolt("My-First-Bolt", new PlusTenPythonBolt()).shuffleGrouping("My-First-Spout");

        //Configuration
        Config conf = new Config();
        conf.setDebug(true);

        //Topology run

        LocalCluster cluster = new LocalCluster();
        try{cluster.submitTopology("My-First-Topology", conf, builder.createTopology());
            Thread.sleep(30000);}
        finally{
            cluster.shutdown();}
    }
}
