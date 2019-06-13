package topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import tridentFunction.LowerCaseFunction;
import tridentFunction.SplitFunction;

public class PersistentAggregateTridentTopology {
    public static void main(String[] args) throws Exception {

        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3,
                new Values("the cow jumped over the moon and the man"),
                new Values("the man is a great cow"),
                new Values("the cow will come home")
        );
        spout.setCycle(true);
        LocalDRPC drpc = new LocalDRPC();
        TridentTopology topology = new TridentTopology();

        TridentState wordCounts = topology.newStream("spout1", spout)
                .map(new LowerCaseFunction())
                .flatMap(new SplitFunction())
                .groupBy(new Fields("sentence"))
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"));

        topology.newDRPCStream("words", drpc).stateQuery(wordCounts, new Fields("args"), new MapGet(), new Fields("count"));

        Config conf = new Config();
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        try{
            cluster.submitTopology("trident-topology", conf, topology.build());
            Thread.sleep(20000);
            for (String word : new String[]{ "cow" ,"dog", "man"}) {
                System.out.println("Result for " + word + ": " + drpc.execute("words", word));
            }
        }
        finally {
            cluster.shutdown();
        }
    }

}
