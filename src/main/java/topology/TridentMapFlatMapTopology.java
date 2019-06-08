package topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.trident.TridentTopology;
import tridentFunction.LowerCaseFunction;
import tridentFunction.SplitFunction;

public class TridentMapFlatMapTopology {

    public static void main(String[] st) {
        LocalDRPC drpc = new LocalDRPC();
        TridentTopology topology = new TridentTopology();
        topology.newDRPCStream("simple",drpc)
                .map(new LowerCaseFunction())
                .flatMap(new SplitFunction());

        Config conf = new Config();
        conf.setDebug(true);


        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("trident-topology", conf, topology.build());

        for (String word : new String[]{ "First Page" , "Second Line", "Third Word in the Book"}) {
            System.out.println("Result for " + word + ": " + drpc.execute("simple", word));
        }
        cluster.shutdown();
    }
}
