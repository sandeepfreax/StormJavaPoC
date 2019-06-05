package topology;

import bolt.PlusTenBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;

public class BasicDRPC {

    public static void main(String[] st) throws Exception {
        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("PlusTenBolt");
        builder.addBolt(new PlusTenBolt(), 3);

        Config config = new Config();
        LocalDRPC localDRPC = new LocalDRPC();
        LocalCluster localCluster = new LocalCluster();

        localCluster.submitTopology("drpc-plusTen", config, builder.createLocalTopology(localDRPC));

        for (Integer number: new Integer[]{53, 62, 70}){
            System.out.println("result for : " + number + " ::: " + localDRPC.execute("PlusTenBolt", number.toString()));
        }

        Thread.sleep(10*1000);

        localCluster.shutdown();
        localDRPC.shutdown();
    }
}
