package topology;

import bolt.PlusTenBolt;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;

import java.util.ArrayList;
import java.util.List;

public class RemoteDRPC {

    public static void main(String[] st) throws Exception {
        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("drpc-PlusTen");
        builder.addBolt(new PlusTenBolt());

        Config conf = new Config();
        List<String> drpcServers = new ArrayList<String>();

        drpcServers.add("localhost");

        conf.put(Config.DRPC_SERVERS, drpcServers);
        conf.put(Config.DRPC_PORT, 3772);

        StormSubmitter.submitTopology("drpc-PlusTen", conf, builder.createRemoteTopology());
    }
}
