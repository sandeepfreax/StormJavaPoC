package topology;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.trident.operation.builtin.FirstN;
import org.apache.storm.trident.spout.IBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.tuple.Fields;
import spout.TwitterTridentSpout;
import tridentFunction.HashTagExtractorFunction;

public class TwitterRemoteTopology {
    public static void main(String[] args) throws Exception {
        final Config conf = new Config();
        final IBatchSpout spout = new TwitterTridentSpout();
        conf.setDebug(true);

        final TridentTopology topology = new TridentTopology();

        topology.newStream("spout", spout)
                .each(new Fields("tweet"), new HashTagExtractorFunction(), new Fields("hashtag"))
                .groupBy(new Fields("hashtag"))
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
                .newValuesStream()
                .applyAssembly(new FirstN(10, "count"))
                .each(new Fields("hashtag", "count"), new Debug());
        //Build and return the topology
        StormSubmitter.submitTopology("hashtag-count-topology", conf, topology.build());
    }
}
