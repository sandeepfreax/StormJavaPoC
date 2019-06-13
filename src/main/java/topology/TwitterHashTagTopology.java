package topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.trident.operation.builtin.FirstN;
import org.apache.storm.trident.spout.IBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.tuple.Fields;
import spout.TwitterTridentSpout;
import tridentFunction.HashTagExtractorFunction;

public class TwitterHashTagTopology {

    public static void main(String[] args) throws Exception {
        final Config conf = new Config();
        final IBatchSpout spout = new TwitterTridentSpout();
        conf.setDebug(true);
        //If no args, assume we are testing locally

        final TridentTopology topology = new TridentTopology();
        //Define the topology:
        //1. spout reads tweets
        //2. HashtagExtractor emits hashtags pulled from tweets
        //3. hashtags are grouped
        //4. a count of each hashtag is created
        //5. each hashtag, and how many times it has occured
        //   is emitted.
        topology.newStream("spout", spout)
                .each(new Fields("tweet"), new HashTagExtractorFunction(), new Fields("hashtag"))
                .groupBy(new Fields("hashtag"))
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
                .newValuesStream()
                .applyAssembly(new FirstN(10, "count"))
                .each(new Fields("hashtag", "count"), new Debug());
        //Build and return the topology

        final LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("hashtag-count-topology", conf, topology.build());
        Thread.sleep(100000);
        cluster.shutdown();

    }
}
