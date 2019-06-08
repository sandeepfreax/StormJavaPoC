package spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterSpout implements IRichSpout {

    private LinkedBlockingQueue<Status> queue;
    private TwitterStream twitterStream;
    private SpoutOutputCollector collector;

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;

        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.setDebugEnabled(true)
                .setOAuthConsumerKey("")
                .setOAuthConsumerSecret("")
                .setOAuthAccessToken("")
                .setOAuthAccessTokenSecret("");
        this.twitterStream = new TwitterStreamFactory(configurationBuilder.build()).getInstance();

        this.queue = new LinkedBlockingQueue<Status>();

        final StatusListener listener = new StatusListener() {
            public void onStatus(Status status) {
                queue.offer(status);
            }

            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

            }

            public void onTrackLimitationNotice(int i) {

            }

            public void onScrubGeo(long l, long l1) {

            }

            public void onStallWarning(StallWarning stallWarning) {

            }

            public void onException(Exception e) {

            }
        };

        twitterStream.addListener(listener);

        final FilterQuery query = new FilterQuery();
        query.track(new String[]{"chocolate"});
        twitterStream.filter(query);
    }

    public void nextTuple() {
        final Status status = queue.poll();
        if(status == null) {
            Utils.sleep(50);
        } else {
            collector.emit(new Values(status));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet"));
    }
    public void close() {
        twitterStream.shutdown();
    }

    public void activate() {}

    public void deactivate() {}

    public void ack(Object msgId) {}

    public void fail(Object msgId) {}

    public Map<String,Object> getComponentConfiguration() { return null;}
}
