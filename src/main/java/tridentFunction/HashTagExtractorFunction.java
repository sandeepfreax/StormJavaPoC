package tridentFunction;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
import twitter4j.HashtagEntity;
import twitter4j.Status;

public class HashTagExtractorFunction extends BaseFunction {

    public void execute(TridentTuple tuple, TridentCollector collector) {
        //Get the tweet
        final Status status = (Status) tuple.get(0);
        //Loop through the hashtags
        for (HashtagEntity hashtag : status.getHashtagEntities()) {
            //Emit each hashtag
            collector.emit(new Values(hashtag.getText()));
        }
    }
}
