package vo;

import java.io.Serializable;

/**
 * Created by user on 2016/11/20.
 */
public class schemaTFIDF implements Serializable{
    public String getTweetId() {
        return tweetId;
    }

    public void setTweetId(String tweetId) {
        this.tweetId = tweetId;
    }

    private String tweetId;

    public String getTweet() {
        return tweet;
    }

    public void setTweet(String tweet) {
        this.tweet = tweet;
    }

    private String tweet;
}
