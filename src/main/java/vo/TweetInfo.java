package vo;

import java.io.Serializable;

public class TweetInfo implements Serializable{
	private String tweetId;
	public String getTweetId() {
		return tweetId;
	}
	public void setTweetId(String tweetId) {
		this.tweetId = tweetId;
	}
	public String getDateString() {
		return dateString;
	}
	public void setDateString(String dateString) {
		this.dateString = dateString;
	}
	public String getUserName() {
		return userName;
	}
	public void setUserName(String userName) {
		this.userName = userName;
	}
	public String getTweet() {
		return tweet;
	}
	public void setTweet(String tweet) {
		this.tweet = tweet;
	}
	public String getMentionMen() {
		return mentionMen;
	}
	public void setMentionMen(String mentionMen) {
		this.mentionMen = mentionMen;
	}
	private String dateString;
	private String userName;
	private String tweet;
	private String mentionMen;
	private String userInteraction;
	public String getUserInteraction() {
		return userInteraction;
	}
	public void setUserInteraction(String userInteraction) {
		this.userInteraction = userInteraction;
	}
}
