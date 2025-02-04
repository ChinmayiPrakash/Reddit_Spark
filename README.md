# Reddit Sentiment Analysis

This project is about Sentiment Analysis of a desired Reddit topic using Apache Spark Structured Streaming, Apache Kafka, Python, and the AFINN Module. You can learn the sentiment status of a topic of interest.

For example, you might be curious about opinions on a particular Reddit post or thread, such as discussions surrounding a new "Game of Thrones" episode. The sentiment can be categorized as **NEGATIVE**, **NEUTRAL**, or **POSITIVE** based on user opinions.

## Code Explanation

1. **Authentication** operations are completed using the **PRAW (Python Reddit API Wrapper)** module in Python. You must create a Reddit API account to obtain your client ID, secret, and user agent.
2. **StreamListener** (now called **RedditListener**) was created for Reddit Streaming. RedditListener produces data for a Kafka Topic named `reddit`.
3. The **StreamListener** also calculates the sentiment value of posts using the **AFINN** module and sends this value to the `reddit` Kafka topic.
4. The data is **filtered** to include only the desired Reddit topic or subreddit.
5. A **Kafka Consumer** is created to consume data from the `reddit` topic.
6. The data is converted into **structured data** and placed into an SQL table named `data`.
7. The data table contains two columns: 
   - `text`: The Reddit post
   - `senti_val`: The sentiment value
8. The **average sentiment** value in the `senti_val` column is calculated using `pyspark.sql.functions`.
9. A **user-defined function (UDF)** named `fun` is created for the **status column**.
10. The **status column** has values: **POSITIVE**, **NEUTRAL**, or **NEGATIVE**, which change based on the real-time average sentiment (`avg(senti_val)`).
