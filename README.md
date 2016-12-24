# masterApp
Find top 3 frequent hashtags between two occurrences of a special hashtag per language. This project was developed at Technical University of Madrid (UPM) as a course project of Large Scale Data Management.

# Tools
Kafka and Storm, hbc-twitter and gson.

# Kafka
* Used to store hashtags of tweets, one topic "DataManagement" with many partitions.
* Each record is associated with a key and value, where key is language and value is the hashtags separated by :. For example, <en, EIT:Digital:Master:School>. Thus, the order of hashtags with the same language is kept.

# Topology
* KafkaSpout is used to consumer the topic "DataManagement". The number of executors are set as the number of languages to be watched. Hence, each instance of this Spout will connect to more than one partition of the topic in Kafka. This Spout will emit tuples in the form of <language, hashtags>. To be mentioned, only languages to be watched will be emitted.

* TwitterTopKBolt is used to form the conditional windows (not consecutive but easy to change to consecutive) and to compute the top k hashtags within the window. This Bolt can also be parallelized. Moreover, tuples with the same language emitted from KafkaSpout will be dealt with at one instance of this Bolt. Further, this Bolt will emit the top k hashtags to the next Bolt.

* WriteTopKHashtagsBolt is used to receive top k hashtags and write them to files. This Bolt can only have one instance and is connected with TwitterTopKBolt by globalGrouping.

# StreamTopK
Used to store all hashtags within a conditional window and return top k.

# Misc
kafkaTopics.sh must be invoked in the folder of kafka/bin/ and the --zookeeper should be changed.
