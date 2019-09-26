package harsh.rane;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;

public class KafkaMain {

	public static void main(String[] args) throws InterruptedException {
		
		Properties streamsConfiguration = new Properties();
		streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "KafkaMain");
		streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		
		
		
		StreamsBuilder builder = new StreamsBuilder();  // building Kafka Streams Model
		//String inputTopic1 = "topic2";
       // KStream<String, String> textLines2 = builder.stream("topic2");
       // KStream<String, String> textLines3 = builder.stream("topic3");//source of streaming is available on topic2 
        /*KTable<String, Long> wordCounts = textLines.flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
        		  .groupBy((key, word) -> word)
        		  .count();*/
       // KStream<String, String> wordCounts = textLines2.leftJoin(textLines3);
        KStream<String, String> text2 = builder.stream( "topic2");
        KStream<String, String> text3 = builder.stream( "topic3");	 
        
        KTable<String, Long> wordCounts1 = text2.flatMapValues(value -> Arrays.asList(value.toLowerCase().split(" ")))
      		  .groupBy((key, word) -> word)
      		  .count();
        
       
       // wordCounts.to(outputTopic);
        aggre.toStream().to(outputTopic,Produced.with(Serdes.String(), Serdes.String())); //sending modified stream to output topic
        
        KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration); //Starting kafka stream
        streams.start();
	}

}
