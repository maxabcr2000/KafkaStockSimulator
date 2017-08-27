package max.kafka.stockproducer;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaProducerServiceImpl implements KafkaProducerService {
	private static final Logger LOG = LoggerFactory.getLogger(KafkaProducerServiceImpl.class);
	private Producer<String, String> producer;

	public void initProducer() {
		try {
			Properties props = new Properties();
			props.put("bootstrap.servers", "localhost:9092");
			props.put("acks", "all");
			props.put("retries", 0);
			props.put("batch.size", 16384);
			props.put("linger.ms", 1);
			props.put("buffer.memory", 33554432);
			props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	
			producer = new KafkaProducer<String, String>(props);
		} catch(Exception e)	{
			LOG.error(e.getMessage(), e);
		}
	}

	public void send(String topic, String key, String value) {
		producer.send(new ProducerRecord<String, String>(topic, key, value));
	}
	
	public void send(String topic, String value) {
		LOG.info("Produce value: {}", value);
		producer.send(new ProducerRecord<String, String>(topic, value));
	}

	public void send(String topic, Map<String, String> messages) {
		for (Map.Entry<String, String> entry: messages.entrySet()) {
			producer.send(new ProducerRecord<String, String>(topic, entry.getKey(), entry.getValue()));
		}
	}

	public void stopProducer() {
		this.producer.close();
	}

}
