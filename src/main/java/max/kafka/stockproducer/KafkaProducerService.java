package max.kafka.stockproducer;

import java.util.Map;

public interface KafkaProducerService {
	public void initProducer();
	public void send(String topic, String key, String value);
	public void send(String topic, String value);
	public void send(String topic, Map<String, String> messages);
	public void stopProducer();
}
