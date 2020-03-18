package com.example;

import com.example.listener.MyMessageListener;
import com.example.model.Greeting;
import com.example.producer.MyMessageProducer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;

import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class KafkaListenerApplication {

	/*public static void main(String[] args) {
		SpringApplication.run(KafkaListenerApplication.class, args);
	}
*/
	public static void main(String[] args) throws Exception {

		ConfigurableApplicationContext context = SpringApplication.run(KafkaListenerApplication.class, args);

		MyMessageProducer producer = context.getBean(MyMessageProducer.class);
		MyMessageListener listener = context.getBean(MyMessageListener.class);
		/*
		 * Sending a Hello World message to topic 'Load'.
		 * Must be recieved by both listeners with group foo
		 * and bar with containerFactory fooKafkaListenerContainerFactory
		 * and barKafkaListenerContainerFactory respectively.
		 * It will also be recieved by the listener with
		 * headersKafkaListenerContainerFactory as container factory
		 */
		producer.sendMessage("Hello, World!");
		listener.latch.await(10, TimeUnit.SECONDS);

		/*
		 * Sending message to a topic with 5 partition,
		 * each message to a different partition. But as per
		 * listener configuration, only the messages from
		 * partition 0 and 3 will be consumed.
		 */
		for (int i = 0; i < 5; i++) {
			producer.sendMessageToPartion("Hello To Partioned Topic!", i);
		}
		listener.partitionLatch.await(10, TimeUnit.SECONDS);

		/*
		 * Sending message to 'filtered' topic. As per listener
		 * configuration,  all messages with char sequence
		 * 'World' will be discarded.
		 */
		producer.sendMessageToFiltered("Hello Baeldung!");
		producer.sendMessageToFiltered("Hello World!");
		listener.filterLatch.await(10, TimeUnit.SECONDS);

		/*
		 * Sending message to 'greeting' topic. This will send
		 * and recieved a java object with the help of
		 * greetingKafkaListenerContainerFactory.
		 */
		producer.sendGreetingMessage(new Greeting("Greetings", "World!"));
		listener.greetingLatch.await(10, TimeUnit.SECONDS);

		context.close();
	}

	@Bean
	public MyMessageProducer messageProducer() {
		return new MyMessageProducer();
	}

	@Bean
	public MyMessageListener messageListener() {
		return new MyMessageListener();
	}
}
