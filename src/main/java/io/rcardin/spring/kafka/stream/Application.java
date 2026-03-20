package io.rcardin.spring.kafka.stream;

import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.Arrays;

@SpringBootApplication
public class Application {

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

	@Bean
	public Queue wordsQueue() {
		return new Queue("words", true);
	}

	@Bean
	public Queue wordCountersQueue() {
		return new Queue("word-counters", true);
	}

	@RabbitListener(queuesToDeclare = @org.springframework.amqp.rabbit.annotation.Queue(name = "words", durable = "true"))
	public void processWord(String word) {
		String[] words = word.split(" ");
		for (String w : words) {
			sendWordCounter(w);
		}
	}

	private void sendWordCounter(String word) {
		// Simulate counting logic
		Long count = 1L;
		rabbitTemplate().convertAndSend("word-counters", word + ":" + count);
	}

	@Bean
	public RabbitTemplate rabbitTemplate() {
		return new RabbitTemplate();
	}
}