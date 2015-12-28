package com.zto.rocketmqtest;

import java.util.List;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;

public class PushConsumer {
	/**
	 * 当前例子是PushConsumer用法，使用方式给用户感觉是消息时
	 * 
	 * @throws MQClientException
	 */
	public static void main(String[] args) throws MQClientException {
		DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(
				"ConsumerGroupNmae");
		consumer.setNamesrvAddr("10.10.19.14:9876");
		consumer.setInstanceName("Consumer");

		consumer.subscribe("TopicTest1", "TagA || TagC || TagD ");
		consumer.subscribe("TopicTest2", "*");

		consumer.registerMessageListener(new MessageListenerConcurrently() {

			public ConsumeConcurrentlyStatus consumeMessage(
					List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
				// TODO Auto-generated method stub
				System.out.println(Thread.currentThread().getName()
						+ "  Receive New Message :" + msgs.size());

				MessageExt msg = msgs.get(0);
				if (msg.getTopic().equals("TopicTest1")) {
					if (msg.getTags() != null && msg.getTags().equals("TagA")) {
						System.out.println(new String(msg.getBody()));
					} else if (msg.getTags() != null
							&& msg.getTags().equals("TagC")) {
						System.out.println(new String(msg.getBody()));
					} else if (msg.getTags() != null
							&& msg.getTags().equals("TagD")) {
						System.out.println(new String(msg.getBody()));
					}

				} else if (msg.getTags().equals("TopicTest2")) {
					System.out.println(new String(msg.getBody()));
				}

				return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
			}
		});
		consumer.start();
		System.out.println("ConsumerStarted");
	}
}
