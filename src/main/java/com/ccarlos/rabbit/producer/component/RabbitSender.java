package com.ccarlos.rabbit.producer.component;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.core.RabbitTemplate.ConfirmCallback;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.UUID;

/**
 * @description: RabbitMQ发送者
 * @author: ccarlos
 * @date: 2020/3/26 21:29
 */
@Component
public class RabbitSender {

    @Autowired
    private RabbitTemplate rabbitTemplate;

    final ConfirmCallback confirmCallback = new RabbitTemplate.ConfirmCallback() {

        /**
         * @description: 确认消息的回调监听接口，用于确认消息是否被broker所接收到
         * @author: ccarlos
         * @date: 2020/3/26 21:32
         * @param: correlationData 作为一个唯一的标识
         * @param: ack broker是否落盘成功
         * @param: cause 失败的一些异常信息
         * @return: void
         */
        @Override
        public void confirm(CorrelationData correlationData, boolean ack, String cause) {

        }
    };

    /**
     * @description: 对外发送消息的方法
     * @author: ccarlos
     * @date: 2020/3/26 21:35
     * @param: message 具体的消息内容
     * @param: properties 额外的附加属性
     * @return: void
     */
    public void send(Object message, Map<String, Object> properties) throws Exception {

        MessageHeaders mhs = new MessageHeaders(properties);
        Message<?> msg = MessageBuilder.createMessage(message, mhs);

        rabbitTemplate.setConfirmCallback(confirmCallback);

        // 指定业务唯一的id
        CorrelationData correlationData = new CorrelationData(UUID.randomUUID().toString());

        MessagePostProcessor mpp = new MessagePostProcessor() {

            @Override
            public org.springframework.amqp.core.Message postProcessMessage
                    (org.springframework.amqp.core.Message message) throws AmqpException {
                System.out.println("--> post to do: " + message);
                return message;
            }
        };

        rabbitTemplate.convertAndSend("exchange-1",
                "springboot.rabbit",
                msg, mpp, correlationData);
    }
}
