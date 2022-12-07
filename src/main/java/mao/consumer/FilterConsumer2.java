package mao.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Project name(项目名称)：RocketMQ_过滤消息的发送与接收
 * Package(包名): mao.consumer
 * Class(类名): FilterConsumer2
 * Author(作者）: mao
 * Author QQ：1296193245
 * GitHub：https://github.com/maomao124/
 * Date(创建日期)： 2022/12/7
 * Time(创建时间)： 14:32
 * Version(版本): 1.0
 * Description(描述)： 消费者2，只消费标签为5的消息
 */

public class FilterConsumer2
{
    public static void main(String[] args) throws MQClientException
    {
        //消费者
        DefaultMQPushConsumer defaultMQPushConsumer = new DefaultMQPushConsumer("mao_group");
        //设置nameserver地址
        defaultMQPushConsumer.setNamesrvAddr("127.0.0.1:9876");
        //设置消费模式-广播模式
        defaultMQPushConsumer.setMessageModel(MessageModel.BROADCASTING);
        //订阅
        defaultMQPushConsumer.subscribe("test_topic", "tag5");
        //注册监听器
        defaultMQPushConsumer.registerMessageListener(new MessageListenerConcurrently()
        {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext)
            {
                for (MessageExt messageExt : list)
                {
                    System.out.println(new String(messageExt.getBody(), StandardCharsets.UTF_8));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        //启动
        defaultMQPushConsumer.start();
    }
}
