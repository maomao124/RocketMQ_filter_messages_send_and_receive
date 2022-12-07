package mao.producer;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;

/**
 * Project name(项目名称)：RocketMQ_过滤消息的发送与接收
 * Package(包名): mao.producer
 * Class(类名): FilterProducer1
 * Author(作者）: mao
 * Author QQ：1296193245
 * GitHub：https://github.com/maomao124/
 * Date(创建日期)： 2022/12/7
 * Time(创建时间)： 14:16
 * Version(版本): 1.0
 * Description(描述)： 生产者
 */

public class FilterProducer1
{
    /**
     * 标签数组
     */
    private static final String[] TAGS = {"tag1", "tag2", "tag3", "tag4", "tag5"};


    public static void main(String[] args)
            throws MQClientException, MQBrokerException, RemotingException, InterruptedException
    {
        //生产者
        DefaultMQProducer defaultMQProducer = new DefaultMQProducer("mao_group");
        //nameserver地址
        defaultMQProducer.setNamesrvAddr("127.0.0.1:9876");
        //启动
        defaultMQProducer.start();
        //发送消息
        for (int i = 0; i < 200; i++)
        {
            //标签
            String tag = TAGS[i % (TAGS.length)];
            //消息体字符串
            String msg = "消息" + i + "     消息标签：" + tag;
            //消息对象
            Message message = new Message("test_topic", tag, msg.getBytes(StandardCharsets.UTF_8));
            //打印
            System.out.println(msg);
            //发送消息
            defaultMQProducer.send(message);
        }
        //关闭
        defaultMQProducer.shutdown();
    }
}
