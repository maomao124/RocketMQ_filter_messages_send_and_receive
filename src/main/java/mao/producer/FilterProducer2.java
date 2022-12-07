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
 * Class(类名): FilterProducer2
 * Author(作者）: mao
 * Author QQ：1296193245
 * GitHub：https://github.com/maomao124/
 * Date(创建日期)： 2022/12/7
 * Time(创建时间)： 14:51
 * Version(版本): 1.0
 * Description(描述)： 无
 */

public class FilterProducer2
{
    /**
     * 得到int随机
     *
     * @param min 最小值
     * @param max 最大值
     * @return int
     */
    public static int getIntRandom(int min, int max)
    {
        if (min > max)
        {
            min = max;
        }
        return min + (int) (Math.random() * (max - min + 1));
    }

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
        for (int i = 0; i < 100; i++)
        {
            //属性
            int a = getIntRandom(0, 10);
            int b = getIntRandom(0, 20);
            //消息体字符串
            String msg = "消息" + i + "     属性a：" + a + "     属性b：" + b;
            //消息对象
            Message message = new Message("test_topic", msg.getBytes(StandardCharsets.UTF_8));
            //填充属性
            message.putUserProperty("a", String.valueOf(a));
            message.putUserProperty("b", String.valueOf(b));
            //打印
            System.out.println(msg);
            //发送消息
            defaultMQProducer.send(message);
        }
        //关闭
        defaultMQProducer.shutdown();
    }
}
