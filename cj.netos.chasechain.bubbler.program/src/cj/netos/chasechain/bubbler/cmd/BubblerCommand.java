package cj.netos.chasechain.bubbler.cmd;

import cj.netos.chasechain.bubbler.ITrafficPoolService;
import cj.netos.chasechain.bubbler.TrafficPool;
import cj.netos.rabbitmq.CjConsumer;
import cj.netos.rabbitmq.IRabbitMQProducer;
import cj.netos.rabbitmq.RabbitMQException;
import cj.netos.rabbitmq.RetryCommandException;
import cj.netos.rabbitmq.consumer.IConsumerCommand;
import cj.studio.ecm.CJSystem;
import cj.studio.ecm.annotation.CjService;
import cj.studio.ecm.annotation.CjServiceRef;
import cj.studio.ecm.net.CircuitException;
import cj.ultimate.util.StringUtil;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@CjConsumer(name = "bubbler")
@CjService(name = "/bubbler.mq#doBubbler")
public class BubblerCommand implements IConsumerCommand {
    @CjServiceRef(refByName = "defaultTrafficPoolService")
    ITrafficPoolService trafficPoolService;

    @CjServiceRef(refByName = "@.rabbitmq.producer.bubbler")
    IRabbitMQProducer rabbitMQProducer;

    @Override
    public void command(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws RabbitMQException, RetryCommandException, IOException {
        Map<String, Object> headers = properties.getHeaders();
        String poolId = headers.get("traffic-pool").toString();
        TrafficPool trafficPool = trafficPoolService.getTrafficPool(poolId);
        if (trafficPool == null) {
            CJSystem.logging().warn(getClass(), String.format("不存在流量池:%s，已丢弃该消息", poolId));
            return;
        }
        if (StringUtil.isEmpty(trafficPool.getParent())) {
            CJSystem.logging().warn(getClass(), String.format("已冒泡到顶级池:%s[%s]", trafficPool.getTitle(), trafficPool.getId()));
            return;
        }
        TrafficPool parentPool = null;
        try {
            parentPool = trafficPoolService.doBubble(poolId, trafficPool.getParent());
        } catch (CircuitException e) {
            CJSystem.logging().error(getClass(),e);
            throw new RabbitMQException(e);
        }
        CJSystem.logging().warn(getClass(), String.format("一次冒泡完成: %s[%s]->%s[%s]", trafficPool.getTitle(), trafficPool.getId(), parentPool.getTitle(), parentPool.getId()));
        //提交父池给队列，消费者会抽取父池，其实还是会进入本类执行，类似于递归，直到父为空
        try {
            commitNextPool(trafficPool.getParent());
        } catch (CircuitException e) {
            CJSystem.logging().warn(getClass(), String.format("提交下一流量池的冒泡指令失败: %s", e));
        }
    }

    private void commitNextPool(String pool) throws CircuitException {
        //该指令与motor项目一致
        AMQP.BasicProperties props = new AMQP.BasicProperties().builder()
                .type("/bubbler.mq")
                .headers(new HashMap<String, Object>() {{
                    put("command", "doBubbler");
                    put("traffic-pool", pool);
                }})
                .build();

        rabbitMQProducer.publish("bubbler", props, new byte[0]);
        CJSystem.logging().info(getClass(), String.format("发现流量池:%s", pool));
    }
}
