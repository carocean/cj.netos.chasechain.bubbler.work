package cj.netos.chasechain.bubbler.service;

import cj.lns.chip.sos.cube.framework.ICube;
import cj.lns.chip.sos.cube.framework.IDocument;
import cj.lns.chip.sos.cube.framework.IQuery;
import cj.netos.chasechain.bubbler.*;
import cj.studio.ecm.CJSystem;
import cj.studio.ecm.IServiceSite;
import cj.studio.ecm.annotation.CjService;
import cj.studio.ecm.annotation.CjServiceRef;
import cj.studio.ecm.annotation.CjServiceSite;
import cj.studio.ecm.net.CircuitException;
import cj.ultimate.util.StringUtil;
import redis.clients.jedis.JedisCluster;

import java.math.BigInteger;

@CjService(name = "defaultTrafficPoolService")
public class DefaultTrafficPoolService implements ITrafficPoolService {
    @CjServiceRef(refByName = "mongodb.netos.home")
    ICube home;
    @CjServiceRef(refByName = "defaultBubblerService")
    IBubblerService bubblerService;
    @CjServiceRef(refByName = "defaultTrafficDashboardService")
    ITrafficDashboardService trafficDashboradService;
    @CjServiceRef(refByName = "defaultInnateBehaviorService")
    IInnateBehaviorService innateBehaviorService;
    @CjServiceRef(refByName = "defaultInnerBehaviorService")
    IInnerBehaviorService innerBehaviorService;
    @CjServiceRef(refByName = "@.redis.cluster")
    JedisCluster jedisCluster;
    @CjServiceSite
    IServiceSite site;
    @Override
    public TrafficPool getTrafficPool(String trafficPool) {
        String cjql = String.format("select {'tuple':'*'} from tuple %s %s where {'tuple.id':'%s'}", TrafficPool._COL_NAME, TrafficPool.class.getName(), trafficPool);
        IQuery<TrafficPool> query = home.createQuery(cjql);
        IDocument<TrafficPool> document = query.getSingleResult();
        if (document == null) {
            return null;
        }
        return document.tuple();
    }

    //返回父池
    @Override
    public TrafficPool doBubble(String poolId, String parent) throws CircuitException {
        TrafficPool sourcePool = getTrafficPool(poolId);
        TrafficPool parentPool = getTrafficPool(parent);
        //每次记录抽取位置，以内容物记录时间为记，并存到源池。下次抽取即从该时间开始
        //1.从先天行为和内部行为中将符合规则的取出，两集合求并集（借助redis)
        //2.按此并集（items)求item,并将每个item插入父（如果在父中不存在），同时将源池item的行为插入到父池item的先天行为里
        synchronized (poolId) {
            doBubbleImpl(sourcePool, parentPool);
            clearDashboardHistories(poolId);
        }
        return parentPool;
    }

    private void clearDashboardHistories(String poolId) throws CircuitException {
        //只保留最新100条
        String retainsStr = site.getProperty("traffic.dashboard.pointers.retains");
        int retains = StringUtil.isEmpty(retainsStr) ? 10 : Integer.valueOf(retainsStr);
        trafficDashboradService.clearPointersExceptTop(poolId,retains);
    }

    private void doBubbleImpl(TrafficPool sourcePool, TrafficPool parentPool) throws CircuitException {
        //(某内容物的某种行为总数/内容物总数)=某内容物的平均某种行为数
        //从上次抽取时间位置开始取，只要某内容物的某种行为数>内容物的平均某种行为数，则冒泡
        //流量时间窗表只有两条记录，时间早期的一条是bottom，最近的一条是top，top是不断被更新的，bottom就等着抽取时删除，删除后将立即插入新记录作为top,原top便变为bottom,
        //0为top,1为bottom
        TrafficDashboardPointer trafficDashBoardPointer = trafficDashboradService.getPointer(sourcePool.getId());
        long lastItemTime = 0;
        try {
            jedisCluster.del(Constants.set_bubbler_items_redis_key);
            innateBehaviorService.putRedisItems(sourcePool.getId(), trafficDashBoardPointer);
            innerBehaviorService.putRedisItems(sourcePool.getId(), trafficDashBoardPointer);
            lastItemTime = bubblerService.bubble(trafficDashBoardPointer, sourcePool, parentPool);
        } catch (Exception e) {
            CircuitException ce = CircuitException.search(e);
            if (ce != null) {
                throw ce;
            }
            throw new CircuitException("500", e);
        } finally {
            trafficDashboradService.movePointer(sourcePool.getId(), trafficDashBoardPointer, lastItemTime);
        }

    }
}
