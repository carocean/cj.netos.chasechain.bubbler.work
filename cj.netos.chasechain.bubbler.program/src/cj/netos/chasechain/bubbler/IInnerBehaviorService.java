package cj.netos.chasechain.bubbler;

import cj.studio.ecm.net.CircuitException;

public interface IInnerBehaviorService {
    void putRedisItems(String pool,TrafficDashboardPointer trafficDashBoardPointer) throws CircuitException;

    ItemBehavior getBehavior(String pool, String item) throws CircuitException;

}
