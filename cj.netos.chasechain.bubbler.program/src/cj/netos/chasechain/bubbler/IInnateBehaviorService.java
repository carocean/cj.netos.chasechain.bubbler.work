package cj.netos.chasechain.bubbler;

import cj.studio.ecm.net.CircuitException;

public interface IInnateBehaviorService {
    void putRedisItems(String pool, TrafficDashboardPointer trafficDashBoardPointer) throws CircuitException;

    ItemBehavior getBehavior(String pool, String item) throws CircuitException;

    void addBehavior(String pool, ItemBehavior innateBehavior) throws CircuitException;

}
