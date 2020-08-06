package cj.netos.chasechain.bubbler;

import cj.studio.ecm.net.CircuitException;

public interface ITrafficPoolService {

    TrafficPool getTrafficPool(String trafficPool);

    TrafficPool doBubble(String poolId, String parent) throws CircuitException;

}
