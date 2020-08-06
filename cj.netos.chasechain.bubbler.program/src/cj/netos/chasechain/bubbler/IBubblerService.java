package cj.netos.chasechain.bubbler;

import cj.studio.ecm.net.CircuitException;

public interface IBubblerService {

    long bubble(TrafficDashboardPointer pointer, TrafficPool sourcePool, TrafficPool parentPool) throws CircuitException;

}
