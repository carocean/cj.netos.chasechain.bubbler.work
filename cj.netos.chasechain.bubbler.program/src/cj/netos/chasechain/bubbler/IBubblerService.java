package cj.netos.chasechain.bubbler;

import cj.studio.ecm.net.CircuitException;

public interface IBubblerService {

    void bubble(TrafficDashboardPointer pointer, TrafficPool sourcePool, TrafficPool parentPool) throws CircuitException;

}
