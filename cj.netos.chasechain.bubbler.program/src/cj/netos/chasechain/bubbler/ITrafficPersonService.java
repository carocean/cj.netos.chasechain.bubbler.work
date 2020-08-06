package cj.netos.chasechain.bubbler;

import cj.studio.ecm.net.CircuitException;

import java.util.List;

public interface ITrafficPersonService {

    void addTrafficPerson(String trafficPool, String person) throws CircuitException;


    boolean existsPerson(String trafficPool, String person) throws CircuitException;

    void removeTrafficPerson(String trafficPool, String person) throws CircuitException;

    List<String> pageTrafficPerson(String trafficPool, int limit, long offset) throws CircuitException;

}
