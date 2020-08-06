package cj.netos.chasechain.bubbler.service;

import cj.lns.chip.sos.cube.framework.ICube;
import cj.lns.chip.sos.cube.framework.IDocument;
import cj.lns.chip.sos.cube.framework.IQuery;
import cj.lns.chip.sos.cube.framework.TupleDocument;
import cj.netos.chasechain.bubbler.AbstractService;
import cj.netos.chasechain.bubbler.ITrafficPersonService;
import cj.netos.chasechain.bubbler.TrafficPerson;
import cj.studio.ecm.annotation.CjService;
import cj.studio.ecm.net.CircuitException;

import java.util.ArrayList;
import java.util.List;

@CjService(name = "defaultTrafficPersonService")
public class DefaultTrafficPersonService extends AbstractService implements ITrafficPersonService {
    @Override
    public void addTrafficPerson(String trafficPool, String person) throws CircuitException {
        ICube cube = cube(trafficPool);
        if (existsPerson(trafficPool, person)) {
            return;
        }
        TrafficPerson trafficPerson = new TrafficPerson();
        trafficPerson.setCtime(System.currentTimeMillis());
        trafficPerson.setLiveness(0L);
        trafficPerson.setPerson(person);
        cube.saveDoc(TrafficPerson._COL_NAME, new TupleDocument<>(trafficPerson));
    }

    @Override
    public boolean existsPerson(String trafficPool, String person) throws CircuitException {
        ICube cube = cube(trafficPool);
        return cube.tupleCount(TrafficPerson._COL_NAME, String.format("{'tuple.person':'%s'}", person)) > 0;
    }

    @Override
    public void removeTrafficPerson(String trafficPool, String person) throws CircuitException {
        ICube cube = cube(trafficPool);
        cube.deleteDocOne(TrafficPerson._COL_NAME, String.format("{'tuple.person':'%s'}", person));
    }

    @Override
    public List<String> pageTrafficPerson(String trafficPool, int limit, long offset) throws CircuitException {
        ICube cube = cube(trafficPool);
        String cjql = String.format("select {'tuple':'*'}.limit(%s).skip(%s) from tuple %s %s where {}",
                limit, offset, TrafficPerson._COL_NAME, TrafficPerson.class.getName());
        IQuery<TrafficPerson> query = cube.createQuery(cjql);
        List<IDocument<TrafficPerson>> list = query.getResultList();
        List<String> persons = new ArrayList<>();
        for (IDocument<TrafficPerson> document : list) {
            persons.add(document.tuple().getPerson());
        }
        return persons;
    }

}
