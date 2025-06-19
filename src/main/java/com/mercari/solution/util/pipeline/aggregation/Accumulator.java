package com.mercari.solution.util.pipeline.aggregation;

import java.util.*;

public class Accumulator {

    private Map<String,Object> map;
    private Map<String,Set<Object>> sets;

    public Map<String, Object> getMap() {
        return map;
    }

    public Map<String, Set<Object>> getSets() {
        return sets;
    }

    public void put(String name, Object value) {
        map.put(name, value);
    }

    public void add(String name, Object value) {
        if(!sets.containsKey(name)) {
            sets.put(name, new HashSet<>());
        }
        sets.get(name).add(value);
    }

    public void append(String name, Object value) {
        if(!map.containsKey(name)) {
            map.put(name, new ArrayList<>());
        }
        ((List) map.get(name)).add(value);
    }

    public Object get(String name) {
        return map.get(name);
    }

    public Set<Object> getSet(String name) {
        return Optional.ofNullable(sets.get(name)).orElseGet(HashSet::new);
    }

    public List<Object> getList(String name) {
        return switch (map.get(name)) {
            case List n -> n;
            case null, default -> new ArrayList<>();
        };
    }

    public Map<String,Object> getMap(String name) {
        return switch (map.get(name)) {
            case Map m -> m;
            case null, default -> new HashMap<>();
        };
    }

    public Number getAsNumber(String name) {
        return switch (map.get(name)) {
            case Number n -> n;
            case String s -> Double.parseDouble(s);
            case null, default -> null;
        };
    }

    public Double getAsDouble(String name) {
        return switch (map.get(name)) {
            case Number n -> n.doubleValue();
            case String s -> Double.parseDouble(s);
            case null, default -> null;
        };
    }


    public boolean isEmpty() {
        return map.isEmpty() && sets.isEmpty();
    }

    public static Accumulator of() {
        return of(new HashMap<>(), new HashMap<>());
    }

    public static Accumulator of(Map<String,Object> map, Map<String,Set<Object>> sets) {
        final Accumulator accumulator = new Accumulator();
        accumulator.map = map;
        accumulator.sets = sets;
        return accumulator;
    }

}
