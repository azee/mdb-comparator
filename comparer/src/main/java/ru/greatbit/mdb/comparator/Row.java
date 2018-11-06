package ru.greatbit.mdb.comparator;

import java.util.Map;
import java.util.TreeMap;

public class Row {
    private final Map<String, Object> data = new TreeMap<>();
    private final String sortFieldKey;
    private final Object sortFieldValue;

    public Row(Map<String, Object> data) {
        this.data.putAll(data);
        this.sortFieldKey = data.entrySet().stream().
                filter(entry -> entry instanceof Number).
                map(Map.Entry::getKey).
                findFirst().orElse(
                    data.keySet().stream().findFirst().orElse("")
                );
        this.sortFieldValue = data.getOrDefault(this.sortFieldKey, "");
    }

    public Map<String, Object> getData() {
        return data;
    }

    public String getSortFieldKey() {
        return sortFieldKey;
    }

    public Object getSortFieldValue() {
        return sortFieldValue;
    }
}
