package io.rwa.server.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class CanonicalJsonService {

    private final ObjectMapper objectMapper;

    public CanonicalJsonService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper.copy();
        this.objectMapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
    }

    public String canonicalString(JsonNode node) {
        try {
            return objectMapper.writeValueAsString(sortNode(node == null ? NullNode.instance : node));
        } catch (Exception e) {
            throw new IllegalStateException("Failed to canonicalize JSON", e);
        }
    }

    public JsonNode sortNode(JsonNode node) {
        if (node == null || node.isNull()) {
            return NullNode.instance;
        }
        if (node.isObject()) {
            ObjectNode sorted = objectMapper.createObjectNode();
            List<String> fieldNames = new ArrayList<>();
            Iterator<String> iterator = node.fieldNames();
            while (iterator.hasNext()) {
                fieldNames.add(iterator.next());
            }
            Collections.sort(fieldNames);
            for (String field : fieldNames) {
                sorted.set(field, sortNode(node.get(field)));
            }
            return sorted;
        }
        if (node.isArray()) {
            ArrayNode arr = objectMapper.createArrayNode();
            for (JsonNode child : node) {
                arr.add(sortNode(child));
            }
            return arr;
        }
        return node;
    }

    public JsonNode mapToSortedNode(Map<String, ?> source) {
        return sortNode(objectMapper.valueToTree(source));
    }
}
