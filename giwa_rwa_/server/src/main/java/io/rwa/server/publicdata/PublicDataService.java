package io.rwa.server.publicdata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.rwa.server.common.ApiException;
import io.rwa.server.outbox.OutboxEventPublisher;
import jakarta.transaction.Transactional;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.web3j.crypto.Hash;

@Service
public class PublicDataService {

    public static final String CLASS_STATUS_IMPORTED = "IMPORTED";
    public static final String CLASS_STATUS_TOKENIZED = "TOKENIZED";

    private final ComplexRepository complexRepository;
    private final PropertyClassRepository classRepository;
    private final UnitRepository unitRepository;
    private final OutboxEventPublisher outboxEventPublisher;
    private final ObjectMapper objectMapper;

    public PublicDataService(
        ComplexRepository complexRepository,
        PropertyClassRepository classRepository,
        UnitRepository unitRepository,
        OutboxEventPublisher outboxEventPublisher,
        ObjectMapper objectMapper
    ) {
        this.complexRepository = complexRepository;
        this.classRepository = classRepository;
        this.unitRepository = unitRepository;
        this.outboxEventPublisher = outboxEventPublisher;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public Map<String, Object> importComplex(JsonNode rawItemJson) {
        String kaptCode = requiredText(rawItemJson, "kaptCode");
        int hoCnt = intValue(rawItemJson, "hoCnt");

        ComplexEntity complex = complexRepository.findById(kaptCode).orElseGet(ComplexEntity::new);
        complex.setKaptCode(kaptCode);
        complex.setKaptName(rawItemJson.path("kaptName").asText(null));
        complex.setKaptAddr(rawItemJson.path("kaptAddr").asText(null));
        complex.setDoroJuso(rawItemJson.path("doroJuso").asText(null));
        complex.setHoCnt(hoCnt);
        complex.setRawJson(rawItemJson.toString());
        complex.setFetchedAt(Instant.now());
        complexRepository.save(complex);

        Map<String, Integer> classCounts = new LinkedHashMap<>();
        putIfPositive(classCounts, "MPAREA_60", intValue(rawItemJson, "kaptMparea60"));
        putIfPositive(classCounts, "MPAREA_85", intValue(rawItemJson, "kaptMparea85"));
        putIfPositive(classCounts, "MPAREA_135", intValue(rawItemJson, "kaptMparea135"));
        putIfPositive(classCounts, "MPAREA_136", intValue(rawItemJson, "kaptMparea136"));

        int sumClassCount = classCounts.values().stream().mapToInt(Integer::intValue).sum();
        int delta = hoCnt - sumClassCount;
        if (delta > 0) {
            classCounts.put("MPAREA_UNKNOWN", delta);
        }

        for (Map.Entry<String, Integer> entry : classCounts.entrySet()) {
            upsertClassAndUnits(kaptCode, entry.getKey(), entry.getValue());
        }

        outboxEventPublisher.publish(
            "Complex",
            kaptCode,
            "ComplexImported",
            kaptCode,
            objectMapper.createObjectNode()
                .put("kaptCode", kaptCode)
                .put("hoCnt", hoCnt)
                .put("sumClassCount", sumClassCount)
                .put("unknownCount", Math.max(delta, 0))
        );

        return Map.of(
            "kaptCode", kaptCode,
            "hoCnt", hoCnt,
            "sumClassCount", sumClassCount,
            "unknownClassCount", Math.max(delta, 0),
            "classCount", classCounts.size()
        );
    }

    public List<ComplexEntity> listComplexes() {
        return complexRepository.findAll();
    }

    public ComplexEntity getComplex(String kaptCode) {
        return complexRepository.findById(kaptCode)
            .orElseThrow(() -> new ApiException(HttpStatus.NOT_FOUND, "Complex not found: " + kaptCode));
    }

    public List<PropertyClassEntity> getClasses(String kaptCode) {
        return classRepository.findByKaptCodeOrderByClassKeyAsc(kaptCode);
    }

    public List<UnitEntity> getUnits(String classId) {
        return unitRepository.findByClassIdOrderByUnitNoAsc(classId);
    }

    private void upsertClassAndUnits(String kaptCode, String classKey, int unitCount) {
        String classId = deriveClassId(kaptCode, classKey);
        Optional<PropertyClassEntity> existingOpt = classRepository.findById(classId);

        PropertyClassEntity entity = existingOpt.orElseGet(PropertyClassEntity::new);
        if (existingOpt.isPresent()
            && CLASS_STATUS_TOKENIZED.equals(existingOpt.get().getStatus())
            && existingOpt.get().getUnitCount() != unitCount) {
            throw new ApiException(HttpStatus.CONFLICT, "TOKENIZED class is immutable");
        }

        entity.setClassId(classId);
        entity.setKaptCode(kaptCode);
        entity.setClassKey(classKey);
        entity.setUnitCount(unitCount);
        if (entity.getStatus() == null) {
            entity.setStatus(CLASS_STATUS_IMPORTED);
        }
        if (entity.getCreatedAt() == null) {
            entity.setCreatedAt(Instant.now());
        }
        entity.setUpdatedAt(Instant.now());
        classRepository.save(entity);

        List<UnitEntity> existingUnits = new ArrayList<>(unitRepository.findByClassIdOrderByUnitNoAsc(classId));
        Map<Integer, UnitEntity> existingByNo = new LinkedHashMap<>();
        for (UnitEntity unit : existingUnits) {
            existingByNo.put(unit.getUnitNo(), unit);
        }

        for (int i = 1; i <= unitCount; i++) {
            UnitEntity unit = existingByNo.getOrDefault(i, new UnitEntity());
            unit.setUnitId(deriveUnitId(kaptCode, classKey, i));
            unit.setClassId(classId);
            unit.setUnitNo(i);
            if (unit.getStatus() == null) {
                unit.setStatus(CLASS_STATUS_IMPORTED);
            }
            unitRepository.save(unit);
        }

        for (UnitEntity unit : existingUnits) {
            if (unit.getUnitNo() > unitCount && !CLASS_STATUS_TOKENIZED.equals(entity.getStatus())) {
                unitRepository.delete(unit);
            }
        }
    }

    public String deriveClassId(String kaptCode, String classKey) {
        return Hash.sha3String((kaptCode + ":" + classKey).toLowerCase());
    }

    public String deriveUnitId(String kaptCode, String classKey, int unitNo) {
        return kaptCode + ":" + classKey + ":" + String.format("%05d", unitNo);
    }

    private void putIfPositive(Map<String, Integer> map, String key, int value) {
        if (value > 0) {
            map.put(key, value);
        }
    }

    private String requiredText(JsonNode raw, String field) {
        String value = raw.path(field).asText(null);
        if (value == null || value.isBlank()) {
            throw new ApiException(HttpStatus.BAD_REQUEST, field + " is required");
        }
        return value;
    }

    private int intValue(JsonNode raw, String field) {
        JsonNode node = raw.path(field);
        if (node.isMissingNode() || node.isNull()) {
            return 0;
        }
        if (node.isInt() || node.isLong()) {
            return node.asInt();
        }
        if (node.isTextual()) {
            try {
                return Integer.parseInt(node.asText().trim());
            } catch (NumberFormatException e) {
                return 0;
            }
        }
        return 0;
    }
}
