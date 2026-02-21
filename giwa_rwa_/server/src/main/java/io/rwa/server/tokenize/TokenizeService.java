package io.rwa.server.tokenize;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.rwa.server.common.ApiException;
import io.rwa.server.common.CanonicalJsonService;
import io.rwa.server.config.RwaProperties;
import io.rwa.server.outbox.OutboxEventPublisher;
import io.rwa.server.publicdata.PropertyClassEntity;
import io.rwa.server.publicdata.PropertyClassRepository;
import io.rwa.server.publicdata.PublicDataService;
import io.rwa.server.publicdata.UnitEntity;
import io.rwa.server.publicdata.UnitRepository;
import io.rwa.server.tx.OutboxTxEntity;
import io.rwa.server.tx.TxOrchestratorService;
import io.rwa.server.web3.ContractGatewayService;
import jakarta.transaction.Transactional;
import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.web3j.crypto.Credentials;

@Service
public class TokenizeService {

    private final PropertyClassRepository classRepository;
    private final UnitRepository unitRepository;
    private final ContractGatewayService contractGatewayService;
    private final TxOrchestratorService txOrchestratorService;
    private final CanonicalJsonService canonicalJsonService;
    private final ObjectMapper objectMapper;
    private final RwaProperties properties;
    private final OutboxEventPublisher outboxEventPublisher;

    public TokenizeService(
        PropertyClassRepository classRepository,
        UnitRepository unitRepository,
        ContractGatewayService contractGatewayService,
        TxOrchestratorService txOrchestratorService,
        CanonicalJsonService canonicalJsonService,
        ObjectMapper objectMapper,
        RwaProperties properties,
        OutboxEventPublisher outboxEventPublisher
    ) {
        this.classRepository = classRepository;
        this.unitRepository = unitRepository;
        this.contractGatewayService = contractGatewayService;
        this.txOrchestratorService = txOrchestratorService;
        this.canonicalJsonService = canonicalJsonService;
        this.objectMapper = objectMapper;
        this.properties = properties;
        this.outboxEventPublisher = outboxEventPublisher;
    }

    @Transactional
    public TokenizeResult tokenizeClass(String classId, TokenizeRequest request, String idempotencyKey) {
        PropertyClassEntity clazz = classRepository.findById(classId)
            .orElseThrow(() -> new ApiException(HttpStatus.NOT_FOUND, "Class not found: " + classId));

        int chunkSize = request == null || request.chunkSize() == null || request.chunkSize() <= 0
            ? 100
            : request.chunkSize();
        long issuedAt = request == null || request.issuedAt() == null
            ? Instant.now().getEpochSecond()
            : request.issuedAt();

        String docHash = computeDocHash(clazz);

        String issuerPrivateKey = requirePrivateKey(properties.getIssuerPrivateKey(), "ISSUER_PRIVATE_KEY");
        String issuerAddress = Credentials.create(issuerPrivateKey).getAddress().toLowerCase();
        String treasuryAddress = Credentials.create(requirePrivateKey(properties.getTreasuryPrivateKey(), "TREASURY_PRIVATE_KEY"))
            .getAddress()
            .toLowerCase();

        List<java.util.UUID> outboxIds = new ArrayList<>();

        OutboxTxEntity reserveTx = txOrchestratorService.submitContractTx(
            idempotencyKey + ":tokenize:reserve:" + classId,
            issuerAddress,
            issuerPrivateKey,
            contractGatewayService.propertyTokenizerAddress(),
            contractGatewayService.fnReserveAndRegisterClass(classId, docHash, clazz.getUnitCount(), issuedAt),
            "TOKENIZE_RESERVE_REGISTER",
            objectMapper.createObjectNode()
                .put("classId", classId)
                .put("docHash", docHash)
                .put("unitCount", clazz.getUnitCount())
                .put("issuedAt", issuedAt)
        );
        outboxIds.add(reserveTx.getOutboxId());

        int unitCount = clazz.getUnitCount();
        for (int start = 0; start < unitCount; start += chunkSize) {
            int count = Math.min(chunkSize, unitCount - start);
            try {
                OutboxTxEntity mintTx = txOrchestratorService.submitContractTx(
                    idempotencyKey + ":tokenize:mint:" + classId + ":" + start,
                    issuerAddress,
                    issuerPrivateKey,
                    contractGatewayService.propertyTokenizerAddress(),
                    contractGatewayService.fnMintUnits(classId, start, count, treasuryAddress),
                    "TOKENIZE_MINT_CHUNK",
                    objectMapper.createObjectNode()
                        .put("classId", classId)
                        .put("startOffset", start)
                        .put("count", count)
                );
                outboxIds.add(mintTx.getOutboxId());
            } catch (ApiException e) {
                if (e.getMessage() != null && e.getMessage().contains("TokenAlreadyMinted")) {
                    continue;
                }
                throw e;
            }
        }

        clazz.setDocHash(docHash);
        clazz.setIssuedAt(Instant.ofEpochSecond(issuedAt));
        clazz.setStatus(PublicDataService.CLASS_STATUS_TOKENIZED);
        clazz.setUpdatedAt(Instant.now());

        try {
            ContractGatewayService.RegistryClassInfo chainClass = contractGatewayService.getRegistryClass(classId);
            if (chainClass.unitCount() > 0) {
                clazz.setBaseTokenId(chainClass.baseTokenId());
                List<UnitEntity> units = unitRepository.findByClassIdOrderByUnitNoAsc(classId);
                for (UnitEntity unit : units) {
                    unit.setTokenId(chainClass.baseTokenId().add(BigInteger.valueOf(unit.getUnitNo() - 1L)));
                    unit.setStatus(PublicDataService.CLASS_STATUS_TOKENIZED);
                    unitRepository.save(unit);
                }
            }
        } catch (Exception ignored) {
            // chain state may not be visible yet; tx status endpoint can be used to track completion.
        }

        classRepository.save(clazz);

        ObjectNode payload = objectMapper.createObjectNode()
            .put("classId", classId)
            .put("docHash", docHash)
            .put("status", clazz.getStatus());
        outboxEventPublisher.publish("Class", classId, "ClassTokenized", classId, payload);

        return new TokenizeResult(classId, docHash, clazz.getStatus(), outboxIds);
    }

    private String computeDocHash(PropertyClassEntity clazz) {
        ObjectNode root = objectMapper.createObjectNode();
        root.put("classId", clazz.getClassId());
        root.put("kaptCode", clazz.getKaptCode());
        root.put("classKey", clazz.getClassKey());
        root.put("unitCount", clazz.getUnitCount());

        ArrayNode units = objectMapper.createArrayNode();
        for (UnitEntity unit : unitRepository.findByClassIdOrderByUnitNoAsc(clazz.getClassId())) {
            ObjectNode u = objectMapper.createObjectNode();
            u.put("unitId", unit.getUnitId());
            u.put("unitNo", unit.getUnitNo());
            units.add(u);
        }
        root.set("units", units);

        String canonical = canonicalJsonService.canonicalString(root);
        return contractGatewayService.keccak256Hex(contractGatewayService.utf8(canonical));
    }

    private String requirePrivateKey(String key, String envName) {
        if (key == null || key.isBlank()) {
            throw new ApiException(HttpStatus.INTERNAL_SERVER_ERROR, envName + " is required");
        }
        return key;
    }

    public record TokenizeResult(
        String classId,
        String docHash,
        String status,
        List<java.util.UUID> outboxIds
    ) {
    }
}
