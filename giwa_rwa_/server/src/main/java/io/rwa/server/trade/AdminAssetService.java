package io.rwa.server.trade;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.rwa.server.common.ApiException;
import io.rwa.server.config.RwaProperties;
import io.rwa.server.publicdata.UnitEntity;
import io.rwa.server.publicdata.UnitRepository;
import io.rwa.server.tx.OutboxTxEntity;
import io.rwa.server.tx.TxOrchestratorService;
import io.rwa.server.wallet.WalletService;
import io.rwa.server.web3.ContractGatewayService;
import java.math.BigInteger;
import java.util.Optional;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.web3j.crypto.Credentials;

@Service
public class AdminAssetService {

    private final WalletService walletService;
    private final UnitRepository unitRepository;
    private final ContractGatewayService contractGatewayService;
    private final TxOrchestratorService txOrchestratorService;
    private final RwaProperties properties;
    private final ObjectMapper objectMapper;

    public AdminAssetService(
        WalletService walletService,
        UnitRepository unitRepository,
        ContractGatewayService contractGatewayService,
        TxOrchestratorService txOrchestratorService,
        RwaProperties properties,
        ObjectMapper objectMapper
    ) {
        this.walletService = walletService;
        this.unitRepository = unitRepository;
        this.contractGatewayService = contractGatewayService;
        this.txOrchestratorService = txOrchestratorService;
        this.properties = properties;
        this.objectMapper = objectMapper;
    }

    public AdminAssetResult faucetMockUsd(FaucetRequest request, String idempotencyKey) {
        String issuerPrivKey = requireKey(properties.getIssuerPrivateKey(), "ISSUER_PRIVATE_KEY");
        String issuerAddress = Credentials.create(issuerPrivKey).getAddress().toLowerCase();
        String toAddress = walletService.getAddress(request.toUserId());

        OutboxTxEntity tx = txOrchestratorService.submitContractTx(
            idempotencyKey + ":faucet:musd:" + request.toUserId(),
            issuerAddress,
            issuerPrivKey,
            contractGatewayService.mockUsdAddress(),
            contractGatewayService.fnMintMockUsd(toAddress, request.amount()),
            "FAUCET_MUSD",
            objectMapper.createObjectNode()
                .put("toUserId", request.toUserId().toString())
                .put("toAddress", toAddress)
                .put("amount", request.amount().toString())
        );

        return new AdminAssetResult(tx.getOutboxId(), tx.getTxType(), tx.getStatus(), tx.getTxHash());
    }

    public AdminAssetResult distributeShares(DistributeSharesRequest request, String idempotencyKey) {
        walletService.assertApproved(request.toUserId());

        String treasuryPrivKey = requireKey(properties.getTreasuryPrivateKey(), "TREASURY_PRIVATE_KEY");
        String treasuryAddress = Credentials.create(treasuryPrivKey).getAddress().toLowerCase();
        String toAddress = walletService.getAddress(request.toUserId());
        BigInteger tokenId = resolveTokenId(request.tokenId(), request.unitId());

        OutboxTxEntity tx = txOrchestratorService.submitContractTx(
            idempotencyKey + ":distribute:shares:" + request.toUserId() + ":" + tokenId,
            treasuryAddress,
            treasuryPrivKey,
            contractGatewayService.propertyShareAddress(),
            contractGatewayService.fnShareSafeTransfer(treasuryAddress, toAddress, tokenId, request.amount()),
            "DISTRIBUTE_SHARES",
            objectMapper.createObjectNode()
                .put("toUserId", request.toUserId().toString())
                .put("toAddress", toAddress)
                .put("tokenId", tokenId.toString())
                .put("amount", request.amount().toString())
        );

        return new AdminAssetResult(tx.getOutboxId(), tx.getTxType(), tx.getStatus(), tx.getTxHash());
    }

    private BigInteger resolveTokenId(String tokenId, String unitId) {
        if (tokenId != null && !tokenId.isBlank()) {
            return new BigInteger(tokenId);
        }
        if (unitId == null || unitId.isBlank()) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "tokenId or unitId is required");
        }
        Optional<UnitEntity> unit = unitRepository.findByUnitId(unitId);
        if (unit.isEmpty() || unit.get().getTokenId() == null) {
            throw new ApiException(HttpStatus.NOT_FOUND, "Token id not found for unit: " + unitId);
        }
        return unit.get().getTokenId();
    }

    private String requireKey(String value, String keyName) {
        if (value == null || value.isBlank()) {
            throw new ApiException(HttpStatus.INTERNAL_SERVER_ERROR, keyName + " is not configured");
        }
        return value;
    }
}
