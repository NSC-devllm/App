package io.rwa.server.trade;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.rwa.server.common.ApiException;
import io.rwa.server.config.SharedConstantsLoader;
import io.rwa.server.publicdata.UnitEntity;
import io.rwa.server.publicdata.UnitRepository;
import io.rwa.server.query.ReadModelQueryService;
import io.rwa.server.tx.OutboxTxEntity;
import io.rwa.server.tx.TxOrchestratorService;
import io.rwa.server.wallet.WalletService;
import io.rwa.server.web3.ContractGatewayService;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

@Service
public class TradeService {

    private static final int MARKET_STATUS_ACTIVE = 1;

    private final WalletService walletService;
    private final UnitRepository unitRepository;
    private final ReadModelQueryService readModelQueryService;
    private final ContractGatewayService contractGatewayService;
    private final TxOrchestratorService txOrchestratorService;
    private final SharedConstantsLoader sharedConstantsLoader;
    private final ObjectMapper objectMapper;

    public TradeService(
        WalletService walletService,
        UnitRepository unitRepository,
        ReadModelQueryService readModelQueryService,
        ContractGatewayService contractGatewayService,
        TxOrchestratorService txOrchestratorService,
        SharedConstantsLoader sharedConstantsLoader,
        ObjectMapper objectMapper
    ) {
        this.walletService = walletService;
        this.unitRepository = unitRepository;
        this.readModelQueryService = readModelQueryService;
        this.contractGatewayService = contractGatewayService;
        this.txOrchestratorService = txOrchestratorService;
        this.sharedConstantsLoader = sharedConstantsLoader;
        this.objectMapper = objectMapper;
    }

    public TradeResult list(TradeListRequest request, String idempotencyKey) {
        walletService.assertApproved(request.sellerUserId());
        String sellerAddress = walletService.getAddress(request.sellerUserId());
        String sellerPrivKey = walletService.decryptUserPrivateKey(request.sellerUserId());
        BigInteger tokenId = resolveTokenId(request.tokenId(), request.unitId());

        List<java.util.UUID> outboxIds = new ArrayList<>();

        boolean approved = contractGatewayService.isApprovedForAll(sellerAddress, contractGatewayService.marketAddress());
        if (!approved) {
            OutboxTxEntity approvalTx = txOrchestratorService.submitContractTx(
                idempotencyKey + ":trade:list:set-approval:" + request.sellerUserId(),
                sellerAddress,
                sellerPrivKey,
                contractGatewayService.propertyShareAddress(),
                contractGatewayService.fnSetApprovalForAll(contractGatewayService.marketAddress(), true),
                "TRADE_SET_APPROVAL_FOR_ALL",
                objectMapper.createObjectNode()
                    .put("sellerUserId", request.sellerUserId().toString())
                    .put("market", contractGatewayService.marketAddress())
            );
            outboxIds.add(approvalTx.getOutboxId());
        }

        OutboxTxEntity listTx = txOrchestratorService.submitContractTx(
            idempotencyKey + ":trade:list:" + request.sellerUserId() + ":" + tokenId,
            sellerAddress,
            sellerPrivKey,
            contractGatewayService.marketAddress(),
            contractGatewayService.fnMarketList(tokenId, request.amount(), request.unitPrice()),
            "TRADE_LIST",
            objectMapper.createObjectNode()
                .put("sellerUserId", request.sellerUserId().toString())
                .put("tokenId", tokenId.toString())
                .put("amount", request.amount().toString())
                .put("unitPrice", request.unitPrice().toString())
        );
        outboxIds.add(listTx.getOutboxId());

        return new TradeResult(
            outboxIds,
            null,
            tokenId,
            request.amount(),
            request.unitPrice(),
            BigInteger.ZERO
        );
    }

    public TradeResult buy(TradeBuyRequest request, String idempotencyKey) {
        walletService.assertApproved(request.buyerUserId());

        String buyerAddress = walletService.getAddress(request.buyerUserId());
        String buyerPrivKey = walletService.decryptUserPrivateKey(request.buyerUserId());

        BigInteger unitPrice = null;
        try {
            Map<String, Object> listing = readModelQueryService.listingById(request.listingId().toString());
            String listingStatus = String.valueOf(listing.getOrDefault("listing_status", ""));
            if ("ACTIVE".equalsIgnoreCase(listingStatus) && listing.get("price") != null) {
                Object price = listing.get("price");
                if (price instanceof BigDecimal decimal) {
                    unitPrice = decimal.toBigInteger();
                } else {
                    unitPrice = new BigInteger(String.valueOf(price));
                }
            }
        } catch (Exception ignored) {
            // read model can lag; on-chain fallback below is authoritative.
        }

        ContractGatewayService.MarketListing chainListing = contractGatewayService.getMarketListing(request.listingId());
        if (chainListing.status() != MARKET_STATUS_ACTIVE) {
            throw new ApiException(HttpStatus.CONFLICT, "Listing is not ACTIVE");
        }
        if (chainListing.remainingAmount().compareTo(request.amount()) < 0) {
            throw new ApiException(HttpStatus.CONFLICT, "Insufficient remaining amount");
        }
        if (!chainListing.payToken().equals(contractGatewayService.mockUsdAddress())) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "Unsupported pay token for MVP");
        }
        if (!chainListing.shareToken().equals(contractGatewayService.propertyShareAddress())) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "Unsupported share token for MVP");
        }

        if (unitPrice == null) {
            unitPrice = chainListing.unitPrice();
        }

        BigInteger shareScale = sharedConstantsLoader.getShareScale();
        BigInteger cost = request.amount().multiply(unitPrice).divide(shareScale);

        List<java.util.UUID> outboxIds = new ArrayList<>();

        BigInteger allowance = contractGatewayService.allowance(buyerAddress, contractGatewayService.marketAddress());
        if (allowance.compareTo(cost) < 0) {
            BigInteger approveAmount = BigInteger.TWO.pow(256).subtract(BigInteger.ONE);
            OutboxTxEntity approveTx = txOrchestratorService.submitContractTx(
                idempotencyKey + ":trade:buy:approve:" + request.buyerUserId(),
                buyerAddress,
                buyerPrivKey,
                contractGatewayService.mockUsdAddress(),
                contractGatewayService.fnMockUsdApprove(contractGatewayService.marketAddress(), approveAmount),
                "TRADE_BUY_APPROVE",
                objectMapper.createObjectNode()
                    .put("buyerUserId", request.buyerUserId().toString())
                    .put("allowanceNeeded", cost.toString())
            );
            outboxIds.add(approveTx.getOutboxId());
        }

        OutboxTxEntity buyTx = txOrchestratorService.submitContractTx(
            idempotencyKey + ":trade:buy:" + request.buyerUserId() + ":" + request.listingId(),
            buyerAddress,
            buyerPrivKey,
            contractGatewayService.marketAddress(),
            contractGatewayService.fnMarketBuy(request.listingId(), request.amount()),
            "TRADE_BUY",
            objectMapper.createObjectNode()
                .put("buyerUserId", request.buyerUserId().toString())
                .put("listingId", request.listingId().toString())
                .put("amount", request.amount().toString())
                .put("cost", cost.toString())
        );
        outboxIds.add(buyTx.getOutboxId());

        return new TradeResult(
            outboxIds,
            request.listingId(),
            chainListing.tokenId(),
            request.amount(),
            unitPrice,
            cost
        );
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
}
