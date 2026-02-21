package io.rwa.server.web3;

import io.rwa.server.common.ApiException;
import io.rwa.server.config.SharedConstantsLoader;
import io.rwa.server.config.SharedDeploymentLoader;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.web3j.abi.TypeReference;
import org.web3j.abi.datatypes.Address;
import org.web3j.abi.datatypes.Bool;
import org.web3j.abi.datatypes.DynamicBytes;
import org.web3j.abi.datatypes.Function;
import org.web3j.abi.datatypes.Type;
import org.web3j.abi.datatypes.generated.Bytes32;
import org.web3j.abi.datatypes.generated.Uint256;
import org.web3j.abi.datatypes.generated.Uint32;
import org.web3j.abi.datatypes.generated.Uint64;
import org.web3j.abi.datatypes.generated.Uint8;
import org.web3j.utils.Numeric;

@Service
public class ContractGatewayService {

    private final SharedDeploymentLoader deploymentLoader;
    private final SharedConstantsLoader constantsLoader;
    private final Web3FunctionService web3FunctionService;

    public ContractGatewayService(
        SharedDeploymentLoader deploymentLoader,
        SharedConstantsLoader constantsLoader,
        Web3FunctionService web3FunctionService
    ) {
        this.deploymentLoader = deploymentLoader;
        this.constantsLoader = constantsLoader;
        this.web3FunctionService = web3FunctionService;
    }

    public String mockUsdAddress() {
        return deploymentLoader.addressOf("MockUSD");
    }

    public String propertyRegistryAddress() {
        return deploymentLoader.addressOf("PropertyRegistry");
    }

    public String propertyShareAddress() {
        return deploymentLoader.addressOf("PropertyShare1155");
    }

    public String propertyTokenizerAddress() {
        return deploymentLoader.addressOf("PropertyTokenizer");
    }

    public String marketAddress() {
        return deploymentLoader.addressOf("FixedPriceMarketDvP");
    }

    public BigInteger shareScale() {
        return constantsLoader.getShareScale();
    }

    public Function fnReserveAndRegisterClass(String classIdHex, String docHashHex, int unitCount, long issuedAt) {
        return new Function(
            "reserveAndRegisterClass",
            List.of(
                toBytes32(classIdHex),
                toBytes32(docHashHex),
                new Uint32(unitCount),
                new Uint64(issuedAt)
            ),
            List.of(new TypeReference<Uint256>() {
            })
        );
    }

    public Function fnMintUnits(String classIdHex, int startOffset, int count, String treasuryAddress) {
        return new Function(
            "mintUnits",
            List.of(
                toBytes32(classIdHex),
                new Uint32(startOffset),
                new Uint32(count),
                new Address(treasuryAddress)
            ),
            List.of()
        );
    }

    public Function fnMintMockUsd(String to, BigInteger amount) {
        return new Function("mint", List.of(new Address(to), new Uint256(amount)), List.of());
    }

    public Function fnShareSafeTransfer(String from, String to, BigInteger tokenId, BigInteger amount) {
        return new Function(
            "safeTransferFrom",
            List.of(
                new Address(from),
                new Address(to),
                new Uint256(tokenId),
                new Uint256(amount),
                new DynamicBytes(new byte[0])
            ),
            List.of()
        );
    }

    public Function fnSetApprovalForAll(String operator, boolean approved) {
        return new Function(
            "setApprovalForAll",
            List.of(new Address(operator), new Bool(approved)),
            List.of()
        );
    }

    public Function fnMarketList(BigInteger tokenId, BigInteger amount, BigInteger unitPrice) {
        return new Function(
            "list",
            List.of(
                new Address(propertyShareAddress()),
                new Uint256(tokenId),
                new Address(mockUsdAddress()),
                new Uint256(amount),
                new Uint256(unitPrice)
            ),
            List.of(new TypeReference<Uint256>() {
            })
        );
    }

    public Function fnMarketBuy(BigInteger listingId, BigInteger amount) {
        return new Function("buy", List.of(new Uint256(listingId), new Uint256(amount)), List.of());
    }

    public Function fnMockUsdApprove(String spender, BigInteger amount) {
        return new Function(
            "approve",
            List.of(new Address(spender), new Uint256(amount)),
            List.of(new TypeReference<Bool>() {
            })
        );
    }

    public boolean isApprovedForAll(String owner, String operator) {
        Function function = new Function(
            "isApprovedForAll",
            List.of(new Address(owner), new Address(operator)),
            List.of(new TypeReference<Bool>() {
            })
        );
        List<Type> output = web3FunctionService.callFunction(propertyShareAddress(), function);
        return ((Bool) output.get(0)).getValue();
    }

    public BigInteger allowance(String owner, String spender) {
        Function function = new Function(
            "allowance",
            List.of(new Address(owner), new Address(spender)),
            List.of(new TypeReference<Uint256>() {
            })
        );
        List<Type> output = web3FunctionService.callFunction(mockUsdAddress(), function);
        return ((Uint256) output.get(0)).getValue();
    }

    public RegistryClassInfo getRegistryClass(String classIdHex) {
        Function function = new Function(
            "getClass",
            List.of(toBytes32(classIdHex)),
            List.of(
                new TypeReference<Bytes32>() {
                },
                new TypeReference<Uint32>() {
                },
                new TypeReference<Uint256>() {
                },
                new TypeReference<Uint8>() {
                },
                new TypeReference<Uint64>() {
                }
            )
        );

        List<Type> output = web3FunctionService.callFunction(propertyRegistryAddress(), function);
        return new RegistryClassInfo(
            toHex((Bytes32) output.get(0)),
            ((Uint32) output.get(1)).getValue().intValue(),
            ((Uint256) output.get(2)).getValue(),
            ((Uint8) output.get(3)).getValue().intValue(),
            ((Uint64) output.get(4)).getValue().longValue()
        );
    }

    public MarketListing getMarketListing(BigInteger listingId) {
        Function function = new Function(
            "listings",
            List.of(new Uint256(listingId)),
            List.of(
                new TypeReference<Address>() {
                },
                new TypeReference<Address>() {
                },
                new TypeReference<Uint256>() {
                },
                new TypeReference<Address>() {
                },
                new TypeReference<Uint256>() {
                },
                new TypeReference<Uint256>() {
                },
                new TypeReference<Uint256>() {
                },
                new TypeReference<Uint8>() {
                }
            )
        );

        List<Type> output = web3FunctionService.callFunction(marketAddress(), function);
        return new MarketListing(
            ((Address) output.get(0)).getValue().toLowerCase(),
            ((Address) output.get(1)).getValue().toLowerCase(),
            ((Uint256) output.get(2)).getValue(),
            ((Address) output.get(3)).getValue().toLowerCase(),
            ((Uint256) output.get(4)).getValue(),
            ((Uint256) output.get(5)).getValue(),
            ((Uint256) output.get(6)).getValue(),
            ((Uint8) output.get(7)).getValue().intValue()
        );
    }

    public Bytes32 toBytes32(String hex) {
        String clean = Numeric.cleanHexPrefix(hex);
        if (clean.length() > 64) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "bytes32 hex too long");
        }
        clean = String.format("%64s", clean).replace(' ', '0');
        byte[] raw = Numeric.hexStringToByteArray("0x" + clean);
        if (raw.length != 32) {
            raw = Arrays.copyOfRange(raw, Math.max(raw.length - 32, 0), raw.length);
        }
        return new Bytes32(raw);
    }

    public String toHex(Bytes32 bytes32) {
        return "0x" + Numeric.toHexStringNoPrefix(bytes32.getValue());
    }

    public String keccak256Hex(String value) {
        return org.web3j.crypto.Hash.sha3String(value);
    }

    public String keccak256Hex(byte[] bytes) {
        return Numeric.toHexString(org.web3j.crypto.Hash.sha3(bytes));
    }

    public byte[] utf8(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    public record RegistryClassInfo(
        String docHash,
        int unitCount,
        BigInteger baseTokenId,
        int status,
        long issuedAt
    ) {
    }

    public record MarketListing(
        String seller,
        String shareToken,
        BigInteger tokenId,
        String payToken,
        BigInteger unitPrice,
        BigInteger totalAmount,
        BigInteger remainingAmount,
        int status
    ) {
    }
}
