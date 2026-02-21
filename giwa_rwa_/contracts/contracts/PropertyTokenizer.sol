// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import {Ownable} from "@openzeppelin/contracts/access/Ownable.sol";

import {PropertyRegistry} from "./PropertyRegistry.sol";
import {PropertyShare1155} from "./PropertyShare1155.sol";

contract PropertyTokenizer is Ownable {
    uint256 public constant SHARE_SCALE = 1e18;
    uint8 public constant STATUS_ACTIVE = 1;

    PropertyRegistry public immutable registry;
    PropertyShare1155 public immutable share;

    uint256 public nextBaseTokenId = 1;

    event ClassReserved(bytes32 indexed classId, uint256 baseTokenId, uint32 unitCount);
    event UnitsMinted(bytes32 indexed classId, uint32 startOffset, uint32 count, address treasury);

    error ZeroAddress();
    error InvalidUnitCount();
    error InvalidMintCount();
    error InvalidMintRange();
    error InactiveClass(bytes32 classId);
    error TokenAlreadyMinted(uint256 tokenId);

    constructor(address registryAddress, address shareAddress) Ownable(msg.sender) {
        if (registryAddress == address(0) || shareAddress == address(0)) {
            revert ZeroAddress();
        }

        registry = PropertyRegistry(registryAddress);
        share = PropertyShare1155(shareAddress);
    }

    function reserveAndRegisterClass(
        bytes32 classId,
        bytes32 docHash,
        uint32 unitCount,
        uint64 issuedAt
    ) external onlyOwner returns (uint256 baseTokenId) {
        if (unitCount == 0) {
            revert InvalidUnitCount();
        }

        baseTokenId = nextBaseTokenId;
        nextBaseTokenId = baseTokenId + uint256(unitCount);

        registry.registerClass(classId, docHash, unitCount, baseTokenId, issuedAt);

        emit ClassReserved(classId, baseTokenId, unitCount);
    }

    function mintUnits(bytes32 classId, uint32 startOffset, uint32 count, address treasury) external onlyOwner {
        if (count == 0) {
            revert InvalidMintCount();
        }
        if (treasury == address(0)) {
            revert ZeroAddress();
        }

        PropertyRegistry.ClassInfo memory classInfo = registry.getClass(classId);

        if (classInfo.status != STATUS_ACTIVE) {
            revert InactiveClass(classId);
        }

        uint256 endExclusive = uint256(startOffset) + uint256(count);
        if (endExclusive > uint256(classInfo.unitCount)) {
            revert InvalidMintRange();
        }

        uint256[] memory ids = new uint256[](count);
        uint256[] memory amounts = new uint256[](count);

        uint256 firstTokenId = classInfo.baseTokenId + uint256(startOffset);
        for (uint256 i = 0; i < count; ) {
            uint256 tokenId = firstTokenId + i;
            if (share.totalSupply(tokenId) != 0) {
                revert TokenAlreadyMinted(tokenId);
            }

            ids[i] = tokenId;
            amounts[i] = SHARE_SCALE;
            unchecked {
                ++i;
            }
        }

        share.mintBatch(treasury, ids, amounts);

        emit UnitsMinted(classId, startOffset, count, treasury);
    }
}
