// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import {Ownable} from "@openzeppelin/contracts/access/Ownable.sol";

contract PropertyRegistry is Ownable {
    struct ClassInfo {
        bytes32 docHash;
        uint32 unitCount;
        uint256 baseTokenId;
        uint8 status;
        uint64 issuedAt;
    }

    uint8 public constant STATUS_INACTIVE = 0;
    uint8 public constant STATUS_ACTIVE = 1;
    uint8 public constant STATUS_PAUSED = 2;

    mapping(bytes32 classId => ClassInfo) private _classes;

    event ClassRegistered(bytes32 indexed classId, uint256 baseTokenId, uint32 unitCount, bytes32 docHash);
    event DocHashUpdated(bytes32 indexed classId, bytes32 oldHash, bytes32 newHash);
    event ClassStatusChanged(bytes32 indexed classId, uint8 status);

    error ClassAlreadyRegistered(bytes32 classId);
    error ClassNotFound(bytes32 classId);
    error InvalidUnitCount();
    error InvalidStatus(uint8 status);

    constructor() Ownable(msg.sender) {}

    function registerClass(
        bytes32 classId,
        bytes32 docHash,
        uint32 unitCount,
        uint256 baseTokenId,
        uint64 issuedAt
    ) external onlyOwner {
        if (unitCount == 0) {
            revert InvalidUnitCount();
        }
        if (_classes[classId].unitCount != 0) {
            revert ClassAlreadyRegistered(classId);
        }

        _classes[classId] = ClassInfo({
            docHash: docHash,
            unitCount: unitCount,
            baseTokenId: baseTokenId,
            status: STATUS_ACTIVE,
            issuedAt: issuedAt
        });

        emit ClassRegistered(classId, baseTokenId, unitCount, docHash);
    }

    function setStatus(bytes32 classId, uint8 status) external onlyOwner {
        _requireClassExists(classId);
        if (status > STATUS_PAUSED) {
            revert InvalidStatus(status);
        }
        _classes[classId].status = status;
        emit ClassStatusChanged(classId, status);
    }

    function updateDocHash(bytes32 classId, bytes32 newDocHash) external onlyOwner {
        _requireClassExists(classId);
        bytes32 oldHash = _classes[classId].docHash;
        _classes[classId].docHash = newDocHash;
        emit DocHashUpdated(classId, oldHash, newDocHash);
    }

    function getClass(bytes32 classId) external view returns (ClassInfo memory) {
        return _classes[classId];
    }

    function _requireClassExists(bytes32 classId) private view {
        if (_classes[classId].unitCount == 0) {
            revert ClassNotFound(classId);
        }
    }
}
