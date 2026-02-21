# Codex Spec Tracker

Last updated: 2026-02-15 (post-live deploy)

## Canonical Baseline (Current)

### Architecture decision
- Compliance model is off-chain.
- On-chain KYC allowlist enforcement is removed.
- Current production set is 5 contracts (not 6):
1. `MockUSD`
2. `PropertyRegistry`
3. `PropertyShare1155`
4. `PropertyTokenizer`
5. `FixedPriceMarketDvP`

### Why this matters
- API/backend must perform compliance checks before allowing user actions.
- Smart contracts no longer block transfers/listing/buying via on-chain allowlist.

## Delta vs Original Spec

### Removed from original
- `KYCRegistry` contract.
- `PropertyShare1155` transfer-time KYC checks.
- Bootstrap allowlist step:
  - `setAllowed(market/treasury/issuer, true)`.

### Added/changed from original
- `PropertyTokenizer.mintUnits` now rejects remint of already-minted token IDs via `TokenAlreadyMinted`.
- `deploy:all:giwa` now includes verification step.
- Deployment `baseURI` default is sourced from `.env` `PROPERTY_SHARE_BASE_URI` (with fallback default).

## Contracts (Implemented Specs)

### 1) MockUSD.sol
- `ERC20 + Ownable`
- `decimals() = 18`
- `mint(address to, uint256 amount)` onlyOwner
- File: `contracts/contracts/MockUSD.sol`

### 2) PropertyRegistry.sol
- Class registry with owner-only mutation
- Struct:
  - `docHash`
  - `unitCount`
  - `baseTokenId`
  - `status`
  - `issuedAt`
- Supports `registerClass`, `setStatus`, `updateDocHash`, `getClass`
- File: `contracts/contracts/PropertyRegistry.sol`

### 3) PropertyShare1155.sol
- `ERC1155 + ERC1155Supply + Ownable`
- `SHARE_SCALE = 1e18`
- URI pattern: `${baseURI}/${id}.json`
- Owner-only `mintBatch`
- Burn allowed by owner or token holder
- No KYC transfer restriction in `_update`
- File: `contracts/contracts/PropertyShare1155.sol`

### 4) PropertyTokenizer.sol
- Owner-only issuance/tokenization flow
- References: `PropertyRegistry`, `PropertyShare1155`
- `nextBaseTokenId` starts at 1
- `reserveAndRegisterClass` allocates token ID range and registers class
- `mintUnits` validates range and mints `SHARE_SCALE` per unit
- Re-mint prevention with `TokenAlreadyMinted`
- File: `contracts/contracts/PropertyTokenizer.sol`

### 5) FixedPriceMarketDvP.sol
- `ReentrancyGuard + ERC1155Holder`
- Listing escrow + atomic DvP buy:
  - ERC20 buyer -> seller
  - ERC1155 market escrow -> buyer
- Uses `Math.mulDiv(amount, unitPrice, SHARE_SCALE)`
- Supports partial fill + cancel lifecycle
- File: `contracts/contracts/FixedPriceMarketDvP.sol`

## Network/Deploy/Verify

### Hardhat config
- Network: `giwaSepolia`
- RPC: `https://sepolia-rpc.giwa.io`
- `chainType: "op"`
- Chain ID: `91342`
- Blockscout endpoints wired for verify
- File: `contracts/hardhat.config.ts`

### Deployment order (current)
1. `MockUSD`
2. `PropertyRegistry`
3. `PropertyShare1155(baseURI)`
4. `PropertyTokenizer(registry, share)`
5. `FixedPriceMarketDvP`

### Bootstrap sequence (current)
1. `PropertyRegistry.owner -> PropertyTokenizer`
2. `PropertyShare1155.owner -> PropertyTokenizer`
3. `MockUSD.owner -> ISSUER_ADDRESS`
4. `PropertyTokenizer.owner -> ISSUER_ADDRESS`
5. Owner assertions (and optional market owner assertion if `owner()` exists)

### NPM commands
- `deploy:giwa`
- `export:deployment:giwa`
- `export:abi`
- `bootstrap:giwa`
- `verify:giwa`
- `deploy:all:giwa` (includes verify)

## Live Deployment Snapshot (GIWA Sepolia)

### Deployment artifacts
- `contracts/deployments/giwa-sepolia.json`
- `shared/deployments/giwa-sepolia.json`

### Deployed at
- `2026-02-15T08:28:32.422Z`

### Addresses (chainId 91342)
- `MockUSD`: `0x336Ff3629dE73804bad107d6291cD54EC538E7a3`
- `PropertyRegistry`: `0x7063D25b17FC4df908531EC7B9f7D9041ADDDbdc`
- `PropertyShare1155`: `0x013f975053770387633caB552311DA591F75692F`
- `PropertyTokenizer`: `0x516Ff79Fa012e2BF965b2c187470B961b5eba529`
- `FixedPriceMarketDvP`: `0x364efa0edc35e2a49D6f112F6CaE28e9Cc51e8C3`

### Post-bootstrap owner state (confirmed)
- `MockUSD.owner = ISSUER_ADDRESS`
- `PropertyTokenizer.owner = ISSUER_ADDRESS`
- `PropertyRegistry.owner = PropertyTokenizer`
- `PropertyShare1155.owner = PropertyTokenizer`

### Verification status
- All 5 contracts verified on GIWA Blockscout.

### Detailed deployment log
- `contracts/logs/giwa-sepolia-deployment-2026-02-15.md` (local log, gitignored by `logs/` rule)

## Test Coverage (Current)

### `contracts/test/rwa.spec.ts`
- Off-chain compliance model behavior (no on-chain allowlist gate)
- DvP atomicity:
  - insufficient ERC20 allowance reverts with no partial state change
  - insufficient escrow path preserves ERC20 state
- Listing lifecycle:
  - partial fills
  - cancel return flow
- Tokenizer:
  - range allocation/mint ID correctness
  - duplicate mint prevention
- PropertyRegistry status validation

### `contracts/test/gas-report.spec.ts`
- Deployment + representative tx gas profiling
- Outputs:
  - `contracts/logs/gas/hardhat-gas-report.json`
  - `contracts/logs/gas/hardhat-gas-report.md`

## Shared Artifacts for Other Modules

### Contract addresses (runtime source of truth)
- `shared/deployments/giwa-sepolia.json`

### Contract ABIs
- `shared/abi/MockUSD.json`
- `shared/abi/PropertyRegistry.json`
- `shared/abi/PropertyShare1155.json`
- `shared/abi/PropertyTokenizer.json`
- `shared/abi/FixedPriceMarketDvP.json`
- Note: legacy `KYCRegistry.json` is removed during ABI export.

### Constants
- `shared/config/constants.json`
  - `SHARE_SCALE = 1e18`
  - `CONFIRMATIONS = 12`

### Event signature registry
- `shared/events/signatures.json` is currently empty and must be populated when ingester/flink event parsing is implemented.

## Integration Guidance (Server / Ingester / Flink)

### Must know before implementing downstream systems
1. Do not depend on on-chain KYC reverts; enforce compliance in backend logic.
2. Read addresses from `shared/deployments/giwa-sepolia.json`, not hardcoded values.
3. Use ABIs from `shared/abi/*` only (no KYC ABI in current model).
4. Build event indexing from contract events (`Listed`, `Bought`, `Cancelled`, registry/tokenizer/share ownership+mint events) and maintain idempotency by tx hash + log index.

### Current gaps to schedule
- Populate `shared/events/signatures.json`.
- Align DB schema/indexing strategy in `shared/db/ddl.sql` with actual emitted events and identifiers.
- Clean stale localhost artifacts that still include `KYCRegistry` key to avoid confusion in tooling.

## Changelog (Condensed)

### 2026-02-14
- Initial contract suite and scripts implemented.
- Hardhat 3 migration completed.

### 2026-02-14 to 2026-02-15
- Migrated from on-chain KYC allowlist model to off-chain compliance model.
- Ownership handoff bootstrap stabilized.
- Base URI handling and verify flow improved.

### 2026-02-15
- Live deploy to GIWA Sepolia completed.
- Bootstrap ownership transfers executed and validated.
- Blockscout verification completed for all deployed contracts.
