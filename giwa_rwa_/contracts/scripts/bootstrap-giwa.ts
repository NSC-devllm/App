import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { network } from "hardhat";

type DeploymentContractKey =
  | "MockUSD"
  | "PropertyRegistry"
  | "PropertyShare1155"
  | "PropertyTokenizer"
  | "FixedPriceMarketDvP";

type DeploymentArtifact = {
  contracts?: Partial<Record<DeploymentContractKey, string>>;
};

type EthersLike = {
  ZeroAddress: string;
  Interface: new (fragments: string[]) => {
    encodeFunctionData(name: string, values?: unknown[]): string;
    decodeFunctionResult(name: string, data: string): unknown[];
  };
  isAddress(address: string): boolean;
  getAddress(address: string): string;
  provider: {
    call(tx: { to: string; data: string }): Promise<string>;
  };
};

function requireEnvAddress(ethers: EthersLike, key: string): string {
  const value = process.env[key];
  if (!value || !ethers.isAddress(value) || value === ethers.ZeroAddress) {
    throw new Error(`Missing or invalid ${key}; set a non-zero address in .env`);
  }

  return ethers.getAddress(value);
}

function assertAddress(label: string, actual: string, expected: string): void {
  if (actual.toLowerCase() !== expected.toLowerCase()) {
    throw new Error(`Owner assertion failed for ${label}: expected ${expected}, got ${actual}`);
  }
}

function requireDeploymentAddress(
  ethers: EthersLike,
  deployment: DeploymentArtifact,
  key: DeploymentContractKey
): string {
  const value = deployment.contracts?.[key];
  if (!value || !ethers.isAddress(value)) {
    throw new Error(`Invalid deployment artifact: missing or invalid contracts.${key}`);
  }

  return ethers.getAddress(value);
}

async function ensureOwnership(
  ownable: {
    owner(): Promise<string>;
    transferOwnership(newOwner: string): Promise<{ hash: string; wait(): Promise<unknown> }>;
  },
  label: string,
  targetOwner: string
): Promise<void> {
  const currentOwner = await ownable.owner();
  if (currentOwner.toLowerCase() === targetOwner.toLowerCase()) {
    console.log(`${label} already owned by target: ${targetOwner}`);
    return;
  }

  const tx = await ownable.transferOwnership(targetOwner);
  await tx.wait();
  console.log(`Transferred ${label} ownership: ${currentOwner} -> ${targetOwner} (tx: ${tx.hash})`);
}

async function tryGetOwner(ethers: EthersLike, contractAddress: string): Promise<string | null> {
  const ownerInterface = new ethers.Interface(["function owner() view returns (address)"]);

  try {
    const data = ownerInterface.encodeFunctionData("owner");
    const raw = await ethers.provider.call({ to: contractAddress, data });
    if (raw === "0x") {
      return null;
    }

    const [owner] = ownerInterface.decodeFunctionResult("owner", raw);
    if (typeof owner !== "string") {
      return null;
    }

    return owner;
  } catch {
    return null;
  }
}

async function main(): Promise<void> {
  const connection = await network.connect();
  const { ethers } = connection;
  const [deployer] = await ethers.getSigners();
  const networkName = connection.networkName;
  const __filename = fileURLToPath(import.meta.url);
  const __dirname = path.dirname(__filename);

  const outputFileName = networkName === "giwaSepolia" ? "giwa-sepolia.json" : `${networkName}.json`;
  const deploymentPath = path.resolve(__dirname, "..", "deployments", outputFileName);
  if (!fs.existsSync(deploymentPath)) {
    throw new Error(`Deployment artifact not found: ${deploymentPath}`);
  }

  const deployment = JSON.parse(fs.readFileSync(deploymentPath, "utf8")) as DeploymentArtifact;

  const issuer = requireEnvAddress(ethers, "ISSUER_ADDRESS");
  const mockUsdAddress = requireDeploymentAddress(ethers, deployment, "MockUSD");
  const registryAddress = requireDeploymentAddress(ethers, deployment, "PropertyRegistry");
  const shareAddress = requireDeploymentAddress(ethers, deployment, "PropertyShare1155");
  const tokenizerAddress = requireDeploymentAddress(ethers, deployment, "PropertyTokenizer");
  const marketAddress = requireDeploymentAddress(ethers, deployment, "FixedPriceMarketDvP");

  const mockUsd = await ethers.getContractAt("MockUSD", mockUsdAddress, deployer);
  const registry = await ethers.getContractAt("PropertyRegistry", registryAddress, deployer);
  const share = await ethers.getContractAt("PropertyShare1155", shareAddress, deployer);
  const tokenizer = await ethers.getContractAt("PropertyTokenizer", tokenizerAddress, deployer);

  // 1) transferOwnership(PropertyRegistry -> Tokenizer)
  await ensureOwnership(registry, "PropertyRegistry", tokenizerAddress);

  // 2) transferOwnership(PropertyShare1155 -> Tokenizer)
  await ensureOwnership(share, "PropertyShare1155", tokenizerAddress);

  // 3) transferOwnership(MockUSD -> issuer)
  await ensureOwnership(mockUsd, "MockUSD", issuer);

  // 4) transferOwnership(Tokenizer -> issuer)
  await ensureOwnership(tokenizer, "PropertyTokenizer", issuer);

  // 5) assert owner() values match; fail otherwise.
  assertAddress("MockUSD.owner", await mockUsd.owner(), issuer);
  assertAddress("PropertyTokenizer.owner", await tokenizer.owner(), issuer);
  assertAddress("PropertyRegistry.owner", await registry.owner(), tokenizerAddress);
  assertAddress("PropertyShare1155.owner", await share.owner(), tokenizerAddress);

  const marketOwner = await tryGetOwner(ethers, marketAddress);
  if (marketOwner !== null) {
    assertAddress("FixedPriceMarketDvP.owner", marketOwner, issuer);
  }

  console.log("Ownership assertions passed.");
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
