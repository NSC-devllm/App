import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { verifyContract } from "@nomicfoundation/hardhat-verify/verify";
import hre, { network } from "hardhat";

type DeploymentContractKey =
  | "MockUSD"
  | "PropertyRegistry"
  | "PropertyShare1155"
  | "PropertyTokenizer"
  | "FixedPriceMarketDvP";

type DeploymentArtifact = {
  contracts?: Partial<Record<DeploymentContractKey, string>>;
};

type VerifyTarget = {
  name: string;
  address: string;
  constructorArguments: unknown[];
};

function isAlreadyVerified(error: unknown): boolean {
  const message = String(error);
  return /already verified|already been verified|smart-contract already verified/i.test(message);
}

function requireDeploymentAddress(
  ethers: { isAddress(address: string): boolean; getAddress(address: string): string },
  deployment: DeploymentArtifact,
  key: DeploymentContractKey
): string {
  const value = deployment.contracts?.[key];
  if (!value || !ethers.isAddress(value)) {
    throw new Error(`Invalid deployment artifact: missing or invalid contracts.${key}`);
  }

  return ethers.getAddress(value);
}

function deriveBaseUriFromTokenUri(tokenUri: string): string | null {
  if (tokenUri.length === 0) {
    return "";
  }

  const suffix = "/0.json";
  if (!tokenUri.endsWith(suffix)) {
    return null;
  }

  return tokenUri.slice(0, -suffix.length);
}

async function main(): Promise<void> {
  const connection = await network.connect();
  const { ethers } = connection;
  const networkName = connection.networkName;
  const __filename = fileURLToPath(import.meta.url);
  const __dirname = path.dirname(__filename);
  const outputFileName = networkName === "giwaSepolia" ? "giwa-sepolia.json" : `${networkName}.json`;
  const deploymentPath = path.resolve(__dirname, "..", "deployments", outputFileName);
  if (!fs.existsSync(deploymentPath)) {
    throw new Error(`Deployment artifact not found: ${deploymentPath}`);
  }

  const deployment = JSON.parse(fs.readFileSync(deploymentPath, "utf8")) as DeploymentArtifact;
  const mockUsdAddress = requireDeploymentAddress(ethers, deployment, "MockUSD");
  const registryAddress = requireDeploymentAddress(ethers, deployment, "PropertyRegistry");
  const shareAddress = requireDeploymentAddress(ethers, deployment, "PropertyShare1155");
  const tokenizerAddress = requireDeploymentAddress(ethers, deployment, "PropertyTokenizer");
  const marketAddress = requireDeploymentAddress(ethers, deployment, "FixedPriceMarketDvP");
  const configuredBaseURI = process.env.PROPERTY_SHARE_BASE_URI?.trim();
  let baseURI = configuredBaseURI;
  if (!baseURI) {
    const share = await ethers.getContractAt("PropertyShare1155", shareAddress);
    const sampleTokenUri = await share.uri(0n);
    const derivedBaseURI = deriveBaseUriFromTokenUri(sampleTokenUri);
    if (derivedBaseURI === null) {
      throw new Error(
        "Unable to derive PROPERTY_SHARE_BASE_URI from on-chain uri(0); set PROPERTY_SHARE_BASE_URI explicitly."
      );
    }

    baseURI = derivedBaseURI;
    console.log(`Derived PROPERTY_SHARE_BASE_URI from on-chain uri(0): ${baseURI}`);
  }

  const targets: VerifyTarget[] = [
    {
      name: "MockUSD",
      address: mockUsdAddress,
      constructorArguments: []
    },
    {
      name: "PropertyRegistry",
      address: registryAddress,
      constructorArguments: []
    },
    {
      name: "PropertyShare1155",
      address: shareAddress,
      constructorArguments: [baseURI]
    },
    {
      name: "PropertyTokenizer",
      address: tokenizerAddress,
      constructorArguments: [registryAddress, shareAddress]
    },
    {
      name: "FixedPriceMarketDvP",
      address: marketAddress,
      constructorArguments: []
    }
  ];

  for (const target of targets) {
    try {
      console.log(`Verifying ${target.name} at ${target.address}`);
      await verifyContract(
        {
          address: target.address,
          constructorArgs: target.constructorArguments,
          provider: "blockscout"
        },
        hre
      );
      console.log(`Verified ${target.name}`);
    } catch (error) {
      if (isAlreadyVerified(error)) {
        console.log(`${target.name} is already verified`);
        continue;
      }

      throw error;
    }
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
