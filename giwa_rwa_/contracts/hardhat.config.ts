import dotenv from "dotenv";
import { defineConfig } from "hardhat/config";
import hardhatEthersPlugin from "@nomicfoundation/hardhat-ethers";
import hardhatEthersChaiMatchersPlugin from "@nomicfoundation/hardhat-ethers-chai-matchers";
import hardhatIgnitionEthersPlugin from "@nomicfoundation/hardhat-ignition-ethers";
import hardhatMochaPlugin from "@nomicfoundation/hardhat-mocha";
import hardhatVerifyPlugin from "@nomicfoundation/hardhat-verify";

dotenv.config();

const privateKey = process.env.PRIVATE_KEY;
const isValidPrivateKey = typeof privateKey === "string" && /^0x[a-fA-F0-9]{64}$/.test(privateKey);
const giwaRpcUrl = process.env.GIWA_SEPOLIA_RPC_URL || "https://sepolia-rpc.giwa.io";
const giwaChainId = Number(process.env.GIWA_SEPOLIA_CHAIN_ID || "91342");
const blockscoutApiUrl = process.env.GIWA_BLOCKSCOUT_API_URL || "https://sepolia-explorer.giwa.io/api";
const blockscoutBrowserUrl = process.env.GIWA_BLOCKSCOUT_BROWSER_URL || "https://sepolia-explorer.giwa.io";

export default defineConfig({
  plugins: [
    hardhatEthersPlugin,
    hardhatEthersChaiMatchersPlugin,
    hardhatMochaPlugin,
    hardhatIgnitionEthersPlugin,
    hardhatVerifyPlugin
  ],
  solidity: {
    version: "0.8.28",
    settings: {
      optimizer: {
        enabled: true,
        runs: 200
      }
    }
  },
  networks: {
    hardhatMainnet: {
      type: "edr-simulated",
      chainType: "l1",
      chainId: 31337
    },
    localhost: {
      type: "http",
      chainType: "l1",
      url: "http://127.0.0.1:8545",
      chainId: 31337
    },
    giwaSepolia: {
      type: "http",
      chainType: "op",
      url: giwaRpcUrl,
      chainId: giwaChainId,
      accounts: isValidPrivateKey ? [privateKey] : []
    }
  },
  chainDescriptors: {
    [giwaChainId]: {
      name: "giwaSepolia",
      chainType: "op",
      blockExplorers: {
        blockscout: {
          name: "GIWA Blockscout",
          url: blockscoutBrowserUrl,
          apiUrl: blockscoutApiUrl
        },
        etherscan: {
          name: "GIWA Blockscout",
          url: blockscoutBrowserUrl,
          apiUrl: blockscoutApiUrl
        }
      }
    }
  },
  verify: {
    blockscout: {
      enabled: true
    },
    etherscan: {
      apiKey: process.env.GIWA_BLOCKSCOUT_API_KEY || "api-key-not-required"
    }
  }
});
