/* eslint-disable prefer-const */
import { ERC20 } from "../../generated/Factory/ERC20"
import { BigInt, Address } from "@graphprotocol/graph-ts"
import { isNullEthValue } from "."
import { ZERO_BI } from "./constants"

export function fetchTokenSymbol(tokenAddress: Address): string {
  let contract = ERC20.bind(tokenAddress)

  // try types string and bytes32 for symbol
  let symbolValue = "unknown"
  let symbolResult = contract.try_symbol()
  symbolValue = symbolResult.value

  return symbolValue
}

export function fetchTokenName(tokenAddress: Address): string {
  let contract = ERC20.bind(tokenAddress)

  // try types string and bytes32 for name
  let nameValue = "unknown"
  let nameResult = contract.try_name()
  nameValue = nameResult.value

  return nameValue
}

export function fetchTokenTotalSupply(tokenAddress: Address): BigInt {
  let contract = ERC20.bind(tokenAddress)
  let totalSupplyValue = ZERO_BI
  let totalSupplyResult = contract.try_totalSupply()
  if (!totalSupplyResult.reverted) {
    totalSupplyValue = totalSupplyResult.value
  }
  return totalSupplyValue
}

export function fetchTokenDecimals(tokenAddress: Address): BigInt {
  let contract = ERC20.bind(tokenAddress)
  // try types uint8 for decimals
  let decimalValue = ZERO_BI
  let decimalResult = contract.try_decimals()
  if (!decimalResult.reverted) {
    decimalValue = BigInt.fromI32(decimalResult.value)
  }

  return decimalValue
}
