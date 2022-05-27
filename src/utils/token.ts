/* eslint-disable prefer-const */
import { ERC20 } from "../types/Factory/ERC20"
import { BigInt, Address } from "@graphprotocol/graph-ts"
import { isNullEthValue } from "."

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
  let totalSupplyValue = null
  let totalSupplyResult = contract.try_totalSupply()
  if (!totalSupplyResult.reverted) {
    totalSupplyValue = totalSupplyResult as i32
  }
  return BigInt.fromI32(totalSupplyValue as i32)
}

export function fetchTokenDecimals(tokenAddress: Address): BigInt {
  let contract = ERC20.bind(tokenAddress)
  // try types uint8 for decimals
  let decimalValue = null
  let decimalResult = contract.try_decimals()
  if (!decimalResult.reverted) {
    decimalValue = decimalResult.value
  }

  return BigInt.fromI32(decimalValue as i32)
}
