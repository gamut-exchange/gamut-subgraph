import { ZERO_BD, ZERO_BI, ONE_BI } from './constants'
/* eslint-disable prefer-const */
import {
  GamutDayData,
  Factory,
  Pool,
  PoolDayData,
  Token,
  TokenDayData,
  TokenHourData,
  Bundle,
  PoolHourData
} from './../../generated/schema'
import { FACTORY_ADDRESS } from './constants'
import { BigInt, ethereum, log } from '@graphprotocol/graph-ts'
import { Pool as PoolABI } from '../../generated/Factory/Pool'
import { tokenAmountToDecimal } from '.'

/**
 * Tracks global aggregate data over daily windows
 * @param event
 */
export function updateGamutDayData(call: ethereum.Call): GamutDayData | null {
  let gamut = Factory.load(FACTORY_ADDRESS)
  if (!gamut) {
    log.error(`updateGamutDayData: factory {} isn't defind`, [FACTORY_ADDRESS])
    return null
  }
  let timestamp = call.block.timestamp.toI32()
  let dayID = timestamp / 86400 // rounded
  let dayStartTimestamp = dayID * 86400
  let gamutDayData = GamutDayData.load(dayID.toString())
  if (gamutDayData === null) {
    gamutDayData = new GamutDayData(dayID.toString())
    gamutDayData.date = dayStartTimestamp
    gamutDayData.volumeBTC = ZERO_BD
    gamutDayData.volumeUSD = ZERO_BD
    gamutDayData.volumeUSDUntracked = ZERO_BD
    gamutDayData.feesUSD = ZERO_BD
  }
  gamutDayData.tvlUSD = gamut.totalValueLockedUSD
  gamutDayData.txCount = gamut.txCount
  gamutDayData.save()
  return gamutDayData as GamutDayData
}

export function updatePoolDayData(call: ethereum.Call, isSwap: bool): PoolDayData | null {
  let timestamp = call.block.timestamp.toI32()
  let dayID = timestamp / 86400
  let dayStartTimestamp = dayID * 86400
  let dayPoolID = call.to
    .toHexString()
    .concat('-')
    .concat(dayID.toString())
  let pool = Pool.load(call.to.toHexString())
  if (!pool) {
    log.error(`updatePoolDayData: pool {} isn't defind`, [call.to.toHexString()])
    return null
  }
  let poolDayData = PoolDayData.load(dayPoolID)
  if (poolDayData === null) {
    poolDayData = new PoolDayData(dayPoolID)
    poolDayData.date = BigInt.fromI32(dayStartTimestamp)
    poolDayData.pool = pool.id
    // things that dont get initialized always
    poolDayData.volumeToken0 = ZERO_BD
    poolDayData.volumeToken1 = ZERO_BD
    poolDayData.volumeUSD = ZERO_BD
    poolDayData.feesUSD = ZERO_BD
    poolDayData.txCount = ZERO_BI
    poolDayData.open = pool.token0Price
    poolDayData.high = pool.token0Price
    poolDayData.low = pool.token0Price
    poolDayData.close = pool.token0Price
    poolDayData.liquidity = ZERO_BD
    poolDayData.liquidityUSD = ZERO_BD
  }

  if (pool.token0Price.gt(poolDayData.high)) {
    poolDayData.high = pool.token0Price
  }
  if (pool.token0Price.lt(poolDayData.low)) {
    poolDayData.low = pool.token0Price
  }

  poolDayData.token0Price = pool.token0Price
  poolDayData.token1Price = pool.token1Price
  poolDayData.tvlUSD = pool.totalValueLockedUSD
  poolDayData.txCount = poolDayData.txCount.plus(ONE_BI)
  let poolContract = PoolABI.bind(call.to)
  poolDayData.liquidity = tokenAmountToDecimal(poolContract.totalSupply(), BigInt.fromI32(poolContract.decimals()))
  poolDayData.liquidityUSD = pool.totalValueLockedUSD

  poolDayData.save()

  return poolDayData as PoolDayData
}

export function updatePoolHourData(call: ethereum.Call, isSwap: bool): PoolHourData | null {
  let timestamp = call.block.timestamp.toI32()
  let hourIndex = timestamp / 3600 // get unique hour within unix history
  let hourStartUnix = hourIndex * 3600 // want the rounded effect
  let hourPoolID = call.to
    .toHexString()
    .concat('-')
    .concat(hourIndex.toString())
  let pool = Pool.load(call.to.toHexString())
  if (!pool) {
    log.error(`updatePoolHourData: pool {} isn't defind`, [call.to.toHexString()])
    return null
  }
  let poolHourData = PoolHourData.load(hourPoolID)
  if (poolHourData === null) {
    poolHourData = new PoolHourData(hourPoolID)
    poolHourData.periodStartUnix = hourStartUnix
    poolHourData.pool = pool.id
    // things that dont get initialized always
    poolHourData.volumeToken0 = ZERO_BD
    poolHourData.volumeToken1 = ZERO_BD
    poolHourData.volumeUSD = ZERO_BD
    poolHourData.txCount = ZERO_BI
    poolHourData.feesUSD = ZERO_BD
    poolHourData.liquidity = ZERO_BD
    poolHourData.open = pool.token0Price
    poolHourData.high = pool.token0Price
    poolHourData.low = pool.token0Price
    poolHourData.close = pool.token0Price
  }

  if (pool.token0Price.gt(poolHourData.high)) {
    poolHourData.high = pool.token0Price
  }
  if (pool.token0Price.lt(poolHourData.low)) {
    poolHourData.low = pool.token0Price
  }

  
  poolHourData.token0Price = pool.token0Price
  poolHourData.token1Price = pool.token1Price
  poolHourData.close = pool.token0Price
  poolHourData.tvlUSD = pool.totalValueLockedUSD
  poolHourData.txCount = poolHourData.txCount.plus(ONE_BI)
  let poolContract = PoolABI.bind(call.to)
  poolHourData.liquidity = tokenAmountToDecimal(poolContract.totalSupply(), BigInt.fromI32(poolContract.decimals()))
  poolHourData.save()

  // test
  return poolHourData as PoolHourData
}

export function updateTokenDayData(token: Token, call: ethereum.Call): TokenDayData {
  let bundle = Bundle.load('1')
  let timestamp = call.block.timestamp.toI32()
  let dayID = timestamp / 86400
  let dayStartTimestamp = dayID * 86400
  let tokenDayID = token.id
    .toString()
    .concat('-')
    .concat(dayID.toString())
  let tokenPrice = !bundle ? ZERO_BD : token.derivedBTC.times(bundle.btcPriceUSD)

  let tokenDayData = TokenDayData.load(tokenDayID)
  if (tokenDayData === null) {
    tokenDayData = new TokenDayData(tokenDayID)
    tokenDayData.date = dayStartTimestamp
    tokenDayData.token = token.id
    tokenDayData.volume = ZERO_BD
    tokenDayData.volumeUSD = ZERO_BD
    tokenDayData.feesUSD = ZERO_BD
    tokenDayData.untrackedVolumeUSD = ZERO_BD
    tokenDayData.open = tokenPrice
    tokenDayData.high = tokenPrice
    tokenDayData.low = tokenPrice
    tokenDayData.close = tokenPrice
  }

  if (tokenPrice.gt(tokenDayData.high)) {
    tokenDayData.high = tokenPrice
  }

  if (tokenPrice.lt(tokenDayData.low)) {
    tokenDayData.low = tokenPrice
  }

  tokenDayData.close = tokenPrice
  tokenDayData.priceUSD = !bundle ? ZERO_BD : token.derivedBTC.times(bundle.btcPriceUSD)
  tokenDayData.totalValueLocked = token.totalValueLocked
  tokenDayData.totalValueLockedUSD = token.totalValueLockedUSD
  tokenDayData.save()

  return tokenDayData as TokenDayData
}

export function updateTokenHourData(token: Token, call: ethereum.Call): TokenHourData {
  let bundle = Bundle.load('1')
  let timestamp = call.block.timestamp.toI32()
  let hourIndex = timestamp / 3600 // get unique hour within unix history
  let hourStartUnix = hourIndex * 3600 // want the rounded effect
  let tokenHourID = token.id
    .toString()
    .concat('-')
    .concat(hourIndex.toString())
  let tokenHourData = TokenHourData.load(tokenHourID)
  let tokenPrice = !bundle ? ZERO_BD : token.derivedBTC.times(bundle.btcPriceUSD)

  if (tokenHourData === null) {
    tokenHourData = new TokenHourData(tokenHourID)
    tokenHourData.periodStartUnix = hourStartUnix
    tokenHourData.token = token.id
    tokenHourData.volume = ZERO_BD
    tokenHourData.volumeUSD = ZERO_BD
    tokenHourData.untrackedVolumeUSD = ZERO_BD
    tokenHourData.feesUSD = ZERO_BD
    tokenHourData.open = tokenPrice
    tokenHourData.high = tokenPrice
    tokenHourData.low = tokenPrice
    tokenHourData.close = tokenPrice
  }

  if (tokenPrice.gt(tokenHourData.high)) {
    tokenHourData.high = tokenPrice
  }

  if (tokenPrice.lt(tokenHourData.low)) {
    tokenHourData.low = tokenPrice
  }

  tokenHourData.close = tokenPrice
  tokenHourData.priceUSD = tokenPrice
  tokenHourData.totalValueLocked = token.totalValueLocked
  tokenHourData.totalValueLockedUSD = token.totalValueLockedUSD
  tokenHourData.save()

  return tokenHourData as TokenHourData
}
