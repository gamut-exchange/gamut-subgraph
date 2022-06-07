import { Address, BigDecimal } from '@graphprotocol/graph-ts'
import { FACTORY_ADDRESS, ZERO_BI, ZERO_BD, ONE_BI, ADDRESS_ZERO } from './../utils/constants'
import { Bundle, SwapData, JoinExitPoolData, Factory, Pool, Token, Swap, Join, Exit } from '../../generated/schema'
import { Factory as FactoryABI } from '../../generated/Factory/Factory'
import { Swap as SwapEvent, PoolBalanceChanged as JoinExitPoolEvent } from '../../generated/Router/Router'
import { Pool as PoolABI } from '../../generated/Factory/Pool'
import { convertTokenToDecimal, loadTransactionFromEvent, safeDiv } from '../utils'
import { findEthPerToken, getEthPriceInUSD, getTrackedAmountUSD, getTokenRatio, getTokenPrice } from '../utils/pricing'
import { log } from '@graphprotocol/graph-ts'
import {
  updatePoolDayDataFromEvent,
  updatePoolHourDataFromEvent,
  updateTokenDayDataFromEvent,
  updateTokenHourDataFromEvent,
  updateGamutDayDataFromEvent
} from '../utils/intervalUpdates'

export function handleSwapEvent(event: SwapEvent): void {
  let bundle = Bundle.load('1')
  if (bundle === null) {
    log.error('bundle is not existed', [])
    return
  }
  let swapData = new SwapData(event.transaction.hash.toHexString() + "-" + event.logIndex.toString())
  swapData.tokenIn = event.params.tokenIn
  swapData.tokenOut = event.params.tokenOut
  swapData.amountIn = event.params.amountIn
  swapData.amountOut = event.params.amountOut
  swapData.protocolSwapFeeAmount = event.params.protocolSwapFeeAmount
  swapData.sender = event.transaction.from

  swapData.block = event.block.hash
  swapData.timestamp = event.block.timestamp

  swapData.transaction = event.transaction.hash

  // get pool address
  let factory = Factory.load(FACTORY_ADDRESS)
  if (factory === null) {
    log.error('factory is not existed', [])
    return
  }
  let factoryContract = FactoryABI.bind(Address.fromString(FACTORY_ADDRESS))

  let poolAddress = factoryContract.getPool(event.params.tokenIn, event.params.tokenOut)
  swapData.pool = poolAddress

  let pool = Pool.load(poolAddress.toHexString())
  if (pool === null) {
    log.error('pool is not existed', [])
    return
  }
  let poolContract = PoolABI.bind(poolAddress)
  let weights = poolContract.getWeights()
  pool.weight0 = weights[0].toBigDecimal()
  pool.weight1 = weights[1].toBigDecimal()

  let token0 = Token.load(pool.token0)
  let token1 = Token.load(pool.token1)

  if (token0 === null || token1 === null) {
    log.error('token is not existed', [])
    return
  }

  // amounts - 0/1 are token deltas: can be positive or negative
  let amount0 = token0.id == event.params.tokenIn.toHexString() ? convertTokenToDecimal(event.params.amountIn) : convertTokenToDecimal(event.params.amountOut.plus(event.params.protocolSwapFeeAmount)).times(BigDecimal.fromString('-1'))
  let amount1 = token1.id == event.params.tokenIn.toHexString() ? convertTokenToDecimal(event.params.amountIn) : convertTokenToDecimal(event.params.amountOut.plus(event.params.protocolSwapFeeAmount)).times(BigDecimal.fromString('-1'))

  // need absolute amounts for volume
  let amount0Abs = amount0
  if (amount0.lt(ZERO_BD)) {
    amount0Abs = amount0.times(BigDecimal.fromString('-1'))
  }
  let amount1Abs = amount1
  if (amount1.lt(ZERO_BD)) {
    amount1Abs = amount1.times(BigDecimal.fromString('-1'))
  }


  let amount0ETH = amount0Abs.times(token0.derivedETH)
  let amount1ETH = amount1Abs.times(token1.derivedETH)
  let amount0USD = amount0ETH.times(bundle.ethPriceUSD)
  let amount1USD = amount1ETH.times(bundle.ethPriceUSD)

  // get amount that should be tracked only - div 2 because cant count both input and output as volume
  let amountTotalUSDTracked = getTrackedAmountUSD(amount0Abs, token0 as Token, amount1Abs, token1 as Token).div(
    BigDecimal.fromString('2')
  )
  let amountTotalETHTracked = safeDiv(amountTotalUSDTracked, bundle.ethPriceUSD)
  let amountTotalUSDUntracked = amount0USD.plus(amount1USD).div(BigDecimal.fromString('2'))

  let feesETH = amountTotalETHTracked.times(pool.feeTier.toBigDecimal()).div(BigDecimal.fromString('100000000000000000'))
  let feesUSD = amountTotalUSDTracked.times(pool.feeTier.toBigDecimal()).div(BigDecimal.fromString('100000000000000000'))

  // global updates
  factory.txCount = factory.txCount.plus(ONE_BI)
  factory.totalVolumeETH = factory.totalVolumeETH.plus(amountTotalETHTracked)
  factory.totalVolumeUSD = factory.totalVolumeUSD.plus(amountTotalUSDTracked)
  factory.untrackedVolumeUSD = factory.untrackedVolumeUSD.plus(amountTotalUSDUntracked)
  factory.totalFeesETH = factory.totalFeesETH.plus(feesETH)
  factory.totalFeesUSD = factory.totalFeesUSD.plus(feesUSD)

  // reset aggregate tvl before individual pool tvl updates
  let currentPoolTvlETH = pool.totalValueLockedETH
  factory.totalValueLockedETH = factory.totalValueLockedETH.minus(currentPoolTvlETH)

  // pool volume
  pool.volumeToken0 = pool.volumeToken0.plus(amount0Abs)
  pool.volumeToken1 = pool.volumeToken1.plus(amount1Abs)
  pool.volumeUSD = pool.volumeUSD.plus(amountTotalUSDTracked)
  pool.untrackedVolumeUSD = pool.untrackedVolumeUSD.plus(amountTotalUSDUntracked)
  pool.feesUSD = pool.feesUSD.plus(feesUSD)
  pool.txCount = pool.txCount.plus(ONE_BI)

  // Update the pool.
  pool.totalValueLockedToken0 = pool.totalValueLockedToken0.plus(amount0)
  pool.totalValueLockedToken1 = pool.totalValueLockedToken1.plus(amount1)

  // update token0 data
  token0.volume = token0.volume.plus(amount0Abs)
  token0.totalValueLocked = token0.totalValueLocked.plus(amount0)
  token0.volumeUSD = token0.volumeUSD.plus(amountTotalUSDTracked)
  token0.untrackedVolumeUSD = token0.untrackedVolumeUSD.plus(amountTotalUSDUntracked)
  token0.feesUSD = token0.feesUSD.plus(feesUSD)
  token0.txCount = token0.txCount.plus(ONE_BI)

  // update token1 data
  token1.volume = token1.volume.plus(amount1Abs)
  token1.totalValueLocked = token1.totalValueLocked.plus(amount1)
  token1.volumeUSD = token1.volumeUSD.plus(amountTotalUSDTracked)
  token1.untrackedVolumeUSD = token1.untrackedVolumeUSD.plus(amountTotalUSDUntracked)
  token1.feesUSD = token1.feesUSD.plus(feesUSD)
  token1.txCount = token1.txCount.plus(ONE_BI)

  // // updated pool ratess
  // let prices = getTokenPrices(Address.fromString(pool.id), token0 as Token, token1 as Token)
  // pool.token0Price = prices[0]
  // pool.token1Price = prices[1]
  // pool.save()

  // update USD pricing
  bundle.ethPriceUSD = getEthPriceInUSD()
  bundle.save()

  // updated pool ratess
  let ratio = getTokenRatio(Address.fromString(pool.id), token0 as Token, token1 as Token)
  pool.ratio = ratio

  pool.save()

  // update token prices
  token0.derivedETH = findEthPerToken(token0 as Token)
  token1.derivedETH = findEthPerToken(token1 as Token)

  token0.save()
  token1.save()

  pool.token0Price = getTokenPrice(token0 as Token)
  pool.token1Price = getTokenPrice(token1 as Token)

  /**
   * Things afffected by new USD rates
   */
  pool.totalValueLockedETH = pool.totalValueLockedToken0
    .times(token0.derivedETH)
    .plus(pool.totalValueLockedToken1.times(token1.derivedETH))
  pool.totalValueLockedUSD = pool.totalValueLockedETH.times(bundle.ethPriceUSD)

  factory.totalValueLockedETH = factory.totalValueLockedETH.plus(pool.totalValueLockedETH)
  factory.totalValueLockedUSD = factory.totalValueLockedETH.times(bundle.ethPriceUSD)

  token0.totalValueLockedUSD = token0.totalValueLocked.times(token0.derivedETH).times(bundle.ethPriceUSD)
  token1.totalValueLockedUSD = token1.totalValueLocked.times(token1.derivedETH).times(bundle.ethPriceUSD)

  // create Swap call
  let transaction = loadTransactionFromEvent(event.transaction, event.block)
  let swap = new Swap(event.transaction.hash.toHexString() + '#' + pool.txCount.toString())
  swap.transaction = transaction.id
  swap.timestamp = transaction.timestamp
  swap.pool = pool.id
  swap.token0 = pool.token0
  swap.token1 = pool.token1
  swap.amount0 = amount0
  swap.amount1 = amount1
  swap.amountUSD = amountTotalUSDTracked
  swap.sender = event.transaction.from
  swap.origin = Address.fromString(ADDRESS_ZERO)

  // interval data
  let gamutDayData = updateGamutDayDataFromEvent(event.transaction, event.block)
  let poolDayData = updatePoolDayDataFromEvent(poolAddress, event.transaction, event.block)
  let poolHourData = updatePoolHourDataFromEvent(poolAddress, event.transaction, event.block)
  let token0DayData = updateTokenDayDataFromEvent(token0 as Token, event.transaction, event.block)
  let token1DayData = updateTokenDayDataFromEvent(token1 as Token, event.transaction, event.block)
  let token0HourData = updateTokenHourDataFromEvent(token0 as Token, event.transaction, event.block)
  let token1HourData = updateTokenHourDataFromEvent(token1 as Token, event.transaction, event.block)

  // update volume metrics
  gamutDayData.volumeETH = gamutDayData.volumeETH.plus(amountTotalETHTracked)
  gamutDayData.volumeUSD = gamutDayData.volumeUSD.plus(amountTotalUSDTracked)
  gamutDayData.feesUSD = gamutDayData.feesUSD.plus(feesUSD)

  poolDayData.volumeUSD = poolDayData.volumeUSD.plus(amountTotalUSDTracked)
  poolDayData.volumeToken0 = poolDayData.volumeToken0.plus(amount0Abs)
  poolDayData.volumeToken1 = poolDayData.volumeToken1.plus(amount1Abs)
  poolDayData.feesUSD = poolDayData.feesUSD.plus(feesUSD)

  poolHourData.volumeUSD = poolHourData.volumeUSD.plus(amountTotalUSDTracked)
  poolHourData.volumeToken0 = poolHourData.volumeToken0.plus(amount0Abs)
  poolHourData.volumeToken1 = poolHourData.volumeToken1.plus(amount1Abs)
  poolHourData.feesUSD = poolHourData.feesUSD.plus(feesUSD)

  token0DayData.volume = token0DayData.volume.plus(amount0Abs)
  token0DayData.volumeUSD = token0DayData.volumeUSD.plus(amountTotalUSDTracked)
  token0DayData.untrackedVolumeUSD = token0DayData.untrackedVolumeUSD.plus(amountTotalUSDTracked)
  token0DayData.feesUSD = token0DayData.feesUSD.plus(feesUSD)

  token0HourData.volume = token0HourData.volume.plus(amount0Abs)
  token0HourData.volumeUSD = token0HourData.volumeUSD.plus(amountTotalUSDTracked)
  token0HourData.untrackedVolumeUSD = token0HourData.untrackedVolumeUSD.plus(amountTotalUSDTracked)
  token0HourData.feesUSD = token0HourData.feesUSD.plus(feesUSD)

  token1DayData.volume = token1DayData.volume.plus(amount1Abs)
  token1DayData.volumeUSD = token1DayData.volumeUSD.plus(amountTotalUSDTracked)
  token1DayData.untrackedVolumeUSD = token1DayData.untrackedVolumeUSD.plus(amountTotalUSDTracked)
  token1DayData.feesUSD = token1DayData.feesUSD.plus(feesUSD)

  token1HourData.volume = token1HourData.volume.plus(amount1Abs)
  token1HourData.volumeUSD = token1HourData.volumeUSD.plus(amountTotalUSDTracked)
  token1HourData.untrackedVolumeUSD = token1HourData.untrackedVolumeUSD.plus(amountTotalUSDTracked)
  token1HourData.feesUSD = token1HourData.feesUSD.plus(feesUSD)

  swap.save()
  token0DayData.save()
  token1DayData.save()
  gamutDayData.save()
  poolDayData.save()
  factory.save()
  token0.save()
  token1.save()
  pool.save()
  swapData.save()
}

export function handleJoinExitPool(event: JoinExitPoolEvent): void {
  let sender = event.params.liquidityProvider
  let tokens = event.params.tokens
  let deltas = event.params.deltas
  let protocolFeeAmounts = event.params.protocolFeeAmounts
  let joinExitPoolData = new JoinExitPoolData(event.transaction.hash.toHexString() + "-" + event.logIndex.toString())
  joinExitPoolData.sender = sender
  joinExitPoolData.token0 = tokens[0]
  joinExitPoolData.token1 = tokens[1]
  joinExitPoolData.amount0 = deltas[0].abs()
  joinExitPoolData.amount1 = deltas[1].abs()
  joinExitPoolData.feeAmount0 = protocolFeeAmounts[0]
  joinExitPoolData.feeAmount1 = protocolFeeAmounts[1]
  joinExitPoolData.isJoin = deltas[0].gt(ZERO_BI) ? true : false

  // get pool address
  let factory = Factory.load(FACTORY_ADDRESS)
  if (factory === null) {
    log.error('factory is not existed', [])
    return
  }
  let factoryContract = FactoryABI.bind(Address.fromString(FACTORY_ADDRESS))

  let poolAddress = factoryContract.getPool(tokens[0], tokens[1])
  joinExitPoolData.pool = poolAddress

  let bundle = Bundle.load('1')
  if (bundle === null) {
    bundle = new Bundle('1')
  }
  bundle.ethPriceUSD = getEthPriceInUSD()
  bundle.save()

  let pool = Pool.load(poolAddress.toHexString())
  if (pool === null) {
    log.error('pool is not existed', [])
    return
  }
  let poolContract = PoolABI.bind(poolAddress)
  let weights = poolContract.getWeights()
  pool.weight0 = weights[0].toBigDecimal()
  pool.weight1 = weights[1].toBigDecimal()
  pool.feeTier = poolContract.getSwapFeePercentage()
  pool.observationIndex = pool.observationIndex.plus(ONE_BI)
  pool.collectedFeesToken0 = pool.collectedFeesToken0.plus(convertTokenToDecimal(protocolFeeAmounts[0]))
  pool.collectedFeesToken1 = pool.collectedFeesToken1.plus(convertTokenToDecimal(protocolFeeAmounts[1]))

  let token0 = Token.load(pool.token0)
  let token1 = Token.load(pool.token1)
  if (token0 === null || token1 === null) {
    log.error('token is not existed', [])
    return
  }
  let amount0 = convertTokenToDecimal(joinExitPoolData.amount0)
  let amount1 = convertTokenToDecimal(joinExitPoolData.amount1)

  pool.collectedFeesUSD = pool.collectedFeesUSD
    .plus(convertTokenToDecimal(protocolFeeAmounts[0]).times(token0.derivedETH.times(bundle.ethPriceUSD)))
    .plus(convertTokenToDecimal(protocolFeeAmounts[1]).times(token1.derivedETH.times(bundle.ethPriceUSD)))

  let amountUSD = amount0
    .times(token0.derivedETH.times(bundle.ethPriceUSD))
    .plus(amount1.times(token1.derivedETH.times(bundle.ethPriceUSD)))

  // reset tvl aggregates until new amounts calculated
  factory.totalValueLockedETH = factory.totalValueLockedETH.minus(pool.totalValueLockedETH)

  // update globals
  factory.txCount = factory.txCount.plus(ONE_BI)

  // update token0 data
  token0.txCount = token0.txCount.plus(ONE_BI)
  if (joinExitPoolData.isJoin === true) {
    token0.totalValueLocked = token0.totalValueLocked.plus(amount0).minus(convertTokenToDecimal(joinExitPoolData.feeAmount0))
  } else {
    token0.totalValueLocked = token0.totalValueLocked.minus(amount0).minus(convertTokenToDecimal(joinExitPoolData.feeAmount0))
  }
  token0.totalValueLockedUSD = token0.totalValueLocked.times(token0.derivedETH.times(bundle.ethPriceUSD))

  // update token1 data
  token1.txCount = token1.txCount.plus(ONE_BI)
  if (joinExitPoolData.isJoin === true) {
    token1.totalValueLocked = token1.totalValueLocked.plus(amount1).minus(convertTokenToDecimal(joinExitPoolData.feeAmount1))
  } else {
    token1.totalValueLocked = token1.totalValueLocked.minus(amount1).minus(convertTokenToDecimal(joinExitPoolData.feeAmount1))
  }
  token1.totalValueLockedUSD = token1.totalValueLocked.times(token1.derivedETH.times(bundle.ethPriceUSD))

  // pool data
  pool.txCount = pool.txCount.plus(ONE_BI)

  if (joinExitPoolData.isJoin === true) {
    pool.totalValueLockedToken0 = pool.totalValueLockedToken0.plus(amount0).minus(convertTokenToDecimal(joinExitPoolData.feeAmount0))
  } else {
    pool.totalValueLockedToken0 = pool.totalValueLockedToken0.minus(amount0).minus(convertTokenToDecimal(joinExitPoolData.feeAmount0))
  }
  if (joinExitPoolData.isJoin === true) {
    pool.totalValueLockedToken1 = pool.totalValueLockedToken1.plus(amount1).minus(convertTokenToDecimal(joinExitPoolData.feeAmount1))
  } else {
    pool.totalValueLockedToken1 = pool.totalValueLockedToken1.minus(amount1).minus(convertTokenToDecimal(joinExitPoolData.feeAmount1))
  }

  pool.liquidity = poolContract.totalSupply()

  // updated pool ratess
  let ratio = getTokenRatio(Address.fromString(pool.id), token0 as Token, token1 as Token)
  pool.ratio = ratio

  pool.save()

  // update token prices
  token0.derivedETH = findEthPerToken(token0 as Token)
  token1.derivedETH = findEthPerToken(token1 as Token)

  pool.token0Price = getTokenPrice(token0 as Token)
  pool.token1Price = getTokenPrice(token1 as Token)

  pool.totalValueLockedETH = pool.totalValueLockedToken0
    .times(token0.derivedETH)
    .plus(pool.totalValueLockedToken1.times(token1.derivedETH))
  pool.totalValueLockedUSD = pool.totalValueLockedETH.times(bundle.ethPriceUSD)

  // reset aggregates with new amounts
  factory.totalValueLockedETH = factory.totalValueLockedETH.plus(pool.totalValueLockedETH)
  factory.totalValueLockedUSD = factory.totalValueLockedETH.times(bundle.ethPriceUSD)

  let transaction = loadTransactionFromEvent(event.transaction, event.block)

  if (joinExitPoolData.isJoin === true) {
    let join = new Join(transaction.id + '#' + pool.txCount.toString())
    join.transaction = transaction.id
    join.timestamp = transaction.timestamp
    join.pool = pool.id
    join.token0 = pool.token0
    join.token1 = pool.token1
    join.sender = sender
    join.origin = event.transaction.from
    join.amount = convertTokenToDecimal(poolContract.totalSupply().minus(pool.liquidity))
    join.amount0 = amount0
    join.amount1 = amount1
    join.amountUSD = amountUSD
    join.logIndex = event.transaction.index
    join.save()
  } else {
    let exit = new Exit(transaction.id + '#' + pool.txCount.toString())
    exit.transaction = transaction.id
    exit.timestamp = transaction.timestamp
    exit.pool = pool.id
    exit.token0 = pool.token0
    exit.token1 = pool.token1
    exit.sender = sender
    exit.origin = event.transaction.from
    exit.amount = convertTokenToDecimal(pool.liquidity.minus(poolContract.totalSupply()))
    exit.amount0 = amount0
    exit.amount1 = amount1
    exit.amountUSD = amountUSD
    exit.logIndex = event.transaction.index
    exit.save()
  }
  pool.save()

  updateGamutDayDataFromEvent(event.transaction, event.block)
  updatePoolDayDataFromEvent(poolAddress, event.transaction, event.block)
  updatePoolHourDataFromEvent(poolAddress, event.transaction, event.block)
  updateTokenDayDataFromEvent(token0 as Token, event.transaction, event.block)
  updateTokenDayDataFromEvent(token1 as Token, event.transaction, event.block)
  updateTokenHourDataFromEvent(token0 as Token, event.transaction, event.block)
  updateTokenHourDataFromEvent(token1 as Token, event.transaction, event.block)

  token0.save()
  token1.save()
  pool.save()
  factory.save()

  joinExitPoolData.save()
}