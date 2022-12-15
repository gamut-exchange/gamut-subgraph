import { Address, BigDecimal, BigInt } from '@graphprotocol/graph-ts'
import { FACTORY_ADDRESS, ZERO_BI, ZERO_BD, ONE_BI } from './../utils/constants'
import { Bundle, Factory, Pool, Token, Swap, WeightBalanceData, PoolTokensPrice, Join, Exit } from '../../generated/schema'
import { Factory as FactoryABI } from '../../generated/Factory/Factory'
import { PoolBalanceChanged, Swap as SwapEvent } from '../../generated/Router/Router'
import { Pool as PoolABI } from '../../generated/Factory/Pool'
import { loadTransaction, safeDiv, tokenAmountToDecimal } from '../utils'
import { findBtcPerToken, getBtcPriceInUSD, getTrackedAmountUSD, getTokenPrices } from '../utils/pricing'
import { log } from '@graphprotocol/graph-ts'
import { updateGamutDayData, updatePoolDayData, updatePoolHourData, updateTokenDayData, updateTokenHourData } from '../utils/intervalUpdates'

export function handleSwap(event: SwapEvent): void {
  let bundle = Bundle.load('1')
  if (!bundle) {
    log.error(`handleSwap: bundle isn't defined`, [])
    return
  }
  // let swapEvent = new SwapEvent(event.transaction.hash.toHex() + "-" + event.logIndex.toString())
  // swapEvent.tokenIn = event.params.tokenIn
  // swapEvent.tokenOut = event.params.tokenOut
  // swapEvent.amountIn = event.params.amountIn
  // swapEvent.amountOut = event.params.amountOut
  // swapEvent.protocolSwapFeeAmount = event.params.protocolSwapFeeAmount
  // swapEvent.sender = event.transaction.from
  // swapEvent.block = event.block.hash
  // swapEvent.timestamp = event.block.timestamp
  // swapEvent.transaction = event.transaction.hash
  // swapEvent.save()

  // get pool address
  let factory = Factory.load(FACTORY_ADDRESS)
  if (!factory) {
    log.error(`handleSwap: factory {} isn't defined`, [FACTORY_ADDRESS])
    return
  }
  let factoryContract = FactoryABI.bind(Address.fromString(FACTORY_ADDRESS))

  let poolAddress = factoryContract.getPool(event.params.tokenIn, event.params.tokenOut)
  let pool = Pool.load(poolAddress.toHexString())
  if (!pool) {
    log.error(`handleSwap: pool {} isn't defined`, [poolAddress.toHexString()])
    return
  }
  let poolContract = PoolABI.bind(Address.fromString(poolAddress.toHexString()))
  let weights = poolContract.getWeights()
  pool.weight0 = weights[0].toBigDecimal();
  pool.weight1 = weights[0].toBigDecimal();
  let token0 = Token.load(pool.token0)
  let token1 = Token.load(pool.token1)
  if (!token0 || !token1) return

  let amount0: BigDecimal, amount1: BigDecimal
  if (event.params.tokenIn.toHexString() == pool.token0) {
    // amounts - 0/1 are token deltas: can be positive or negative
    amount0 = tokenAmountToDecimal(event.params.amountIn, token0.decimals)
    amount1 = tokenAmountToDecimal(event.params.amountOut, token1.decimals).times(BigDecimal.fromString('-1'))
  } else if (event.params.tokenIn.toHexString() == pool.token1) {
    // amounts - 0/1 are token deltas: can be positive or negative
    amount0 = tokenAmountToDecimal(event.params.amountOut, token0.decimals).times(BigDecimal.fromString('-1'))
    amount1 = tokenAmountToDecimal(event.params.amountIn, token1.decimals)
  } else {
    log.error("handleSwap: event tokenIn is not mathced with pool tokens", [])
    return
  }

  // need absolute amounts for volume
  let amount0Abs = amount0, amount1Abs = amount1
  if (amount0.lt(ZERO_BD)) {
    amount0Abs = amount0.times(BigDecimal.fromString('-1'))
  }
  if (amount1.lt(ZERO_BD)) {
    amount1Abs = amount1.times(BigDecimal.fromString('-1'))
  }

  let amount0KAVA = amount0Abs.times(token0.derivedBTC)
  let amount1KAVA = amount1Abs.times(token1.derivedBTC)
  let amount0USD = amount0KAVA.times(bundle.KAVAPriceUSD)
  let amount1USD = amount1KAVA.times(bundle.KAVAPriceUSD)

  // get amount that should be tracked only - div 2 because cant count both input and output as volume
  let amountTotalUSDTracked = getTrackedAmountUSD(amount0Abs, token0 as Token, amount1Abs, token1 as Token).div(
    BigDecimal.fromString('2')
  )
  let amountTotalKAVATracked = safeDiv(amountTotalUSDTracked, bundle.KAVAPriceUSD)
  let amountTotalUSDUntracked = amount0USD.plus(amount1USD).div(BigDecimal.fromString('2'))

  let feesKAVA = amountTotalKAVATracked.times(pool.feeTier.toBigDecimal()).div(BigDecimal.fromString('100000000000000000'))
  let feesUSD = amountTotalUSDTracked.times(pool.feeTier.toBigDecimal()).div(BigDecimal.fromString('100000000000000000'))

  // global updates
  factory.txCount = factory.txCount.plus(ONE_BI)
  factory.totalVolumeKAVA = factory.totalVolumeKAVA.plus(amountTotalKAVATracked)
  factory.totalVolumeUSD = factory.totalVolumeUSD.plus(amountTotalUSDTracked)
  factory.untrackedVolumeUSD = factory.untrackedVolumeUSD.plus(amountTotalUSDUntracked)
  factory.totalFeesKAVA = factory.totalFeesKAVA.plus(feesKAVA)
  factory.totalFeesUSD = factory.totalFeesUSD.plus(feesUSD)
  
  // reset aggregate tvl before individual pool tvl updates
  let currentPoolTvlKAVA = pool.totalValueLockedKAVA
  factory.totalValueLockedKAVA = factory.totalValueLockedKAVA.minus(currentPoolTvlKAVA)
  
  // pool volume
  pool.volumeToken0 = pool.volumeToken0.plus(amount0Abs)
  pool.volumeToken1 = pool.volumeToken1.plus(amount1Abs)
  pool.volumeUSD = pool.volumeUSD.plus(amountTotalUSDTracked)
  pool.untrackedVolumeUSD = pool.untrackedVolumeUSD.plus(amountTotalUSDUntracked)
  pool.feesUSD = pool.feesUSD.plus(feesUSD)
  pool.txCount = pool.txCount.plus(ONE_BI)
  
  // Update the pool.
  let poolBalances = poolContract.getPoolBalancesAndChangeBlock()
  pool.totalValueLockedToken0 = tokenAmountToDecimal(poolBalances.getBalance0(), token0.decimals)
  pool.totalValueLockedToken1 = tokenAmountToDecimal(poolBalances.getBalance1(), token1.decimals)
  
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
  
  // updated pool ratess
  let prices = getTokenPrices(Address.fromString(pool.id), token0 as Token, token1 as Token)
  pool.token0Price = prices[0]
  pool.token1Price = prices[1]
  pool.save()
  
  // update USD pricing
  bundle.KAVAPriceUSD = getBtcPriceInUSD()
  bundle.save()
  token0.derivedBTC = findBtcPerToken(token0 as Token)
  token1.derivedBTC = findBtcPerToken(token1 as Token)
  
  /**
   * Things afffected by new USD rates
   */
  pool.totalValueLockedKAVA = pool.totalValueLockedToken0
    .times(token0.derivedBTC)
    .plus(pool.totalValueLockedToken1.times(token1.derivedBTC))
  pool.totalValueLockedUSD = pool.totalValueLockedKAVA.times(bundle.KAVAPriceUSD)
  pool.save()
  
  factory.totalValueLockedKAVA = factory.totalValueLockedKAVA.plus(pool.totalValueLockedKAVA)
  factory.totalValueLockedUSD = factory.totalValueLockedKAVA.times(bundle.KAVAPriceUSD)
  
  token0.totalValueLockedUSD = token0.totalValueLocked.times(token0.derivedBTC).times(bundle.KAVAPriceUSD)
  token1.totalValueLockedUSD = token1.totalValueLocked.times(token1.derivedBTC).times(bundle.KAVAPriceUSD)
  
  // create Swap call
  let transaction = loadTransaction(event)
  let swap = new Swap(transaction.id + '#' + pool.txCount.toString())
  swap.transaction = transaction.id
  swap.block = event.block.number
  swap.timestamp = transaction.timestamp
  swap.pool = pool.id
  swap.token0 = pool.token0
  swap.token1 = pool.token1
  swap.amount0 = amount0
  swap.amount1 = amount1
  swap.amountUSD = amountTotalUSDTracked
  swap.sender = event.transaction.from
  swap.recipient = event.transaction.from
  swap.protocolSwapFeeAmount = event.params.protocolSwapFeeAmount
  
  // interval data
  let gamutDayData = updateGamutDayData(event)
  if (!gamutDayData) {
    log.error(`handleSwap: gamutDayData {} isn't defined`, [poolAddress.toHexString()])
    return
  }
  let poolDayData = updatePoolDayData(event, poolAddress)
  if (!poolDayData) {
    log.error(`handleSwap: poolDayData {} isn't defined`, [poolAddress.toHexString()])
    return
  }
  let poolHourData = updatePoolHourData(event, poolAddress)
  if (!poolHourData) {
    log.error(`handleSwap: poolHourData {} isn't defined`, [poolAddress.toHexString()])
    return
  }
  let token0DayData = updateTokenDayData(token0 as Token, event)
  let token1DayData = updateTokenDayData(token1 as Token, event)
  let token0HourData = updateTokenHourData(token0 as Token, event)
  let token1HourData = updateTokenHourData(token1 as Token, event)
  
  // update volume metrics`
  gamutDayData.volumeBTC = gamutDayData.volumeBTC.plus(amountTotalKAVATracked)
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
  pool.save()
  token0.save()
  token1.save()
  
  let weightBalanceData = WeightBalanceData.load(poolAddress.toHexString() + '#' + event.block.timestamp.toString())
  if (!weightBalanceData) {
    weightBalanceData = new WeightBalanceData(poolAddress.toHexString() + '#' + event.block.timestamp.toString())
    weightBalanceData.pool = poolAddress.toHexString()
    weightBalanceData.token0 = token0.id
    weightBalanceData.token1 = token1.id
    weightBalanceData.weight0 = ZERO_BD
    weightBalanceData.weight1 = ZERO_BD
    weightBalanceData.timestamp = ZERO_BI
  }
  weightBalanceData.weight0 = pool.weight0.div(pool.weight0.plus(pool.weight1))
  weightBalanceData.weight1 = pool.weight1.div(pool.weight0.plus(pool.weight1))
  weightBalanceData.timestamp = event.block.timestamp
  weightBalanceData.save()
  
  let poolTokensPrice = PoolTokensPrice.load(poolAddress.toHexString() + '#' + event.block.timestamp.toString())
  if (!poolTokensPrice) {
    poolTokensPrice = new PoolTokensPrice(poolAddress.toHexString() + '#' + event.block.timestamp.toString())
    poolTokensPrice.pool = poolAddress.toHexString()
    poolTokensPrice.token0 = token0.id
    poolTokensPrice.token1 = token1.id
    poolTokensPrice.token0Price = ZERO_BD
    poolTokensPrice.token1Price = ZERO_BD
    poolTokensPrice.timestamp = ZERO_BI
  }
  poolTokensPrice.token0Price = pool.token0Price
  poolTokensPrice.token1Price = pool.token1Price
  poolTokensPrice.timestamp = event.block.timestamp
  poolTokensPrice.save()
}

export function handleJoinExitPool(event: PoolBalanceChanged): void {
  let bundle = Bundle.load('1')
  if (!bundle) {
    log.error(`handleSwap: bundle isn't defined`,[])
    return
  }
  let sender = event.params.liquidityProvider
  let deltas = event.params.deltas
  let protocolFeeAmounts = event.params.protocolFeeAmounts

  let factory = Factory.load(FACTORY_ADDRESS)
  if (!factory) {
    log.error(`handleSwap: factory {} isn't defined`, [FACTORY_ADDRESS])
    return
  }
  let factoryContract = FactoryABI.bind(Address.fromString(FACTORY_ADDRESS))
  let poolAddress = factoryContract.getPool(event.params.tokens[0], event.params.tokens[1])
  let pool = Pool.load(poolAddress.toHexString())
  if (pool == null) {
    log.error(`handleJoinPool: pool {} isn't defined`, [poolAddress.toHexString()])
    return
  }
  let poolContract = PoolABI.bind(Address.fromString(poolAddress.toHexString()))
  let weights = poolContract.getWeights()
  pool.weight0 = weights[0].toBigDecimal()
  pool.weight1 = weights[1].toBigDecimal()
  let poolBalances = poolContract.getPoolBalancesAndChangeBlock()
  let joinLiquidity = tokenAmountToDecimal(poolContract.totalSupply(), BigInt.fromI32(poolContract.decimals())).minus(pool.liquidity)
  pool.liquidity = tokenAmountToDecimal(poolContract.totalSupply(), BigInt.fromI32(poolContract.decimals()))
  let token0 = Token.load(pool.token0)
  let token1 = Token.load(pool.token1)
  if (!token0 || !token1) return
  let amount0: BigDecimal, amount1: BigDecimal
  if (pool.token0 == event.params.tokens[0].toHexString()) {
    amount0 = tokenAmountToDecimal(deltas[0], token0.decimals)
    amount1 = tokenAmountToDecimal(deltas[1], token1.decimals)
  } else if (pool.token1 == event.params.tokens[1].toHexString()) {
    amount0 = tokenAmountToDecimal(deltas[1], token0.decimals)
    amount1 = tokenAmountToDecimal(deltas[0], token1.decimals)
  } else {
    log.error("handleJoinExitPool: pool tokens not match", [])
    return
  }
  // reset tvl aggregates until new amounts calculated
  factory.totalValueLockedKAVA = factory.totalValueLockedKAVA.minus(pool.totalValueLockedKAVA)
  // update globals
  factory.txCount = factory.txCount.plus(ONE_BI)
  // update USD pricing
  // updated pool ratess
  let prices = getTokenPrices(Address.fromString(pool.id), token0 as Token, token1 as Token)
  pool.token0Price = prices[0]
  pool.token1Price = prices[1]
  pool.save()
  bundle.KAVAPriceUSD = getBtcPriceInUSD()
  bundle.save()
  token0.derivedBTC = findBtcPerToken(token0 as Token)
  token1.derivedBTC = findBtcPerToken(token1 as Token)
  let amountUSD = amount0
    .times(token0.derivedBTC.times(bundle.KAVAPriceUSD))
    .plus(amount1.times(token1.derivedBTC.times(bundle.KAVAPriceUSD)))

  // update token0 data
  token0.txCount = token0.txCount.plus(ONE_BI)
  token0.totalValueLocked = token0.totalValueLocked.plus(amount0)
  token0.totalValueLockedUSD = token0.totalValueLocked.times(token0.derivedBTC.times(bundle.KAVAPriceUSD))

  // update token1 data
  token1.txCount = token1.txCount.plus(ONE_BI)
  token1.totalValueLocked = token1.totalValueLocked.plus(amount1)
  token1.totalValueLockedUSD = token1.totalValueLocked.times(token1.derivedBTC.times(bundle.KAVAPriceUSD))

  // pool data
  pool.txCount = pool.txCount.plus(ONE_BI)

  pool.totalValueLockedToken0 = tokenAmountToDecimal(poolBalances.getBalance0(), token0.decimals)
  pool.totalValueLockedToken1 = tokenAmountToDecimal(poolBalances.getBalance1(), token1.decimals)
  pool.totalValueLockedKAVA = pool.totalValueLockedToken0
    .times(token0.derivedBTC)
    .plus(pool.totalValueLockedToken1.times(token1.derivedBTC))
  pool.totalValueLockedUSD = pool.totalValueLockedKAVA.times(bundle.KAVAPriceUSD)
  pool.save()

  // reset aggregates with new amounts
  factory.totalValueLockedKAVA = factory.totalValueLockedKAVA.plus(pool.totalValueLockedKAVA)
  factory.totalValueLockedUSD = factory.totalValueLockedKAVA.times(bundle.KAVAPriceUSD)

  let transaction = loadTransaction(event)
  let recipient = sender
  let timestamp = event.block.timestamp
  if (deltas[0].gt(ZERO_BI)) {
    let join = new Join(transaction.id.toString() + '#' + pool.txCount.toString())
    join.transaction = event.transaction.hash.toHexString()
    join.pool = pool.id
    join.sender = sender
    join.receiver = recipient
    join.protocolSwapFeePercentage = pool.feeTier
    join.amountsIn = deltas
    join.protocolSwapFeeAmount = protocolFeeAmounts
    join.timestamp = timestamp
    join.token0 = pool.token0
    join.token1 = pool.token1
    join.amountUSD = amountUSD
    join.logIndex = event.transaction.index
    join.liquidity = joinLiquidity
    join.save()
  } else {
    let exit = new Exit(transaction.id.toString() + '#' + pool.txCount.toString())
    exit.transaction = event.transaction.hash.toHexString()
    exit.pool = pool.id
    exit.sender = sender
    exit.receiver = recipient
    exit.protocolSwapFeePercentage = pool.feeTier
    exit.amountsOut = deltas
    exit.protocolSwapFeeAmount = protocolFeeAmounts
    exit.timestamp = timestamp
    exit.token0 = pool.token0
    exit.token1 = pool.token1
    exit.amountUSD = amountUSD
    exit.logIndex = event.transaction.index
    exit.liquidity = joinLiquidity
    exit.save()

  }

  // let poolToken = ERC20.bind(call.to)
  // join.poolTokenBalance = poolToken.balanceOf(sender)
  updateGamutDayData(event)
  updatePoolDayData(event, poolAddress)
  updatePoolHourData(event, poolAddress)
  updateTokenDayData(token0 as Token, event)
  updateTokenDayData(token1 as Token, event)
  updateTokenHourData(token0 as Token, event)
  updateTokenHourData(token1 as Token, event)

  token0.save()
  token1.save()
  pool.save()
  factory.save()

  let weightBalanceData = WeightBalanceData.load(poolAddress.toHexString() + '#' + event.block.timestamp.toString())
  if (!weightBalanceData) {
    weightBalanceData = new WeightBalanceData(poolAddress.toHexString() + '#' + event.block.timestamp.toString())
    weightBalanceData.pool = poolAddress.toHexString()
    weightBalanceData.token0 = token0.id
    weightBalanceData.token1 = token1.id
    weightBalanceData.weight0 = ZERO_BD
    weightBalanceData.weight1 = ZERO_BD
    weightBalanceData.timestamp = ZERO_BI
  }
  weightBalanceData.weight0 = pool.weight0.div(pool.weight0.plus(pool.weight1))
  weightBalanceData.weight1 = pool.weight1.div(pool.weight0.plus(pool.weight1))
  weightBalanceData.timestamp = event.block.timestamp
  weightBalanceData.save()

  let poolTokensPrice = PoolTokensPrice.load(poolAddress.toHexString() + '#' + event.block.timestamp.toString())
  if (!poolTokensPrice) {
    poolTokensPrice = new PoolTokensPrice(poolAddress.toHexString() + '#' + event.block.timestamp.toString())
    poolTokensPrice.pool = poolAddress.toHexString()
    poolTokensPrice.token0 = token0.id
    poolTokensPrice.token1 = token1.id
    poolTokensPrice.token0Price = ZERO_BD
    poolTokensPrice.token1Price = ZERO_BD
    poolTokensPrice.timestamp = ZERO_BI
  }
  poolTokensPrice.token0Price = pool.token0Price
  poolTokensPrice.token1Price = pool.token1Price
  poolTokensPrice.timestamp = event.block.timestamp
  poolTokensPrice.save()
}