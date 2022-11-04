/* eslint-disable prefer-const */
import { Bundle, Factory, Pool, Swap, Join, Exit, Token, WeightBalanceData, PoolTokensPrice } from '../../generated/schema'
import { Pool as PoolABI, SwapFeePercentageChanged } from '../../generated/Factory/Pool'
import { Address, BigDecimal, BigInt } from '@graphprotocol/graph-ts'
import {
  SwapFeePercentageChanged as SwapFeeEvent,
  OnSwapCall as SwapCall,
  OnJoinPoolCall as JoinPoolCall,
  OnExitPoolCall as ExitPoolCall
} from '../../generated/templates/Pool/Pool'
import { convertTokenToDecimal, loadTransaction, safeDiv, tokenAmountToDecimal } from '../utils'
import { FACTORY_ADDRESS, ONE_BI, ZERO_BD, ZERO_BI } from '../utils/constants'
import { findBtcPerToken, getBtcPriceInUSD, getTrackedAmountUSD, getTokenPrices } from '../utils/pricing'
import {
  updatePoolDayData,
  updatePoolHourData,
  updateTokenDayData,
  updateTokenHourData,
  updateGamutDayData
} from '../utils/intervalUpdates'
import { log } from '@graphprotocol/graph-ts'
import { ERC20 } from '../../generated/Factory/ERC20'

export function handleSwapFee(event: SwapFeeEvent): void {
  let poolAddress = event.address.toHexString()
  let pool = Pool.load(poolAddress)
  if (!pool) {
    log.error(`handleSwapFee: pool {} isn't defined`, [poolAddress])
    return
  }
  if (!event.params.swapFeePercentage) {
    log.error(`handleSwapFee: swapFeePercentage isn't defined`, [])
  }
  pool.feeTier = event.params.swapFeePercentage
  pool.save()
}

export function handleSwap(call: SwapCall): void {
  let bundle = Bundle.load('1')
  if (!bundle) {
    log.error(`handleSwap: bundle isn't defined`,[])
    return
  }
  let factory = Factory.load(FACTORY_ADDRESS)
  if (!factory) {
    log.error(`handleSwap: factory {} isn't defined`,[FACTORY_ADDRESS])
    return
  }
  // let factoryContract = FactoryABI.bind(Address.fromString(FACTORY_ADDRESS))
  // let poolAddress = factoryContract.getPool(call.inputs.tokenIn, call.from)
  let poolAddress = call.to.toHexString()
  let pool = Pool.load(poolAddress)
  if (!pool) {
    log.error(`handleSwap: pool {} isn't defined`, [poolAddress])
    return
  }
  let poolContract = PoolABI.bind(Address.fromString(poolAddress))
  let weights = poolContract.getWeights()
  pool.weight0 = weights[0].toBigDecimal();
  pool.weight1 = weights[0].toBigDecimal();
  let token0 = Token.load(pool.token0)
  let token1 = Token.load(pool.token1)
  if (!token0 || !token1) return

  // amounts - 0/1 are token deltas: can be positive or negative
  let amount0 = tokenAmountToDecimal(call.inputs.amountIn, token0.decimals)
  let amount1 = tokenAmountToDecimal(call.outputs.value0, token1.decimals)

  // need absolute amounts for volume
  let amount0Abs = amount0
  if (amount0.lt(ZERO_BD)) {
    amount0Abs = amount0.times(BigDecimal.fromString('-1'))
  }
  let amount1Abs = amount1
  if (amount1.lt(ZERO_BD)) {
    amount1Abs = amount1.times(BigDecimal.fromString('-1'))
  }

  let amount0BTC = amount0Abs.times(token0.derivedBTC)
  let amount1BTC = amount1Abs.times(token1.derivedBTC)
  let amount0USD = amount0BTC.times(bundle.btcPriceUSD)
  let amount1USD = amount1BTC.times(bundle.btcPriceUSD)

  // get amount that should be tracked only - div 2 because cant count both input and output as volume
  let amountTotalUSDTracked = getTrackedAmountUSD(amount0Abs, token0 as Token, amount1Abs, token1 as Token).div(
    BigDecimal.fromString('2')
  )
  let amountTotalBTCTracked = safeDiv(amountTotalUSDTracked, bundle.btcPriceUSD)
  let amountTotalUSDUntracked = amount0USD.plus(amount1USD).div(BigDecimal.fromString('2'))

  let feesBTC = amountTotalBTCTracked.times(pool.feeTier.toBigDecimal()).div(BigDecimal.fromString('100000000000000000'))
  let feesUSD = amountTotalUSDTracked.times(pool.feeTier.toBigDecimal()).div(BigDecimal.fromString('100000000000000000'))

  // global updates
  factory.txCount = factory.txCount.plus(ONE_BI)
  factory.totalVolumeBTC = factory.totalVolumeBTC.plus(amountTotalBTCTracked)
  factory.totalVolumeUSD = factory.totalVolumeUSD.plus(amountTotalUSDTracked)
  factory.untrackedVolumeUSD = factory.untrackedVolumeUSD.plus(amountTotalUSDUntracked)
  factory.totalFeesBTC = factory.totalFeesBTC.plus(feesBTC)
  factory.totalFeesUSD = factory.totalFeesUSD.plus(feesUSD)

  // reset aggregate tvl before individual pool tvl updates
  let currentPoolTvlBTC = pool.totalValueLockedBTC
  factory.totalValueLockedBTC = factory.totalValueLockedBTC.minus(currentPoolTvlBTC)

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
  bundle.btcPriceUSD = getBtcPriceInUSD()
  bundle.save()
  token0.derivedBTC = findBtcPerToken(token0 as Token)
  token1.derivedBTC = findBtcPerToken(token1 as Token)

  /**
   * Things afffected by new USD rates
   */
  pool.totalValueLockedBTC = pool.totalValueLockedToken0
  .times(token0.derivedBTC)
  .plus(pool.totalValueLockedToken1.times(token1.derivedBTC))
  pool.totalValueLockedUSD = pool.totalValueLockedBTC.times(bundle.btcPriceUSD)
  pool.save()
  
  factory.totalValueLockedBTC = factory.totalValueLockedBTC.plus(pool.totalValueLockedBTC)
  factory.totalValueLockedUSD = factory.totalValueLockedBTC.times(bundle.btcPriceUSD)

  token0.totalValueLockedUSD = token0.totalValueLocked.times(token0.derivedBTC).times(bundle.btcPriceUSD)
  token1.totalValueLockedUSD = token1.totalValueLocked.times(token1.derivedBTC).times(bundle.btcPriceUSD)

  // create Swap call
  let transaction = loadTransaction(call)
  let swap = new Swap(transaction.id + '#' + pool.txCount.toString())
  swap.transaction = transaction.id
  swap.timestamp = transaction.timestamp
  swap.pool = pool.id
  swap.token0 = pool.token0
  swap.token1 = pool.token1
  swap.amount0 = amount0
  swap.amount1 = amount1
  swap.amountUSD = amountTotalUSDTracked
  swap.sender = call.from
  swap.recipient = call.from

  // interval data
  let gamutDayData = updateGamutDayData(call)
  if (!gamutDayData) {
    log.error(`handleSwap: gamutDayData {} isn't defined`, [poolAddress])
    return
  }
  let poolDayData = updatePoolDayData(call, true)
  if (!poolDayData) {
    log.error(`handleSwap: poolDayData {} isn't defined`, [poolAddress])
    return
  }
  let poolHourData = updatePoolHourData(call, true)
  if (!poolHourData) {
    log.error(`handleSwap: poolHourData {} isn't defined`, [poolAddress])
    return
  }
  let token0DayData = updateTokenDayData(token0 as Token, call)
  let token1DayData = updateTokenDayData(token1 as Token, call)
  let token0HourData = updateTokenHourData(token0 as Token, call)
  let token1HourData = updateTokenHourData(token1 as Token, call)

  // update volume metrics
  gamutDayData.volumeBTC = gamutDayData.volumeBTC.plus(amountTotalBTCTracked)
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

  let weightBalanceData = WeightBalanceData.load(call.to.toHexString() + '#' + call.block.timestamp.toString())
  if (!weightBalanceData) {
    weightBalanceData = new WeightBalanceData(call.to.toHexString() + '#' + call.block.timestamp.toString())
    weightBalanceData.pool = call.to.toHexString()
    weightBalanceData.token0 = token0.id
    weightBalanceData.token1 = token1.id
    weightBalanceData.weight0 = ZERO_BD
    weightBalanceData.weight1 = ZERO_BD
    weightBalanceData.timestamp = ZERO_BI
  }
  weightBalanceData.weight0 = pool.weight0.div(pool.weight0.plus(pool.weight1))
  weightBalanceData.weight1 = pool.weight1.div(pool.weight0.plus(pool.weight1))
  weightBalanceData.timestamp = call.block.timestamp
  weightBalanceData.save()

  let poolTokensPrice = PoolTokensPrice.load(call.to.toHexString() + '#' + call.block.timestamp.toString())
  if (!poolTokensPrice) {
    poolTokensPrice = new PoolTokensPrice(call.to.toHexString() + '#' + call.block.timestamp.toString())
    poolTokensPrice.pool = call.to.toHexString()
    poolTokensPrice.token0 = token0.id
    poolTokensPrice.token1 = token1.id
    poolTokensPrice.token0Price = ZERO_BD
    poolTokensPrice.token1Price = ZERO_BD
    poolTokensPrice.timestamp = ZERO_BI
  }
  poolTokensPrice.token0Price = pool.token0Price
  poolTokensPrice.token1Price = pool.token1Price
  poolTokensPrice.timestamp = call.block.timestamp
  poolTokensPrice.save()
}

export function handleJoinPool(call: JoinPoolCall): void {
  let bundle = Bundle.load('1')
  if (!bundle) {
    log.error(`handleSwap: bundle isn't defined`,[])
    return
  }
  let poolAddress = call.to.toHexString()
  let pool = Pool.load(poolAddress)
  if (pool == null) {
    log.error(`handleJoinPool: pool {} isn't defined`, [poolAddress])
    return
  }
  let factory = Factory.load(FACTORY_ADDRESS)
  if (factory == null) {
    log.error(`factory is not defined`, [])
    return
  }
  let poolContract = PoolABI.bind(Address.fromString(poolAddress))
  let weights = poolContract.getWeights()
  pool.weight0 = weights[0].toBigDecimal()
  pool.weight1 = weights[1].toBigDecimal()
  let poolBalances = poolContract.getPoolBalancesAndChangeBlock()
  let joinLiquidity = tokenAmountToDecimal(poolContract.totalSupply(), BigInt.fromI32(poolContract.decimals())).minus(pool.liquidity)
  pool.liquidity = tokenAmountToDecimal(poolContract.totalSupply(), BigInt.fromI32(poolContract.decimals()))
  let token0 = Token.load(pool.token0)
  let token1 = Token.load(pool.token1)
  if (!token0 || !token1) return
  let amount0 = tokenAmountToDecimal(call.inputs.balances[0], token0.decimals)
  let amount1 = tokenAmountToDecimal(call.inputs.balances[1], token1.decimals)

  // reset tvl aggregates until new amounts calculated
  factory.totalValueLockedBTC = factory.totalValueLockedBTC.minus(pool.totalValueLockedBTC)

  // update globals
  factory.txCount = factory.txCount.plus(ONE_BI)
  
  // update USD pricing
  // updated pool ratess
  let prices = getTokenPrices(Address.fromString(pool.id), token0 as Token, token1 as Token)
  pool.token0Price = prices[0]
  pool.token1Price = prices[1]
  pool.save()
  bundle.btcPriceUSD = getBtcPriceInUSD()
  bundle.save()
  token0.derivedBTC = findBtcPerToken(token0 as Token)
  token1.derivedBTC = findBtcPerToken(token1 as Token)
  let amountUSD = amount0
    .times(token0.derivedBTC.times(bundle.btcPriceUSD))
    .plus(amount1.times(token1.derivedBTC.times(bundle.btcPriceUSD)))

  // update token0 data
  token0.txCount = token0.txCount.plus(ONE_BI)
  token0.totalValueLocked = token0.totalValueLocked.plus(amount0)
  token0.totalValueLockedUSD = token0.totalValueLocked.times(token0.derivedBTC.times(bundle.btcPriceUSD))

  // update token1 data
  token1.txCount = token1.txCount.plus(ONE_BI)
  token1.totalValueLocked = token1.totalValueLocked.plus(amount1)
  token1.totalValueLockedUSD = token1.totalValueLocked.times(token1.derivedBTC.times(bundle.btcPriceUSD))

  // pool data
  pool.txCount = pool.txCount.plus(ONE_BI)

  pool.totalValueLockedToken0 = tokenAmountToDecimal(poolBalances.getBalance0(), token0.decimals)
  pool.totalValueLockedToken1 = tokenAmountToDecimal(poolBalances.getBalance1(), token1.decimals)
  pool.totalValueLockedBTC = pool.totalValueLockedToken0
    .times(token0.derivedBTC)
    .plus(pool.totalValueLockedToken1.times(token1.derivedBTC))
  pool.totalValueLockedUSD = pool.totalValueLockedBTC.times(bundle.btcPriceUSD)
  pool.save()

  // reset aggregates with new amounts
  factory.totalValueLockedBTC = factory.totalValueLockedBTC.plus(pool.totalValueLockedBTC)
  factory.totalValueLockedUSD = factory.totalValueLockedBTC.times(bundle.btcPriceUSD)

  let transaction = loadTransaction(call)
  let sender = call.inputs.sender
  let recipient = call.inputs.recipient
  let balances = call.inputs.balances
  let protocolSwapFeePercentage = call.inputs.protocolSwapFeePercentage
  let userData = call.inputs.userData
  let amountsIn = call.outputs.amountsIn
  let protocolSwapFeeAmount = call.outputs.protocolSwapFeeAmount
  let timestamp = call.block.timestamp
  let join = new Join(transaction.id.toString() + '#' + pool.txCount.toString())
  if (join == null) {
    join = new Join(call.transaction.hash.toHexString())
  }
  join.transaction = call.transaction.hash.toHexString()
  join.pool = pool.id
  join.sender = sender
  join.receiver = recipient
  join.balances = balances
  join.protocolSwapFeePercentage = protocolSwapFeePercentage
  join.userData = userData
  join.amountsIn = amountsIn
  join.protocolSwapFeeAmount = protocolSwapFeeAmount
  join.timestamp = timestamp
  join.token0 = pool.token0
  join.token1 = pool.token1
  join.amountUSD = amountUSD
  join.logIndex = call.transaction.index
  join.liquidity = joinLiquidity

  // let poolToken = ERC20.bind(call.to)
  // join.poolTokenBalance = poolToken.balanceOf(sender)
  updateGamutDayData(call)
  updatePoolDayData(call, false)
  updatePoolHourData(call, false)
  updateTokenDayData(token0 as Token, call)
  updateTokenDayData(token1 as Token, call)
  updateTokenHourData(token0 as Token, call)
  updateTokenHourData(token1 as Token, call)

  token0.save()
  token1.save()
  pool.save()
  factory.save()
  join.save()

  let weightBalanceData = WeightBalanceData.load(call.to.toHexString() + '#' + call.block.timestamp.toString())
  if (!weightBalanceData) {
    weightBalanceData = new WeightBalanceData(call.to.toHexString() + '#' + call.block.timestamp.toString())
    weightBalanceData.pool = call.to.toHexString()
    weightBalanceData.token0 = token0.id
    weightBalanceData.token1 = token1.id
    weightBalanceData.weight0 = ZERO_BD
    weightBalanceData.weight1 = ZERO_BD
    weightBalanceData.timestamp = ZERO_BI
  }
  weightBalanceData.weight0 = pool.weight0.div(pool.weight0.plus(pool.weight1))
  weightBalanceData.weight1 = pool.weight1.div(pool.weight0.plus(pool.weight1))
  weightBalanceData.timestamp = call.block.timestamp
  weightBalanceData.save()

  let poolTokensPrice = PoolTokensPrice.load(call.to.toHexString() + '#' + call.block.timestamp.toString())
  if (!poolTokensPrice) {
    poolTokensPrice = new PoolTokensPrice(call.to.toHexString() + '#' + call.block.timestamp.toString())
    poolTokensPrice.pool = call.to.toHexString()
    poolTokensPrice.token0 = token0.id
    poolTokensPrice.token1 = token1.id
    poolTokensPrice.token0Price = ZERO_BD
    poolTokensPrice.token1Price = ZERO_BD
    poolTokensPrice.timestamp = ZERO_BI
  }
  poolTokensPrice.token0Price = pool.token0Price
  poolTokensPrice.token1Price = pool.token1Price
  poolTokensPrice.timestamp = call.block.timestamp
  poolTokensPrice.save()
}

export function handleExitPool(call: ExitPoolCall): void {
  let bundle = Bundle.load('1')
  if (!bundle) {
    log.error(`handleSwap: bundle isn't defined`,[])
    return
  }
  // update USD pricing
  let poolAddress = call.to.toHexString()
  let pool = Pool.load(poolAddress)
  if (pool == null) {
    log.error(`handleJoinPool: pool {} isn't defined`, [poolAddress])
    return
  }
  let factory = Factory.load(FACTORY_ADDRESS)
  if (factory == null) {
    log.error(`factory is not defined`, [])
    return
  }
  let poolContract = PoolABI.bind(Address.fromString(poolAddress))
  let weights = poolContract.getWeights()
  pool.weight0 = weights[0].toBigDecimal()
  pool.weight1 = weights[1].toBigDecimal()
  let poolBalances = poolContract.getPoolBalancesAndChangeBlock()
  let exitLiquidity = pool.liquidity.minus(tokenAmountToDecimal(poolContract.totalSupply(), BigInt.fromI32(poolContract.decimals())))
  pool.liquidity = tokenAmountToDecimal(poolContract.totalSupply(), BigInt.fromI32(poolContract.decimals()))
  let token0 = Token.load(pool.token0)
  let token1 = Token.load(pool.token1)
  if (!token0 || !token1) return
  let amount0 = tokenAmountToDecimal(call.inputs.balances[0], token0.decimals)
  let amount1 = tokenAmountToDecimal(call.inputs.balances[1], token1.decimals)

  // reset tvl aggregates until new amounts calculated
  factory.totalValueLockedBTC = factory.totalValueLockedBTC.minus(pool.totalValueLockedBTC)

  // update globals
  factory.txCount = factory.txCount.plus(ONE_BI)
  
  // update USD pricing
  // updated pool ratess
  let prices = getTokenPrices(Address.fromString(pool.id), token0 as Token, token1 as Token)
  pool.token0Price = prices[0]
  pool.token1Price = prices[1]
  pool.save()
  bundle.btcPriceUSD = getBtcPriceInUSD()
  bundle.save()
  token0.derivedBTC = findBtcPerToken(token0 as Token)
  token1.derivedBTC = findBtcPerToken(token1 as Token)
  
  let amountUSD = amount0
    .times(token0.derivedBTC.times(bundle.btcPriceUSD))
    .plus(amount1.times(token1.derivedBTC.times(bundle.btcPriceUSD)))

  // update token0 data
  token0.txCount = token0.txCount.plus(ONE_BI)
  token0.totalValueLocked = token0.totalValueLocked.plus(amount0)
  token0.totalValueLockedUSD = token0.totalValueLocked.times(token0.derivedBTC.times(bundle.btcPriceUSD))

  // update token1 data
  token1.txCount = token1.txCount.plus(ONE_BI)
  token1.totalValueLocked = token1.totalValueLocked.plus(amount1)
  token1.totalValueLockedUSD = token1.totalValueLocked.times(token1.derivedBTC.times(bundle.btcPriceUSD))

  // pool data
  pool.txCount = pool.txCount.plus(ONE_BI)

  pool.totalValueLockedToken0 = tokenAmountToDecimal(poolBalances.getBalance0(), token0.decimals)
  pool.totalValueLockedToken1 = tokenAmountToDecimal(poolBalances.getBalance1(), token1.decimals)
  pool.totalValueLockedBTC = pool.totalValueLockedToken0
    .times(token0.derivedBTC)
    .plus(pool.totalValueLockedToken1.times(token1.derivedBTC))
  pool.totalValueLockedUSD = pool.totalValueLockedBTC.times(bundle.btcPriceUSD)
  pool.save()

  // reset aggregates with new amounts
  factory.totalValueLockedBTC = factory.totalValueLockedBTC.plus(pool.totalValueLockedBTC)
  factory.totalValueLockedUSD = factory.totalValueLockedBTC.times(bundle.btcPriceUSD)

  let transaction = loadTransaction(call)
  let sender = call.inputs.sender
  let recipient = call.inputs.recipient
  let balances = call.inputs.balances
  let protocolSwapFeePercentage = call.inputs.protocolSwapFeePercentage
  let userData = call.inputs.userData
  let value0 = call.outputs.value0
  let value1 = call.outputs.value1
  let timestamp = call.block.timestamp
  let exit = new Exit(transaction.id.toString() + '#' + pool.txCount.toString())
  if (exit == null) {
    exit = new Exit(call.transaction.hash.toHexString())
  }
  exit.transaction = call.transaction.hash.toHexString()
  exit.pool = pool.id
  exit.sender = sender
  exit.receiver = recipient
  exit.balances = balances
  exit.protocolSwapFeePercentage = protocolSwapFeePercentage
  exit.userData = userData
  exit.value0 = value0
  exit.value1 = value1
  exit.timestamp = timestamp
  exit.token0 = pool.token0
  exit.token1 = pool.token1
  exit.amountUSD = amountUSD
  exit.logIndex = call.transaction.index
  exit.liquidity = exitLiquidity

  updateGamutDayData(call)
  updatePoolDayData(call, false)
  updatePoolHourData(call, false)
  updateTokenDayData(token0 as Token, call)
  updateTokenDayData(token1 as Token, call)
  updateTokenHourData(token0 as Token, call)
  updateTokenHourData(token1 as Token, call)

  token0.save()
  token1.save()
  factory.save()
  exit.save()

  let weightBalanceData = WeightBalanceData.load(call.to.toHexString() + '#' + call.block.timestamp.toString())
  if (!weightBalanceData) {
    weightBalanceData = new WeightBalanceData(call.to.toHexString() + '#' + call.block.timestamp.toString())
    weightBalanceData.pool = call.to.toHexString()
    weightBalanceData.token0 = token0.id
    weightBalanceData.token1 = token1.id
    weightBalanceData.weight0 = ZERO_BD
    weightBalanceData.weight1 = ZERO_BD
    weightBalanceData.timestamp = ZERO_BI
  }
  weightBalanceData.weight0 = pool.weight0.div(pool.weight0.plus(pool.weight1))
  weightBalanceData.weight1 = pool.weight1.div(pool.weight0.plus(pool.weight1))
  weightBalanceData.timestamp = call.block.timestamp
  weightBalanceData.save()

  let poolTokensPrice = PoolTokensPrice.load(call.to.toHexString() + '#' + call.block.timestamp.toString())
  if (!poolTokensPrice) {
    poolTokensPrice = new PoolTokensPrice(call.to.toHexString() + '#' + call.block.timestamp.toString())
    poolTokensPrice.pool = call.to.toHexString()
    poolTokensPrice.token0 = token0.id
    poolTokensPrice.token1 = token1.id
    poolTokensPrice.token0Price = ZERO_BD
    poolTokensPrice.token1Price = ZERO_BD
    poolTokensPrice.timestamp = ZERO_BI
  }
  poolTokensPrice.token0Price = pool.token0Price
  poolTokensPrice.token1Price = pool.token1Price
  poolTokensPrice.timestamp = call.block.timestamp
  poolTokensPrice.save()
}

// export function handleJoinPool(call: JoinPoolCall): void {
//   let bundle = Bundle.load('1')
//   let poolAddress = call.from.toHexString()
//   let pool = Pool.load(poolAddress)
//   let factory = Factory.load(FACTORY_ADDRESS)

//   let token0 = Token.load(pool.token0)
//   let token1 = Token.load(pool.token1)
//   let amount0 = convertTokenToDecimal(call.inputValues[2].value.toBigIntArray()[0])
//   let amount1 = convertTokenToDecimal(call.inputValues[2].value.toBigIntArray()[1])

//   let amountUSD = amount0
//     .times(token0.derivedBTC.times(bundle.btcPriceUSD))
//     .plus(amount1.times(token1.derivedBTC.times(bundle.btcPriceUSD)))

//   // reset tvl aggregates until new amounts calculated
//   factory.totalValueLockedBTC = factory.totalValueLockedBTC.minus(pool.totalValueLockedBTC)

//   // update globals
//   factory.txCount = factory.txCount.plus(ONE_BI)

//   // update token0 data
//   token0.txCount = token0.txCount.plus(ONE_BI)
//   token0.totalValueLocked = token0.totalValueLocked.plus(amount0)
//   token0.totalValueLockedUSD = token0.totalValueLocked.times(token0.derivedBTC.times(bundle.btcPriceUSD))

//   // update token1 data
//   token1.txCount = token1.txCount.plus(ONE_BI)
//   token1.totalValueLocked = token1.totalValueLocked.plus(amount1)
//   token1.totalValueLockedUSD = token1.totalValueLocked.times(token1.derivedBTC.times(bundle.btcPriceUSD))

//   // pool data
//   pool.txCount = pool.txCount.plus(ONE_BI)

//   pool.totalValueLockedToken0 = pool.totalValueLockedToken0.plus(amount0)
//   pool.totalValueLockedToken1 = pool.totalValueLockedToken1.plus(amount1)
//   pool.totalValueLockedBTC = pool.totalValueLockedToken0
//     .times(token0.derivedBTC)
//     .plus(pool.totalValueLockedToken1.times(token1.derivedBTC))
//   pool.totalValueLockedUSD = pool.totalValueLockedBTC.times(bundle.btcPriceUSD)

//   // reset aggregates with new amounts
//   factory.totalValueLockedBTC = factory.totalValueLockedBTC.plus(pool.totalValueLockedBTC)
//   factory.totalValueLockedUSD = factory.totalValueLockedBTC.times(bundle.btcPriceUSD)

//   let transaction = loadTransaction(call)
//   let join = new Join(transaction.id.toString() + '#' + pool.txCount.toString())
//   join.transaction = transaction.id
//   join.timestamp = transaction.timestamp
//   join.pool = pool.id
//   join.token0 = pool.token0
//   join.token1 = pool.token1
//   join.sender = call.inputs.sender
//   join.origin = call.transaction.from
//   join.amount0 = amount0
//   join.amount1 = amount1
//   join.amountUSD = amountUSD
//   join.logIndex = call.transaction.index

//   updateGamutDayData(call)
//   updatePoolDayData(call)
//   updatePoolHourData(call)
//   updateTokenDayData(token0 as Token, call)
//   updateTokenDayData(token1 as Token, call)
//   updateTokenHourData(token0 as Token, call)
//   updateTokenHourData(token1 as Token, call)

//   token0.save()
//   token1.save()
//   pool.save()
//   factory.save()
//   join.save()
// }

// export function handleExitPool(call: ExitPoolCall): void {
//   let bundle = Bundle.load('1')
//   let poolAddress = call.from.toHexString()
//   let pool = Pool.load(poolAddress)
//   let factory = Factory.load(FACTORY_ADDRESS)
//   let poolContract = PoolABI.bind(Address.fromString(poolAddress));
//   let weights = poolContract.getWeights();

//   let token0 = Token.load(pool.token0)
//   let token1 = Token.load(pool.token1)
//   let amount0 = convertTokenToDecimal(call.inputValues[2].value.toBigIntArray()[0])
//   let amount1 = convertTokenToDecimal(call.inputValues[2].value.toBigIntArray()[1])

//   let amountUSD = amount0
//     .times(token0.derivedBTC.times(bundle.btcPriceUSD))
//     .plus(amount1.times(token1.derivedBTC.times(bundle.btcPriceUSD)))

//   // reset tvl aggregates until new amounts calculated
//   factory.totalValueLockedBTC = factory.totalValueLockedBTC.minus(pool.totalValueLockedBTC)

//   // update globals
//   factory.txCount = factory.txCount.plus(ONE_BI)

//   // update pool
//   pool.weight0 = weights[0].toBigDecimal()
//   pool.weight1 = weights[1].toBigDecimal()

//   // update token0 data
//   token0.txCount = token0.txCount.plus(ONE_BI)
//   token0.totalValueLocked = token0.totalValueLocked.minus(amount0)
//   token0.totalValueLockedUSD = token0.totalValueLocked.times(token0.derivedBTC.times(bundle.btcPriceUSD))

//   // update token1 data
//   token1.txCount = token1.txCount.plus(ONE_BI)
//   token1.totalValueLocked = token1.totalValueLocked.minus(amount1)
//   token1.totalValueLockedUSD = token1.totalValueLocked.times(token1.derivedBTC.times(bundle.btcPriceUSD))

//   // pool data
//   pool.txCount = pool.txCount.plus(ONE_BI)

//   pool.totalValueLockedToken0 = pool.totalValueLockedToken0.minus(amount0)
//   pool.totalValueLockedToken1 = pool.totalValueLockedToken1.minus(amount1)
//   pool.totalValueLockedBTC = pool.totalValueLockedToken0
//     .times(token0.derivedBTC)
//     .plus(pool.totalValueLockedToken1.times(token1.derivedBTC))
//   pool.totalValueLockedUSD = pool.totalValueLockedBTC.times(bundle.btcPriceUSD)

//   // reset aggregates with new amounts
//   factory.totalValueLockedBTC = factory.totalValueLockedBTC.plus(pool.totalValueLockedBTC)
//   factory.totalValueLockedUSD = factory.totalValueLockedBTC.times(bundle.btcPriceUSD)

//   // burn entity
//   let transaction = loadTransaction(call)
//   let exit = new Exit(transaction.id + '#' + pool.txCount.toString())
//   exit.transaction = transaction.id
//   exit.timestamp = transaction.timestamp
//   exit.pool = pool.id
//   exit.token0 = pool.token0
//   exit.token1 = pool.token1
//   exit.sender = call.inputs.sender
//   exit.origin = call.transaction.from
//   exit.amount0 = amount0
//   exit.amount1 = amount1
//   exit.amountUSD = amountUSD

//   updateGamutDayData(call)
//   updatePoolDayData(call)
//   updatePoolHourData(call)
//   updateTokenDayData(token0 as Token, call)
//   updateTokenDayData(token1 as Token, call)
//   updateTokenHourData(token0 as Token, call)
//   updateTokenHourData(token1 as Token, call)

//   token0.save()
//   token1.save()
//   pool.save()
//   factory.save()
//   exit.save()
// }
