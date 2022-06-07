/* eslint-disable prefer-const */
import { Pool as PoolABI } from "../../generated/templates/Pool/Pool"
import { ONE_BD, ZERO_BD, ZERO_BI } from './constants'
import { Bundle, Pool, Token } from '../../generated/schema'
import { Address, BigDecimal, BigInt } from '@graphprotocol/graph-ts'
import { exponentToBigDecimal, safeDiv } from '../utils/index'
import { log } from '@graphprotocol/graph-ts'

const TETH_ADDRESS = '0x0f3cd4d9cfc58aa42426fd7742837175ccea5918'
const USDC_ADDRESS = '0x2a12b95dba4383f2537901df1f113bbd566a48d1'
const DAI_ADDRESS = "0xec3be3f94b7e4bc635603537087c53355b180723"
const USDC_TETH_POOL = '0x1719c44ca5bed9590c7d21e5144121ebf762196e'

// token where amounts should contribute to tracked volume and liquidity
// usually tokens that many tokens are paired with s
export let WHITELIST_TOKENS: string[] = [
  TETH_ADDRESS, // TETH
  USDC_ADDRESS, // USDC
  DAI_ADDRESS, // DAI
  '0x1cbfd025eb289b9c806a034cbd48d89234971700', // BTC
]

// let Q192 = 2 ** 192
// export function sqrtPriceX96ToTokenPrices(sqrtPriceX96: BigInt, token0: Token, token1: Token): BigDecimal[] {
//   let num = sqrtPriceX96.times(sqrtPriceX96).toBigDecimal()
//   let denom = BigDecimal.fromString(Q192.toString())
//   let price1 = num
//     .div(denom)
//     .times(exponentToBigDecimal(token0.decimals))
//     .div(exponentToBigDecimal(token1.decimals))

//   let price0 = safeDiv(BigDecimal.fromString('1'), price1)
//   return [price0, price1]
// }

export function getTokenPrices(poolAddress: Address, token0: Token, token1: Token): BigDecimal[] {
  let poolContract = PoolABI.bind(poolAddress);

  let poolWeights = poolContract.getWeights()
  let weight0 = poolWeights[0].toBigDecimal()
  let weight1 = poolWeights[1].toBigDecimal()

  let poolBalances = poolContract.getPoolBalancesAndChangeBlock()
  let poolBalance0 = poolBalances.value0.toBigDecimal()
  let poolBalance1 = poolBalances.value1.toBigDecimal()

  // let whiteList0 = token0.whitelistPools
  // let whiteList1 = token1.whitelistPools

  // let bundle = Bundle.load('1')

  let price0 = (poolBalance1.div(weight1)).div(poolBalance0.div(weight0))
  let price1 = ONE_BD.div(price0)

  // for (let i = 0; i < whiteList0.length; ++i) {

  //   let unitPoolAddress = whiteList0[i]
  //   let unitPool = Pool.load(unitPoolAddress)

  //   if (unitPool.token0 == token0.id) {
  //     if (unitPool.token0)
  //   }
  //   if(unitPool.token0 == USDC_ADDRESS || unitPool.token0 == DAI_ADDRESS) {
  //     let price0 = unitPool.weight0.div(unitPool.weight1)
  //     let price1 = poolWeights[1].toBigDecimal().div(poolWeights[0].toBigDecimal()).times(price0)
  //     return [price0, price1]
  //   }
  //   if(unitPool.token0 == TETH_ADDRESS) {
  //     let price0 = unitPool.weight0.div(unitPool.weight1).times(bundle.ethPriceUSD)
  //     let price1 = poolWeights[1].toBigDecimal().div(poolWeights[0].toBigDecimal()).times(price0)
  //     return [price0, price1]
  //   }
  //   if(unitPool.token1 == USDC_ADDRESS || unitPool.token1 == DAI_ADDRESS) {
  //     let price0 = unitPool.weight1.div(unitPool.weight0)
  //     let price1 = poolWeights[1].toBigDecimal().div(poolWeights[0].toBigDecimal()).times(price0)
  //     return [price0, price1]
  //   }
  //   if(unitPool.token1 == TETH_ADDRESS) {
  //     let price0 = unitPool.weight1.div(unitPool.weight0).times(bundle.ethPriceUSD)
  //     let price1 = poolWeights[1].toBigDecimal().div(poolWeights[0].toBigDecimal()).times(price0)
  //     return [price0, price1]
  //   }
  // }

  return [price0, price1]
}

export function getTokenPrice(token: Token): BigDecimal {
  let bundle = Bundle.load('1')
  if (bundle === null) {
    log.error('bundle is not existed', [])
    return ZERO_BD
  }
  if (token.id == USDC_ADDRESS) {
    return ONE_BD
  }
  return token.derivedETH.times(bundle.ethPriceUSD)
}

export function getTokenRatio(poolAddress: Address, token0: Token, token1: Token): BigDecimal {
  let poolContract = PoolABI.bind(poolAddress);

  let poolWeights = poolContract.getWeights()
  let weight0 = poolWeights[0].toBigDecimal()
  let weight1 = poolWeights[1].toBigDecimal()

  let poolBalances = poolContract.getPoolBalancesAndChangeBlock()
  let poolBalance0 = poolBalances.value0.toBigDecimal()
  let poolBalance1 = poolBalances.value1.toBigDecimal()

  // let whiteList0 = token0.whitelistPools
  // let whiteList1 = token1.whitelistPools

  // let bundle = Bundle.load('1')

  return (poolBalance1.div(weight1)).div(poolBalance0.div(weight0))
}

export function getEthPriceInUSD(): BigDecimal {
  // fetch eth prices for each stablecoin
  let usdcPool = Pool.load(USDC_TETH_POOL) // dai is token0

  let poolContract = PoolABI.bind(Address.fromString(USDC_TETH_POOL))
  let poolBalances = poolContract.getPoolBalancesAndChangeBlock()
  let poolBalance0 = poolBalances.value0.toBigDecimal()
  let poolBalance1 = poolBalances.value1.toBigDecimal()
  if (usdcPool !== null && usdcPool.weight1.gt(BigDecimal.fromString('0'))) {
    return (poolBalance1.div(usdcPool.weight1)).div(poolBalance0.div(usdcPool.weight0))
  } else {
    return ZERO_BD
  }
}

/**
 * Search through graph to find derived Eth per token.
 * @todo update to be derived ETH (add stablecoin estimates)
 **/
export function findEthPerToken(token: Token): BigDecimal {
  if (token.id == TETH_ADDRESS) {
    return ONE_BD
  }
  let whiteList = token.whitelistPools
  // for now just take USD from pool with greatest TVL
  // need to update this to actually detect best rate based on liquidity distribution
  let largestLiquidityETH = ZERO_BD
  let priceSoFar = ZERO_BD
  for (let i = 0; i < whiteList.length; ++i) {
    let poolAddress = whiteList[i]
    let pool = Pool.load(poolAddress)
    if (pool === null) {
      log.error('pool is not existed', [])
      return ZERO_BD
    }
    if (pool.liquidity.gt(ZERO_BI)) {
      if (pool.token0 == token.id) {
        // whitelist token is token1
        let token1 = Token.load(pool.token1)
        if (token1 === null) {
          log.error('token is not existed', [])
          return ZERO_BD
        }
        let token1DerivedETH = token1.derivedETH
        if (token1DerivedETH == ZERO_BD && token1.id == TETH_ADDRESS) {
          token1DerivedETH = ONE_BD
        }
        // get the derived ETH in pool
        let ethLocked = pool.totalValueLockedToken1.times(token1DerivedETH)
        if (ethLocked.gt(largestLiquidityETH)) {
          largestLiquidityETH = ethLocked
          // token1 per our token * Eth per token1
          priceSoFar = token1DerivedETH.times(pool.ratio)
        }
      }
      if (pool.token1 == token.id) {
        let token0 = Token.load(pool.token0)
        if (token0 === null) {
          log.error('token is not existed', [])
          return ZERO_BD
        }
        let token0DerivedETH = token0.derivedETH
        if (token0DerivedETH == ZERO_BD && token0.id == TETH_ADDRESS) {
          token0DerivedETH = ONE_BD
        }
        // get the derived ETH in pool
        let ethLocked = pool.totalValueLockedToken0.times(token0DerivedETH)
        if (ethLocked.gt(largestLiquidityETH)) {
          largestLiquidityETH = ethLocked
          // token0 per our token * ETH per token0
          priceSoFar = token0DerivedETH.div(pool.ratio)
        }
      }
    }
  }
  return priceSoFar // nothing was found return 0
}

/**
 * Accepts tokens and amounts, return tracked amount based on token whitelist
 * If one token on whitelist, return amount in that token converted to USD * 2.
 * If both are, return sum of two amounts
 * If neither is, return 0
 */
export function getTrackedAmountUSD(
  tokenAmount0: BigDecimal,
  token0: Token,
  tokenAmount1: BigDecimal,
  token1: Token
): BigDecimal {
  let bundle = Bundle.load('1')
  if (bundle === null) {
    log.error('bundle is not existed', [])
    return ZERO_BD
  }
  let price0USD = token0.derivedETH.times(bundle.ethPriceUSD)
  let price1USD = token1.derivedETH.times(bundle.ethPriceUSD)

  // both are whitelist tokens, return sum of both amounts
  if (WHITELIST_TOKENS.includes(token0.id) && WHITELIST_TOKENS.includes(token1.id)) {
    return tokenAmount0.times(price0USD).plus(tokenAmount1.times(price1USD))
  }

  // take double value of the whitelisted token amount
  if (WHITELIST_TOKENS.includes(token0.id) && !WHITELIST_TOKENS.includes(token1.id)) {
    return tokenAmount0.times(price0USD).times(BigDecimal.fromString('2'))
  }

  // take double value of the whitelisted token amount
  if (!WHITELIST_TOKENS.includes(token0.id) && WHITELIST_TOKENS.includes(token1.id)) {
    return tokenAmount1.times(price1USD).times(BigDecimal.fromString('2'))
  }

  // neither token is on white list, tracked amount is 0
  return ZERO_BD
}
