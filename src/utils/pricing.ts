/* eslint-disable prefer-const */
import { Pool as PoolABI } from "../types/templates/Pool/Pool"
import { ONE_BD, ZERO_BD, ZERO_BI } from './constants'
import { Bundle, Pool, Token } from './../types/schema'
import { Address, BigDecimal, BigInt } from '@graphprotocol/graph-ts'
import { exponentToBigDecimal, safeDiv } from '../utils/index'

const TETH_ADDRESS = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
const USDC_ADDRESS = '0x2a12B95Dba4383f2537901Df1f113bbd566A48D1'
const DAI_ADDRESS = "0xEC3bE3f94B7E4bc635603537087c53355b180723"
const USDC_TETH_POOL = '0x1719C44ca5bed9590c7D21e5144121eBF762196e'

// token where amounts should contribute to tracked volume and liquidity
// usually tokens that many tokens are paired with s
export let WHITELIST_TOKENS: string[] = [
  TETH_ADDRESS, // TETH
  USDC_ADDRESS, // USDC
  DAI_ADDRESS, // DAI
  '0x1CbFD025Eb289b9c806A034cbD48d89234971700', // BTC
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

export function getTokenPrices(poolAddress: Address, token0: Token, token1: Token) {
  let poolContract = PoolABI.bind(poolAddress);
  let poolWeights = poolContract.getWeights();
  let whiteList0 = token0.whitelistPools
  let whiteList1 = token1.whitelistPools
  let bundle = Bundle.load('1')


  if(token0.id === USDC_ADDRESS || token0.id === DAI_ADDRESS) {
    let price0 = ONE_BD;
    let price1 = poolWeights[1].div(poolWeights[0]).toBigDecimal()
    return [price0, price1]
  }

  if(token1.id === USDC_ADDRESS || token1.id === DAI_ADDRESS) {
    let price1 = ONE_BD;
    let price0 = poolWeights[0].div(poolWeights[1]).toBigDecimal()
      return [price0, price1]
  }

  for (let i = 0; i < whiteList0.length; ++i) {
    let unitPoolAddress = whiteList0[i]
    let unitPool = Pool.load(unitPoolAddress)
    if(unitPool.token0 === USDC_ADDRESS || unitPool.token0 === DAI_ADDRESS) {
      let price0 = unitPool.weight0.div(unitPool.weight1)
      let price1 = poolWeights[1].toBigDecimal().div(poolWeights[0].toBigDecimal()).times(price0)
      return [price0, price1]
    }
    if(unitPool.token0 === TETH_ADDRESS) {
      let price0 = unitPool.weight0.div(unitPool.weight1).times(bundle.ethPriceUSD)
      let price1 = poolWeights[1].toBigDecimal().div(poolWeights[0].toBigDecimal()).times(price0)
      return [price0, price1]
    }
    if(unitPool.token1 === USDC_ADDRESS || unitPool.token1 === DAI_ADDRESS) {
      let price0 = unitPool.weight1.div(unitPool.weight0)
      let price1 = poolWeights[1].toBigDecimal().div(poolWeights[0].toBigDecimal()).times(price0)
      return [price0, price1]
    }
    if(unitPool.token1 === TETH_ADDRESS) {
      let price0 = unitPool.weight1.div(unitPool.weight0).times(bundle.ethPriceUSD)
      let price1 = poolWeights[1].toBigDecimal().div(poolWeights[0].toBigDecimal()).times(price0)
      return [price0, price1]
    }
  }

  for (let i = 0; i < whiteList1.length; ++i) {
    let unitPoolAddress = whiteList1[i]
    let unitPool = Pool.load(unitPoolAddress)
    if(unitPool.token0 === USDC_ADDRESS || unitPool.token0 === DAI_ADDRESS) {
      let price1 = unitPool.weight0.div(unitPool.weight1)
      let price0 = poolWeights[0].toBigDecimal().div(poolWeights[1].toBigDecimal()).times(price1)
      return [price0, price1]
    }
    if(unitPool.token0 === TETH_ADDRESS) {
      let price1 = unitPool.weight0.div(unitPool.weight1).times(bundle.ethPriceUSD)
      let price0 = poolWeights[0].toBigDecimal().div(poolWeights[1].toBigDecimal()).times(price1)
      return [price0, price1]
    }
    if(unitPool.token1 === USDC_ADDRESS || unitPool.token1 === DAI_ADDRESS) {
      let price1 = unitPool.weight1.div(unitPool.weight0)
      let price0 = poolWeights[0].toBigDecimal().div(poolWeights[1].toBigDecimal()).times(price1)
      return [price0, price1]
    }
    if(unitPool.token1 === TETH_ADDRESS) {
      let price1 = unitPool.weight1.div(unitPool.weight0).times(bundle.ethPriceUSD)
      let price0 = poolWeights[0].toBigDecimal().div(poolWeights[1].toBigDecimal()).times(price1)
      return [price0, price1]
    }
  }

}

export function getEthPriceInUSD(): BigDecimal {
  // fetch eth prices for each stablecoin
  let usdcPool = Pool.load(USDC_TETH_POOL) // dai is token0
  if (usdcPool !== null) {
    return usdcPool.token0Price
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
    if (pool.liquidity.gt(ZERO_BI)) {
      if (pool.token0 == token.id) {
        // whitelist token is token1
        let token1 = Token.load(pool.token1)
        // get the derived ETH in pool
        let ethLocked = pool.totalValueLockedToken1.times(token1.derivedETH)
        if (ethLocked.gt(largestLiquidityETH)) {
          largestLiquidityETH = ethLocked
          // token1 per our token * Eth per token1
          priceSoFar = pool.token1Price.times(token1.derivedETH as BigDecimal)
        }
      }
      if (pool.token1 == token.id) {
        let token0 = Token.load(pool.token0)
        // get the derived ETH in pool
        let ethLocked = pool.totalValueLockedToken0.times(token0.derivedETH)
        if (ethLocked.gt(largestLiquidityETH)) {
          largestLiquidityETH = ethLocked
          // token0 per our token * ETH per token0
          priceSoFar = pool.token0Price.times(token0.derivedETH as BigDecimal)
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
