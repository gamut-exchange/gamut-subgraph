/* eslint-disable prefer-const */
import { Pool as PoolABI } from "../../generated/templates/Pool/Pool"
import { ONE_BD, ZERO_BD, ZERO_BI } from './constants'
import { Bundle, Pool, Token } from '../../generated/schema'
import { Address, BigDecimal, BigInt, log } from '@graphprotocol/graph-ts'
import { exponentToBigDecimal, safeDiv, tokenAmountToDecimal } from '../utils/index'

const KAVA_ADDRESS = '0xc86c7c0efbd6a49b35e8714c5f59d99de09a225b'
const DAI_ADDRESS = '0x765277eebeca2e31912c9946eae1021199b39c61'
const GBUSD_ADDRESS = "0x332730a4f6e03d9c55829435f10360e13cfa41ff"
const GBUSD_KAVA_POOL = '0x6be57618c8832ad25cceadf2745d5c92de7ab7b2'
// const WBTC_ADDRESS = '0x818ec0a7fe18ff94269904fced6ae3dae6d6dc0b'

// token where amounts should contribute to tracked volume and liquidity
// usually tokens that many tokens are paired with s
export let WHITELIST_TOKENS: string[] = [
  KAVA_ADDRESS,
  GBUSD_ADDRESS,
  DAI_ADDRESS,
  // WBTC_ADDRESS
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

export function getTokenPrices(poolAddress: Address, token0: Token, token1: Token):BigDecimal[] {
  let poolContract = PoolABI.bind(poolAddress);
  let poolWeights = poolContract.getWeights();
  let poolBalances = poolContract.getPoolBalancesAndChangeBlock()
  let whiteList0 = token0.whitelistPools
  let whiteList1 = token1.whitelistPools
  let bundle = Bundle.load('1')
  if (!bundle) {
    log.error(`getTokenPrices: bundle isn't defind`, [])
    return [ZERO_BD, ZERO_BD]
  }

  if(token0.id == GBUSD_ADDRESS) {
    let price0 = ONE_BD;
    let price1 = tokenAmountToDecimal(poolBalances.getBalance0(), token0.decimals).div(poolWeights[0].toBigDecimal()).div(tokenAmountToDecimal(poolBalances.getBalance1(), token1.decimals).div(poolWeights[1].toBigDecimal()))
    return [price0, price1]
  }

  if(token1.id == GBUSD_ADDRESS) {
    let price1 = ONE_BD;
    let price0 = tokenAmountToDecimal(poolBalances.getBalance1(), token1.decimals).div(poolWeights[1].toBigDecimal()).div(tokenAmountToDecimal(poolBalances.getBalance0(), token0.decimals).div(poolWeights[0].toBigDecimal()))
      return [price0, price1]
  }

  for (let i = 0; i < whiteList0.length; ++i) {
    let unitPoolAddress = whiteList0[i]
    let unitPool = Pool.load(unitPoolAddress)
    if (unitPool) {
      if(unitPool.token0 == GBUSD_ADDRESS) {
        let price0 = unitPool.token1Price
        let price1 = tokenAmountToDecimal(poolBalances.getBalance0(), token0.decimals).div(poolWeights[0].toBigDecimal()).div(tokenAmountToDecimal(poolBalances.getBalance1(), token1.decimals).div(poolWeights[1].toBigDecimal())).times(price0)
        return [price0, price1]
      }
      if(unitPool.token0 == KAVA_ADDRESS) {
        let price0 = unitPool.token1Price
        let price1 = tokenAmountToDecimal(poolBalances.getBalance0(), token0.decimals).div(poolWeights[0].toBigDecimal()).div(tokenAmountToDecimal(poolBalances.getBalance1(), token1.decimals).div(poolWeights[1].toBigDecimal()))
        return [price0, price1]
      }
      if(unitPool.token1 == GBUSD_ADDRESS) {
        let price0 = unitPool.token0Price
        let price1 = tokenAmountToDecimal(poolBalances.getBalance0(), token0.decimals).div(poolWeights[0].toBigDecimal()).div(tokenAmountToDecimal(poolBalances.getBalance1(), token1.decimals).div(poolWeights[1].toBigDecimal()))
        return [price0, price1]
      }
      if(unitPool.token1 == KAVA_ADDRESS) {
        let price0 = unitPool.token0Price
        let price1 = tokenAmountToDecimal(poolBalances.getBalance0(), token0.decimals).div(poolWeights[0].toBigDecimal()).div(tokenAmountToDecimal(poolBalances.getBalance1(), token1.decimals).div(poolWeights[1].toBigDecimal()))
        return [price0, price1]
      }
    }
  }

  for (let i = 0; i < whiteList1.length; ++i) {
    let unitPoolAddress = whiteList1[i]
    let unitPool = Pool.load(unitPoolAddress)
    if (unitPool) {
      if(unitPool.token0 == GBUSD_ADDRESS) {
        let price1 = unitPool.token1Price
        let price0 = tokenAmountToDecimal(poolBalances.getBalance1(), token1.decimals).div(poolWeights[1].toBigDecimal()).div(tokenAmountToDecimal(poolBalances.getBalance0(), token0.decimals).div(poolWeights[0].toBigDecimal()))
        return [price0, price1]
      }
      if(unitPool.token0 == KAVA_ADDRESS) {
        let price1 = unitPool.token1Price
        let price0 = poolBalances.getBalance1().times(poolWeights[0]).div(poolBalances.getBalance0().times(poolWeights[1])).toBigDecimal().times(price1)
        return [price0, price1]
      }
      if(unitPool.token1 == GBUSD_ADDRESS) {
        let price1 = unitPool.token0Price
        let price0 = tokenAmountToDecimal(poolBalances.getBalance1(), token1.decimals).div(poolWeights[1].toBigDecimal()).div(tokenAmountToDecimal(poolBalances.getBalance0(), token0.decimals).div(poolWeights[0].toBigDecimal()))
        return [price0, price1]
      }
      if(unitPool.token1 == KAVA_ADDRESS) {
        let price1 = unitPool.token0Price
        let price0 = tokenAmountToDecimal(poolBalances.getBalance1(), token1.decimals).div(poolWeights[1].toBigDecimal()).div(tokenAmountToDecimal(poolBalances.getBalance0(), token0.decimals).div(poolWeights[0].toBigDecimal()))
        return [price0, price1]
      }
    }
  }

  return [ZERO_BD, ZERO_BD]

}

export function getBtcPriceInUSD(): BigDecimal {
  // fetch eth prices for each stablecoin
  let daiPool = Pool.load(GBUSD_KAVA_POOL) // dai is token0
  if (daiPool !== null) {
    return daiPool.token1Price
  } else {
    return ZERO_BD
  }
}

/**
 * Search through graph to find derived Eth per token.
 * @todo update to be derived ETH (add stablecoin estimates)
 **/
export function findBtcPerToken(token: Token): BigDecimal {
  if (token.id == KAVA_ADDRESS) {
    return ONE_BD
  }
  let whiteList = token.whitelistPools
  // for now just take USD from pool with greatest TVL
  // need to update this to actually detect best rate based on liquidity distribution
  let largestLiquidityBTC = ZERO_BD
  let priceSoFar = ZERO_BD
  for (let i = 0; i < whiteList.length; ++i) {
    let poolAddress = whiteList[i]
    let pool = Pool.load(poolAddress)
    let poolContract = PoolABI.bind(Address.fromString(poolAddress))
    let balances = poolContract.getPoolBalancesAndChangeBlock()
    let weights = poolContract.getWeights()
    if (pool && pool.liquidity.gt(ZERO_BD)) {
      if (pool.token0 == token.id) {
        // whitelist token is token1
        let token1 = Token.load(pool.token1)
        let token0 = Token.load(pool.token0)
        if (token1 && token0) {
          // get the derived ETH in pool
          let btcLocked = balances.getBalance1().toBigDecimal().times(token1.derivedBTC)
          if (btcLocked.gt(largestLiquidityBTC)) {
            largestLiquidityBTC = btcLocked
            // token1 per our token * Eth per token1
            priceSoFar = balances.getBalance1().toBigDecimal().div(exponentToBigDecimal(token1.decimals)).div(weights[1].toBigDecimal()).div(balances.getBalance0().toBigDecimal().div(exponentToBigDecimal(token0.decimals)).div(weights[0].toBigDecimal())).times(token1.derivedBTC)
          }
        }
      }
      if (pool.token1 == token.id) {
        let token0 = Token.load(pool.token0)
        let token1 = Token.load(pool.token1)
        if (token0 && token1) {
          // get the derived ETH in pool
          let btcLocked = balances.getBalance0().toBigDecimal().times(token0.derivedBTC)
          if (btcLocked.gt(largestLiquidityBTC)) {
            largestLiquidityBTC = btcLocked
            // token0 per our token * ETH per token0
            priceSoFar = balances.getBalance0().toBigDecimal().div(exponentToBigDecimal(token0.decimals)).div(weights[0].toBigDecimal()).div(balances.getBalance1().toBigDecimal().div(exponentToBigDecimal(token1.decimals)).div(weights[1].toBigDecimal())).times(token0.derivedBTC)
          }
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
  let price0USD = !bundle ? ZERO_BD : token0.derivedBTC.times(bundle.KAVAPriceUSD)
  let price1USD = !bundle ? ZERO_BD : token1.derivedBTC.times(bundle.KAVAPriceUSD)

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
