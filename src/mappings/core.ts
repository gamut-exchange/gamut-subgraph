/* eslint-disable prefer-const */
import { Pool } from '../../generated/schema'
import {
  SwapFeePercentageChanged as SwapFeeEvent,
} from '../../generated/templates/Pool/Pool'
import { log } from '@graphprotocol/graph-ts'

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