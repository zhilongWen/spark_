package com.at.spark

import java.sql.Timestamp

/**
 * @author zero
 * @create 2021-03-23 17:48
 */
case class AdsInfo(ts: Long,
                   timestamp: Timestamp,
                   dayString: String,
                   hmString: String,
                   area: String,
                   city: String,
                   userId: String,
                   adsId: String)
