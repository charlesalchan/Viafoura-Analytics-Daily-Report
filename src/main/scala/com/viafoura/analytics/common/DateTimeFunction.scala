package com.viafoura.analytics.common

import java.time.{Instant, ZoneOffset, LocalDateTime}
import java.util.{TimeZone, Calendar, UUID}
/**
 * Created by charles on 27/4/2016.
 */
object DateTimeFunction {
  def id2uuid(id: BigInt) : UUID = UUID.fromString(f"00000000-0000-4000-8000-${id.longValue()}%012x")

  def getLocalTime(y: Int, m: Int, d: Int): Instant = {
    val c = Calendar.getInstance()
    //c.setTimeZone(TimeZone.getTimeZone("UTC"));
    c.set(y, m -1, d , 0 , 0, 0);
    c.set(Calendar.MILLISECOND, 0 );

    return c.toInstant;
  }
}
