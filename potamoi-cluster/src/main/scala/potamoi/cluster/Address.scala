package potamoi.cluster

import zio.json.JsonCodec
import akka.actor

/**
 * Mirror of [[akka.actor.Address]]
 */
case class Address(protocol: String, system: String, host: String, port: Int) derives JsonCodec:

  def toAkka: actor.Address = actor.Address(
    protocol = protocol,
    system = system,
    host = host,
    port = port
  )

  def hostPortString: String = s"$host:$port"
  def fullString: String     = s"$protocol://$system@$host:$port"

object Address:

  def apply(address: actor.Address): Address = Address(
    protocol = address.protocol,
    system = address.system,
    host = address.host.getOrElse(""),
    port = address.port.getOrElse(0)
  )
