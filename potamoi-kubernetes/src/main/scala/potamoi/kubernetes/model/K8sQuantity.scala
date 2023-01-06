package potamoi.kubernetes.model

import potamoi.codecs
import potamoi.kubernetes.model.QuantityUnits.given_JsonCodec_QuantityUnit
import zio.json.{JsonCodec, JsonDecoder, JsonEncoder}

import scala.math.pow

/**
 * Kubernetes resource quantity.
 * see: https://kubernetes.io/docs/reference/kubernetes-api/common-definitions/quantity
 */
case class K8sQuantity(value: Double, unit: QuantityUnit) derives JsonCodec:
  import QuantityUnit.*

  def to(targetUnit: QuantityUnit): Double =
    if (unit == targetUnit) value
    else if (unit.ordinal <= E.ordinal && targetUnit.ordinal <= E.ordinal) value * pow(1000, unit.ordinal - targetUnit.ordinal)
    else if (unit.ordinal >= Ki.ordinal && targetUnit.ordinal >= Ki.ordinal) value * pow(1024, unit.ordinal - targetUnit.ordinal)
    else if (unit.ordinal <= E.ordinal) value * 1.024 * pow(1000, (targetUnit.ordinal - Ki.ordinal) - (unit.ordinal - k.ordinal))
    else value / 1.024 * pow(1000, (unit.ordinal - Ki.ordinal) - (targetUnit.ordinal - k.ordinal))

  def show: String = value + unit.toString

object K8sQuantity:
  def apply(quantity: String): K8sQuantity =
    val unit  = QuantityUnit.values.find(unit => quantity.endsWith(unit.toString)).getOrElse(QuantityUnit.k)
    val value = quantity.split(unit.toString)(0).trim.toDouble
    K8sQuantity(value, unit)

enum QuantityUnit:
  case n, u, m, k, M, G, T, P, E, Ki, Mi, Gi, Ti, Pi, Ei

object QuantityUnits:
  given JsonCodec[QuantityUnit] = codecs.simpleEnumJsonCodec(QuantityUnit.values)
