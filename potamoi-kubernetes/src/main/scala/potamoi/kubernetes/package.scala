package potamoi

import com.coralogix.zio.k8s.client.model.K8sNamespace
import zio.prelude.data.Optional
import zio.prelude.data.Optional.{Absent, Present}

package object kubernetes:

  /**
   * Convert [[String]] to [[K8sNamespace]].
   */
  given Conversion[String, K8sNamespace] = namespace => K8sNamespace(namespace)

  /**
   * Convert [[Option]] to [[Optional]].
   */
  extension [A](option: Option[A])
    def toOptional: Optional[A] = option match
      case None    => Absent
      case Some(v) => Present(v)
