package potamoi.kubernetes

import potamoi.kubernetes.model.K8sQuantity
import potamoi.kubernetes.model.QuantityUnit.*

class K8sQuantitySpec extends munit.FunSuite:

  test("Convert string to QuantityUnit") {
    assert(K8sQuantity("100m") == K8sQuantity(100, m))
    assert(K8sQuantity("233Ki") == K8sQuantity(233, Ki))
    assert(K8sQuantity("233Gi") == K8sQuantity(233, Gi))
    assert(K8sQuantity("233") == K8sQuantity(233, k))
  }

  test("Convert value by unit") {
    assert(K8sQuantity(1000, m).to(u) == 1000 * 1000)
    assert(K8sQuantity(1000, u).to(m) == 1)
    assert(K8sQuantity(1, Gi).to(Ki) == 1024 * 1024)
    assert(K8sQuantity(1024, Ki).to(Mi) == 1)
    assert(K8sQuantity(1024 * 1024, Ki).to(Gi) == 1)
    assert(K8sQuantity(1000, k).to(k) == 1000)
    assert(K8sQuantity(1000, Ki).to(Ki) == 1000)

    assert(K8sQuantity(1.024, Ki).to(k) == 1)
    assert(K8sQuantity(1.024, Gi).to(G) == 1)
    assert(K8sQuantity(1.024, Mi).to(M) == 1)
    assert(K8sQuantity(1.024, Ki).to(m) == 1000)
    assert(K8sQuantity(1.024, Gi).to(k) == 1000 * 1000)

    assert(K8sQuantity(1, k).to(Ki) == 1.024)
    assert(K8sQuantity(1, G).to(Gi) == 1.024)
    assert(K8sQuantity(1, M).to(Mi) == 1.024)
    assert(K8sQuantity(1.5, k).to(Mi) == 1536)
  }
