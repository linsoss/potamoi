import sbt.ModuleID

object BuildHelper {

  implicit class ModuleIdExtension(mid: ModuleID) {
    def ~(amid: ModuleID): Seq[ModuleID]         = Seq(mid, amid)
    def ~(amidSeq: Seq[ModuleID]): Seq[ModuleID] = mid +: amidSeq
  }

  implicit class ModuleIdSeqExtension(midSeq: Seq[ModuleID]) {
    def ~(amid: ModuleID): Seq[ModuleID]         = midSeq :+ amid
    def ~(amidSeq: Seq[ModuleID]): Seq[ModuleID] = midSeq ++ amidSeq
  }

}
