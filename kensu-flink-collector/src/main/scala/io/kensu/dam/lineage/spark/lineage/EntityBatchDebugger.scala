package io.kensu.dam.lineage.spark.lineage

import io.kensu.dam.model.BatchEntityReportProxy

object BatchDebugger {

  implicit class BatchDebuggerImplicits(bProxy: BatchEntityReportProxy) {
    private def count[T](items: Option[Seq[T]]) = items.map(_.size).getOrElse(0)

    def toShortOverview: String                 = {
      val b            = bProxy.batch
      val entityCounts = Map(
        "projects" -> count(b.projects),
        "processes" -> count(b.processes),
        "processRuns" -> count(b.processRuns),
        "processRunStats" -> count(b.processRunStats),
        "processLineages" -> count(b.processLineages),
        "lineageRuns" -> count(b.lineageRuns),
        "schemas" -> count(b.schemas),
        "schemaFieldTags" -> count(b.schemaFieldTags),
        "physicalLocations" -> count(b.physicalLocations),
        "dataSources" -> count(b.dataSources),
        "codeVersions" -> count(b.codeVersions),
        "codeBases" -> count(b.codeBases),
        "users" -> count(b.users),
        "dataStats" -> count(b.dataStats),
        "models" -> count(b.models),
        "modelTrainings" -> count(b.modelTrainings),
        "modelMetrics" -> count(b.modelMetrics)
      ).filter { case (_, count) => count > 0 }
      if (entityCounts.isEmpty) "Empty batch"
      else entityCounts.map { case (k, v) => s"$k: $v" }.mkString("Batch(", ",", ")")
    }
  }
}
