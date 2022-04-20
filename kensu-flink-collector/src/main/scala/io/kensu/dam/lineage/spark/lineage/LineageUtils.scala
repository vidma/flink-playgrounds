package io.kensu.dam.lineage.spark.lineage

import io.kensu.dam.lineage.spark.lineage.LineageUtils.{maybeDepsOption, ProcessLineageHelpers}
import io.kensu.dam.model.ModelHelpers.SchemaHelper
import io.kensu.dam.model.{ProcessLineage, ProcessLineagePK, ProcessRef, Schema, SchemaLineageDependencyDef, SchemaRef}

trait LineageFallbackStrategy {

  def apply(
    existingDataFlow: Seq[SchemaLineageDependencyDef],
    inputSchemas: Seq[Schema],
    outputSchema: Schema
  ): Seq[SchemaLineageDependencyDef]
}

object LineageFallbackStrategy {

  def withName(name: String): Option[ColumnNameMatcherStrategy] =
    name match {
      case "CaseInsensitiveColumnNameMatcherStrat"     => Some(CaseInsensitiveColumnNameMatcherStrat)
      case "CaseSensitiveColumnNameMatcherStrat"       => Some(CaseSensitiveColumnNameMatcherStrat)
      case "AllToAllLineageStrat"                      => Some(AllToAllLineageStrat)
      case "OutFieldEndsWithInFieldNameLineageStrat"   => Some(OutFieldEndsWithInFieldNameLineageStrat)
      case "OutFieldStartsWithInFieldNameLineageStrat" => Some(OutFieldStartsWithInFieldNameLineageStrat)
      case _                                           => None
    }
}

abstract class ColumnNameMatcherStrategy extends LineageFallbackStrategy {
  def isFieldNameMatch(inputField: String, outputField: String): Boolean

  def apply(
    existingDataFlow: Seq[SchemaLineageDependencyDef],
    inputSchemas: Seq[Schema],
    outputSchema: Schema
  ): Seq[SchemaLineageDependencyDef] = {
    val existingDataFlowBySchemas: Map[(SchemaRef, SchemaRef), Seq[SchemaLineageDependencyDef]] =
      existingDataFlow.groupBy(x => x.fromSchemaRef.compact -> x.toSchemaRef.compact)
    for {
      inSchema <- inputSchemas
    } yield {
      val inSchemaRef                                         = inSchema.toRef
      val outputSchemaRef                                     = outputSchema.toRef
      val existingColumnDeps: Seq[SchemaLineageDependencyDef] =
        existingDataFlowBySchemas.getOrElse(inSchemaRef.compact -> outputSchemaRef.compact, default = Seq.empty)
      SchemaLineageDependencyDef(
        fromSchemaRef = inSchemaRef,
        toSchemaRef               = outputSchemaRef,
        columnDataDependencies    = maybeDepsOption(
          outputSchema.pk.fields
            .map(_.name)
            .map { outField =>
//              val outColHasExistingDeps = existingColumnDeps.map(_.columnDataDependencies.getOrElse(Map.empty))
//                .exists(d => d.getOrElse(outField, Seq.empty).nonEmpty)
//              if (outColHasExistingDeps)
              outField -> inSchema.pk.fields
                .map(_.name)
                .filter(inField => isFieldNameMatch(inField, outField))
            }
            .toMap
        ),
        columnControlDependencies = existingColumnDeps.map(_.columnControlDependencies).headOption.flatten
      )
    }

  }
}

object CaseInsensitiveColumnNameMatcherStrat extends ColumnNameMatcherStrategy {

  override def isFieldNameMatch(inputField: String, outputField: String) =
    inputField.toLowerCase == outputField.toLowerCase
}

object CaseSensitiveColumnNameMatcherStrat extends ColumnNameMatcherStrategy {
  override def isFieldNameMatch(inputField: String, outputField: String) = inputField == outputField
}

object AllToAllLineageStrat extends ColumnNameMatcherStrategy {
  override def isFieldNameMatch(inputField: String, outputField: String) = true
}

object OutFieldEndsWithInFieldNameLineageStrat extends ColumnNameMatcherStrategy {
  override def isFieldNameMatch(inputField: String, outputField: String) = outputField.endsWith(inputField)
}

object OutFieldStartsWithInFieldNameLineageStrat extends ColumnNameMatcherStrategy {
  override def isFieldNameMatch(inputField: String, outputField: String) = outputField.startsWith(inputField)
}
// FIXME: another strategy could also check if datatype is similar?

object Lineage {

  def empty(p: ProcessRef) = ProcessLineage(
    name           = "",
    operationLogic = None,
    pk             = ProcessLineagePK(processRef = p, dataFlow = Seq.empty)
  )

  def approx(p: ProcessRef, inputSchemas: Seq[Schema], outputSchema: Schema): ProcessLineage =
    Lineage.empty(p)
      // FIXME : here I'd like a different logic, use all-to-all only when certain field has no matches
      .withColumnLineageFallbackIfEmpty(
        maybeStrategy = Some(CaseInsensitiveColumnNameMatcherStrat),
        inputSchemas  = inputSchemas,
        outputSchema  = outputSchema
      ).withColumnLineageFallbackIfEmpty(
        maybeStrategy = Some(AllToAllLineageStrat),
        inputSchemas  = inputSchemas,
        outputSchema  = outputSchema
      )
}

object LineageUtils {

  def maybeDepsOption(m: Map[String, Seq[String]]): Option[Map[String, Seq[String]]] =
    if (m.nonEmpty) Some(m) else None

  implicit class ProcessLineageHelpers(l: ProcessLineage) {

    def isColumnLineageEmpty: Boolean =
      l.pk.dataFlow.forall(_.columnControlDependencies.getOrElse(Map.empty).isEmpty) &&
      l.pk.dataFlow.forall(_.columnDataDependencies.getOrElse(Map.empty).isEmpty)

    def withColumnLineageFallbackIfEmpty(
      maybeStrategy: Option[LineageFallbackStrategy],
      inputSchemas: Seq[Schema],
      outputSchema: Schema
    ): ProcessLineage =
      (l.isColumnLineageEmpty, maybeStrategy) match {
        case (true, Some(strat)) =>
          // FIXME: rename lineage
          l.copy(pk = l.pk.copy(dataFlow = strat.apply(l.pk.dataFlow, inputSchemas, outputSchema)))
        case _                   => l
      }
  }

}
