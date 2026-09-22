"""
Framework minimalista de Data Quality em PySpark puro.

A ideia e a mesma do Great Expectations / PyDeequ: declarar *expectativas*
sobre o dado e medir quantas linhas as violam. A diferenca e que aqui tudo e
expresso como `Column` do Spark, o que traz duas vantagens importantes:

* funciona em **streaming** (Great Expectations e PyDeequ trabalham em modo
  batch e exigem acao de coleta, o que quebra uma query de micro-batch);
* nao ha `collect()` nem UDF Python - tudo e avaliado de forma distribuida.

Uso tipico::

    expectations = [
        Expectation("minuto_valido", F.col("minute").between(0, 90)),
        Expectation("audiencia_nao_negativa", F.col("audience_count") >= 0),
    ]
    annotated = annotate_violations(df, expectations)
    valid_df, quarantine_df = split_by_quality(annotated)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

LOGGER = logging.getLogger(__name__)

#: Nome da coluna tecnica que carrega a lista de regras violadas pela linha.
QUALITY_ERRORS_COLUMN = "quality_errors"

#: Nome da coluna tecnica booleana que indica se a linha passou em tudo.
IS_VALID_COLUMN = "is_valid"


class DataQualityError(RuntimeError):
    """Erro levantado quando uma expectativa critica e violada."""


@dataclass(frozen=True)
class Expectation:
    """Uma regra de qualidade declarativa.

    Attributes:
        name: identificador curto da regra (aparece na coluna de erros).
        condition: expressao Spark que deve ser verdadeira para uma linha valida.
        description: texto livre para documentacao / relatorios.
        critical: se True, `assert_expectations` falha ao encontrar violacoes.
    """

    name: str
    condition: Column
    description: str = ""
    critical: bool = False

    def violation_flag(self) -> Column:
        """Retorna o nome da regra quando ela e violada e NULL caso contrario.

        Importante: usamos `eqNullSafe(True)` para tratar NULL como violacao.
        Em SQL, `NULL >= 0` resulta em NULL (nao em False), e um registro com
        campo nulo passaria despercebido por um simples filtro.
        """
        return F.when(~self.condition.eqNullSafe(F.lit(True)), F.lit(self.name))


def annotate_violations(df: DataFrame, expectations: list[Expectation]) -> DataFrame:
    """Anexa as colunas tecnicas de qualidade ao DataFrame.

    Adiciona:
        * ``quality_errors``: array com os nomes das regras violadas;
        * ``is_valid``: booleano indicando ausencia de violacoes.

    A funcao e pura (DataFrame -> DataFrame) e serve tanto para batch quanto
    para streaming, o que permite testa-la com DataFrames estaticos.
    """
    if not expectations:
        return df.withColumn(QUALITY_ERRORS_COLUMN, F.array().cast("array<string>")).withColumn(
            IS_VALID_COLUMN, F.lit(True)
        )

    # `array()` + `array_compact()` monta a lista de violacoes sem UDF.
    errors = F.array_compact(F.array(*[exp.violation_flag() for exp in expectations]))
    return df.withColumn(QUALITY_ERRORS_COLUMN, errors).withColumn(
        IS_VALID_COLUMN, F.size(F.col(QUALITY_ERRORS_COLUMN)) == 0
    )


def split_by_quality(df: DataFrame, drop_technical_columns: bool = True) -> tuple[DataFrame, DataFrame]:
    """Separa o DataFrame anotado em (validos, quarentena).

    Args:
        df: DataFrame ja processado por :func:`annotate_violations`.
        drop_technical_columns: remove as colunas tecnicas do lado valido
            (a quarentena sempre as mantem, pois sao o motivo da rejeicao).

    Returns:
        Tupla ``(valid_df, quarantine_df)``.
    """
    if IS_VALID_COLUMN not in df.columns:
        raise ValueError(
            f"DataFrame sem a coluna `{IS_VALID_COLUMN}`. "
            "Chame `annotate_violations` antes de `split_by_quality`."
        )

    valid_df = df.filter(F.col(IS_VALID_COLUMN))
    quarantine_df = df.filter(~F.col(IS_VALID_COLUMN))

    if drop_technical_columns:
        valid_df = valid_df.drop(QUALITY_ERRORS_COLUMN, IS_VALID_COLUMN)

    return valid_df, quarantine_df


def quality_summary(df: DataFrame, expectations: list[Expectation]) -> DataFrame:
    """Calcula, em uma unica passada, quantas linhas violam cada regra.

    Retorna um DataFrame de uma linha com as colunas ``total_rows`` e
    ``violations__<nome_da_regra>``. Feito com um unico `agg` para evitar
    varios scans da tabela.
    """
    aggregations = [F.count(F.lit(1)).alias("total_rows")]
    aggregations += [
        F.sum(F.when(~exp.condition.eqNullSafe(F.lit(True)), F.lit(1)).otherwise(F.lit(0))).alias(
            f"violations__{exp.name}"
        )
        for exp in expectations
    ]
    return df.agg(*aggregations)


def assert_expectations(df: DataFrame, expectations: list[Expectation]) -> dict[str, int]:
    """Valida um DataFrame **batch** e falha se alguma regra critica for violada.

    Use este helper em jobs batch (por exemplo, um job de validacao pos-carga
    no Databricks Workflows). Em streaming, prefira `annotate_violations` +
    `split_by_quality`, que desviam o dado ruim para a quarentena em vez de
    derrubar a query.

    Returns:
        Dicionario ``{nome_da_regra: numero_de_violacoes}``.

    Raises:
        DataQualityError: se alguma expectativa marcada como `critical` falhar.
    """
    summary_row = quality_summary(df, expectations).first()
    if summary_row is None:  # pragma: no cover - agg sempre retorna 1 linha
        return {}

    results = {exp.name: int(summary_row[f"violations__{exp.name}"] or 0) for exp in expectations}
    total = int(summary_row["total_rows"] or 0)

    for name, violations in results.items():
        level = logging.ERROR if violations else logging.INFO
        LOGGER.log(level, "[DQ] %s: %s violacoes em %s linhas.", name, violations, total)

    critical_failures = {
        exp.name: results[exp.name] for exp in expectations if exp.critical and results[exp.name] > 0
    }
    if critical_failures:
        raise DataQualityError(f"Expectativas criticas violadas: {critical_failures}")

    return results
