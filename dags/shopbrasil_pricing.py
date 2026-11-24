from __future__ import annotations

from datetime import timedelta
from typing import Any

import pendulum
import requests
from airflow.decorators import dag, task, task_group
from airflow.providers.postgres.hooks.postgres import PostgresHook

FAKESTORE_PRODUCTS_URL = "https://fakestoreapi.com/products"
POSTGRES_CONN_ID = "postgres_lab"
SAO_PAULO_TZ = pendulum.timezone("America/Sao_Paulo")


def on_success_callback(context: dict[str, Any]) -> None:
    task_id = context["task"].task_id
    print(f"✓ Task {task_id} completou com sucesso!")


def on_failure_callback(context: dict[str, Any]) -> None:
    task_id = context["task"].task_id
    exception = context.get("exception")
    print(f"✗ Task {task_id} falhou: {exception}")


def on_retry_callback(context: dict[str, Any]) -> None:
    task_id = context["task"].task_id
    print(f"↻ Task {task_id} vai fazer retry")


@dag(
    dag_id="shopbrasil_pricing_01",
    description="Pipeline diário de preços por categoria da ShopBrasil",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 1, 1, 6, 0, tz=SAO_PAULO_TZ),
    catchup=False,
    max_active_runs=1,
    tags=["shopbrasil", "pricing", "airflow"],
)
def pipeline():
    @task_group(group_id="tg_ingestao")
    def ingestao_group():
        @task(
            task_id="buscar_produtos",
            on_success_callback=on_success_callback,
            on_failure_callback=on_failure_callback,
            on_retry_callback=on_retry_callback,
            retries=4,
            retry_delay=timedelta(seconds=15),
            retry_exponential_backoff=True,
            max_retry_delay=timedelta(minutes=5),
        )
        def buscar_produtos() -> list[dict[str, Any]]:
            try:
                response = requests.get(FAKESTORE_PRODUCTS_URL, timeout=15)
                response.raise_for_status()
                return response.json()
            except requests.RequestException as exc:
                raise RuntimeError(f"Erro ao buscar produtos na API: {exc}") from exc

        @task(task_id="agrupar_por_categoria")
        def agrupar_por_categoria(produtos: list[dict[str, Any]]) -> list[dict[str, Any]]:
            grupos: dict[str, list[float]] = {}
            for produto in produtos:
                categoria = produto["category"].strip().lower()
                grupos.setdefault(categoria, []).append(float(produto["price"]))

            return [
                {"categoria": categoria, "precos": precos}
                for categoria, precos in sorted(grupos.items())
            ]

        return agrupar_por_categoria(buscar_produtos())

    @task_group(group_id="tg_analise")
    def analise_group(categorias: list[dict[str, Any]]) -> list[dict[str, Any]]:
        @task(
            task_id="calcular_metricas_categoria",
            map_index_template="{{ task.op_kwargs['grupo']['categoria'] }}",
        )
        def calcular_metricas_categoria(grupo: dict[str, Any]) -> dict[str, Any]:
            precos = grupo["precos"]
            quantidade = len(precos)
            return {
                "categoria": grupo["categoria"],
                "preco_medio": round(sum(precos) / quantidade, 2),
                "preco_minimo": round(min(precos), 2),
                "preco_maximo": round(max(precos), 2),
                "quantidade_produtos": quantidade,
            }

        return calcular_metricas_categoria.expand(grupo=categorias)

    @task_group(group_id="tg_persistencia")
    def persistencia_group(metricas: list[dict[str, Any]]) -> None:
        @task(task_id="preparar_tabela")
        def preparar_tabela() -> None:
            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            hook.run(
                """
                CREATE TABLE IF NOT EXISTS preco_categoria_snapshot (
                    data_referencia DATE NOT NULL,
                    categoria VARCHAR(120) NOT NULL,
                    preco_medio NUMERIC(10, 2) NOT NULL,
                    preco_minimo NUMERIC(10, 2) NOT NULL,
                    preco_maximo NUMERIC(10, 2) NOT NULL,
                    quantidade_produtos INTEGER NOT NULL,
                    atualizado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (data_referencia, categoria)
                );
                """
            )

        @task(task_id="salvar_no_postgres")
        def salvar_no_postgres(
            itens_metricas: list[dict[str, Any]], **context: Any
        ) -> None:
            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            data_ref = context["ds"]
            registros = list(itens_metricas)

            sql_snapshot = """
            INSERT INTO preco_categoria_snapshot (
                data_referencia,
                categoria,
                preco_medio,
                preco_minimo,
                preco_maximo,
                quantidade_produtos,
                atualizado_em
            )
            VALUES (%s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (data_referencia, categoria)
            DO UPDATE SET
                preco_medio = EXCLUDED.preco_medio,
                preco_minimo = EXCLUDED.preco_minimo,
                preco_maximo = EXCLUDED.preco_maximo,
                quantidade_produtos = EXCLUDED.quantidade_produtos,
                atualizado_em = NOW();
            """

            conn = hook.get_conn()
            try:
                with conn.cursor() as cur:
                    for item in registros:
                        cur.execute(
                            sql_snapshot,
                            (
                                data_ref,
                                item["categoria"],
                                item["preco_medio"],
                                item["preco_minimo"],
                                item["preco_maximo"],
                                item["quantidade_produtos"],
                            ),
                        )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()

        preparar_tabela() >> salvar_no_postgres(metricas)

    categorias_prontas = ingestao_group()
    metricas_calculadas = analise_group(categorias_prontas)
    persistencia_group(metricas_calculadas)

pipeline()