import polars as pl
import json
from pathlib import Path
import logging

# Configuração de log
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_processing():
    """
    Camada de Processamento Refinada:
    1. Deduplicação de registros (correção de peso dobrado).
    2. Cálculo de KM de Retorno (preservação de Opex).
    3. Tratamento de truncagem de dados.
    4. Remoção de colunas redundantes (_txt).
    """
    project_root = Path(__file__).resolve().parents[2]
    interim_dir = project_root / "data" / "02_interim"
    processed_dir = project_root / "data" / "03_processed"
    
    processed_dir.mkdir(parents=True, exist_ok=True)
    
    logging.info("Iniciando processamento com regras de negócio refinadas...")

    try:
        # 1. Carga de Dados
        df_rs = pl.read_parquet((interim_dir / "rotasul_ingested.parquet").as_posix())
        df_lp = pl.read_parquet((interim_dir / "logiprime_ingested.parquet").as_posix())
        df = pl.concat([df_rs, df_lp], how="vertical")

        initial_count = df.height
        
        # 2. Deduplicação (Hipótese C: Peso Dobrado)
        # Removemos duplicatas exatas de entrega para corrigir o erro de integração
        df = df.unique(subset=[
            "codigo_rota", "seq_entrega", "pdv_id", 
            "peso_kg_entrega", "volume_m3_entrega", 
            "km_trecho", "tempo_trecho_min"
        ])
        deduplicated_count = initial_count - df.height
        logging.info(f"Deduplicação concluída: {deduplicated_count} registros redundantes removidos.")


        # 3.1 Remoção de colunas de erro negativo
        cols_to_drop = [c for c in df.columns if c.endswith("_txt") or c in ["erro_negativo_km", "erro_negativo_tempo"]]
        df = df.drop(cols_to_drop)

        # 4. Tratamento de Negativos (Módulo) e preenchimento de nulos
        df = df.with_columns([
            pl.col("km_trecho").abs().fill_null(0.0),
            pl.col("tempo_trecho_min").abs().fill_null(0.0),
            pl.col("peso_kg_entrega").fill_null(0.0),
            pl.col("volume_m3_entrega").fill_null(0.0),
            pl.col("km_ate_ponto").fill_null(0.0) if "km_ate_ponto" in df.columns else pl.lit(0.0),
            pl.col("tempo_ate_ponto_min").fill_null(0.0) if "tempo_ate_ponto_min" in df.columns else pl.lit(0.0)
        ])

        # 5. Lógica de KM e Tempo (Hipótese B: Retorno à Base vs Truncagem)
        # Agregamos os valores granulares por rota
        route_stats = df.group_by("codigo_rota").agg([
            pl.col("km_trecho").sum().alias("soma_km_granular"),
            pl.col("tempo_trecho_min").sum().alias("soma_tempo_granular"),
            pl.col("km_total_rota").first().alias("km_total_bruto"),
            pl.col("tempo_total_rota_min").first().alias("tempo_total_bruto"),
            pl.col("peso_kg_entrega").sum().alias("peso_total_calculado"),
            pl.col("volume_m3_entrega").sum().alias("vol_total_calculado")
        ])

        # Aplicamos as regras de KM de Retorno e Truncagem
        route_stats = route_stats.with_columns([
            # Se informado > soma -> Diferença é o Retorno à Base
            # Se soma > informado -> Erro de truncagem, assumimos a soma
            pl.when(pl.col("km_total_bruto") > pl.col("soma_km_granular"))
            .then(pl.col("km_total_bruto"))
            .otherwise(pl.col("soma_km_granular"))
            .alias("km_total_final"),

            pl.when(pl.col("km_total_bruto") > pl.col("soma_km_granular"))
            .then(pl.col("km_total_bruto") - pl.col("soma_km_granular"))
            .otherwise(0.0)
            .alias("km_retorno_base"),

            # Lógica análoga para tempo
            pl.when(pl.col("tempo_total_bruto") > pl.col("soma_tempo_granular"))
            .then(pl.col("tempo_total_bruto"))
            .otherwise(pl.col("soma_tempo_granular"))
            .alias("tempo_total_final")
        ])

        # 6. Consolidação da Base Processada
        df = df.join(
            route_stats.select([
                "codigo_rota", "km_total_final", "km_retorno_base", 
                "tempo_total_final", "peso_total_calculado", "vol_total_calculado"
            ]), 
            on="codigo_rota", how="left"
        )

        # Substituímos as colunas originais pelas saneadas
        df = df.with_columns([
            pl.col("km_total_final").alias("km_total_rota"),
            pl.col("tempo_total_final").alias("tempo_total_rota_min"),
            pl.col("peso_total_calculado").alias("peso_kg_total_rota"),
            pl.col("vol_total_calculado").alias("volume_m3_total_rota")
        ]).drop(["km_total_final", "tempo_total_final", "peso_total_calculado", "vol_total_calculado"])

        # 7. Relatório de Qualidade
        report = {
            "diagnostico_final": {
                "registros_iniciais": initial_count,
                "duplicatas_removidas": deduplicated_count,
                "registros_finais": df.height,
                "km_total_retorno_identificado": float(df.select("km_retorno_base").sum().item())
            },
            "avisos": "A base processada agora contém a coluna 'km_retorno_base' para análise de Opex."
        }

        # 8. Exportação
        with open(processed_dir / "relatorio_qualidade.json", 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=4, ensure_ascii=False)
        
        df.write_parquet((processed_dir / "dados_limpos.parquet").as_posix())
        logging.info("Processamento finalizado com sucesso. Base salva na camada 03_processed.")

    except Exception as e:
        logging.error(f"Falha no processamento: {e}")
        raise

if __name__ == "__main__":
    run_processing()
