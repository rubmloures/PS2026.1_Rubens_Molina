import duckdb
import polars as pl
from pathlib import Path
import logging

# Configuração básica de log para monitorar o pipeline
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_ingestion():
    """
    Função responsável por ler os arquivos CSV brutos usando DuckDB 
    e salvá-los como arquivos Parquet otimizados usando Polars.
    """
    # 1. Definindo os caminhos dinamicamente
    project_root = Path(__file__).resolve().parents[2]
    raw_dir = project_root / "data" / "01_raw"
    interim_dir = project_root / "data" / "02_interim"
    
    # Garante que a pasta de destino exista
    interim_dir.mkdir(parents=True, exist_ok=True)
    
    logging.info("Iniciando a camada de ingestão de dados...")

    try:
        # 2. Ingestão dos dados da RotaSul (12 arquivos de uma vez usando DuckDB)
        rotasul_pattern = (raw_dir / "RotaSul_entregas_2024_*.csv").as_posix()
        logging.info(f"Lendo múltiplos arquivos da RotaSul: {rotasul_pattern}")
        
        # DuckDB lê todos os CSVs que dão match no padrão e converte para Polars DataFrame (.pl())
        df_rotasul = duckdb.query(f"SELECT * FROM read_csv_auto('{rotasul_pattern}')").pl()
        
        # Salvando em formato Parquet na camada interim
        rotasul_out_path = interim_dir / "rotasul_ingested.parquet"
        df_rotasul.write_parquet(rotasul_out_path)
        logging.info(f"RotaSul ingerida com sucesso! Linhas: {df_rotasul.height}. Salvo em: {rotasul_out_path}")

        # 3. Ingestão dos dados da LogiPrime (1 arquivo)
        logiprime_file = (raw_dir / "LogiPrime_entregas_2024.csv").as_posix()
        logging.info(f"Lendo arquivo da LogiPrime: {logiprime_file}")
        
        df_logiprime = duckdb.query(f"SELECT * FROM read_csv_auto('{logiprime_file}')").pl()
        
        # Salvando em formato Parquet na camada interim
        logiprime_out_path = interim_dir / "logiprime_ingested.parquet"
        df_logiprime.write_parquet(logiprime_out_path)
        logging.info(f"LogiPrime ingerida com sucesso! Linhas: {df_logiprime.height}. Salvo em: {logiprime_out_path}")

        logging.info("Pipeline de ingestão finalizado com sucesso.")

    except Exception as e:
        logging.error(f"Erro durante a ingestão de dados: {e}")
        raise

if __name__ == "__main__":
    run_ingestion()