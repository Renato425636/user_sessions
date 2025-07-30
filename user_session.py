import logging
import sys
import os
import yaml
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import (
    col, lag, unix_timestamp, when, sum, concat, lit, min, max, 
    count, countDistinct, collect_set, to_timestamp, hour, dayofweek, avg
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

class AdvancedRetailAnalyticsPipeline:
    def __init__(self, config_path: str):
        self.config = self._load_config(config_path)
        self.spark = self._initialize_spark()
        self._setup_logging()

    def _load_config(self, config_path: str) -> dict:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)

    def _setup_logging(self):
        logging.basicConfig(level=self.config.get("log_level", "INFO"), format="%(asctime)s - [%(levelname)s] - %(message)s")
        self.logger = logging.getLogger(self.config.get("pipeline_name", "RetailPipeline"))
    
    def _initialize_spark(self) -> SparkSession:
        self.logger = logging.getLogger(self.config.get("pipeline_name", "RetailPipeline"))
        try:
            return SparkSession.builder \
                .appName(self.config["spark"]["app_name"]) \
                .master(self.config["spark"]["master"]) \
                .config("spark.sql.session.timeZone", "UTC") \
                .getOrCreate()
        except Exception as e:
            self.logger.critical(f"Falha ao inicializar SparkSession: {e}", exc_info=True)
            sys.exit(1)

    def _load_and_clean_data(self) -> DataFrame:
        """Carrega e limpa os dados brutos de eventos."""
        source_path = self.config["data"]["source_path"]
        self.logger.info(f"Carregando dados de: {source_path}")
        if not os.path.exists(source_path):
            self.logger.critical(f"Arquivo de dados não encontrado em '{source_path}'. Por favor, baixe o dataset do Kaggle e coloque-o neste caminho.")
            sys.exit(1)
            
        schema = StructType([
            StructField("event_time", StringType(), True), StructField("event_type", StringType(), True),
            StructField("product_id", LongType(), True), StructField("category_id", LongType(), True),
            StructField("category_code", StringType(), True), StructField("brand", StringType(), True),
            StructField("price", DoubleType(), True), StructField("user_id", LongType(), True)
        ])
        
        df = self.spark.read.csv(source_path, header=True, schema=schema).limit(1000000) 
        
        df_cleaned = df.withColumn("event_timestamp", to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss 'UTC'")) \
                       .na.drop(subset=["user_id", "event_timestamp"]) \
                       .drop("event_time")
        
        self.logger.info("Dados carregados e limpos com sucesso.")
        return df_cleaned

    def _sessionize_events(self, df: DataFrame) -> DataFrame:
        """Aplica a lógica de sessionização para atribuir um ID de sessão customizado."""
        self.logger.info("Iniciando o processo de sessionização...")
        
        user_window = Window.partitionBy("user_id").orderBy("event_timestamp")
        session_timeout = self.config["session_timeout_seconds"]

        df_with_session_flag = df.withColumn("previous_timestamp", lag("event_timestamp", 1).over(user_window)) \
                                 .withColumn("time_diff", unix_timestamp(col("event_timestamp")) - unix_timestamp(col("previous_timestamp"))) \
                                 .withColumn("new_session_flag", when((col("time_diff") > session_timeout) | col("time_diff").isNull(), 1).otherwise(0))
        
        df_with_session_group = df_with_session_flag.withColumn("session_group", sum("new_session_flag").over(user_window))
        
        df_sessionized = df_with_session_group.withColumn("session_id", concat(col("user_id"), lit("_"), col("session_group"))) \
                                              .drop("previous_timestamp", "time_diff", "new_session_flag", "session_group")
        
        self.logger.info("Sessionização concluída.")
        return df_sessionized

    def _aggregate_to_session_features(self, df: DataFrame) -> DataFrame:
        """Agrega os eventos por sessão para criar features analíticas, incluindo análise de funil."""
        self.logger.info("Iniciando engenharia de features por sessão...")
        
        session_features_df = df.groupBy("user_id", "session_id").agg(
            min("event_timestamp").alias("session_start_time"),
            max("event_timestamp").alias("session_end_time"),
            count("*").alias("total_events"),
            countDistinct("product_id").alias("distinct_products_viewed"),
 
            collect_set("event_type").alias("unique_event_types")
        )
        

        final_df = session_features_df \
            .withColumn("session_duration_seconds", unix_timestamp(col("session_end_time")) - unix_timestamp(col("session_start_time"))) \
            .withColumn("hour_of_day", hour(col("session_start_time"))) \
            .withColumn("day_of_week", dayofweek(col("session_start_time"))) \
            .withColumn("funnel_stage",
                when(col("unique_event_types").contains("purchase"), "completed_purchase")
                .when(col("unique_event_types").contains("cart"), "added_to_cart")
                .when(col("unique_event_types").contains("view"), "view_only")
                .otherwise("other")
            )
        
        self.logger.info("Engenharia de features por sessão concluída.")
        return final_df

    def _aggregate_to_user_features(self, session_features_df: DataFrame) -> DataFrame:
        """Agrega as features de sessão para criar um resumo por usuário (visão 360)."""
        self.logger.info("Iniciando agregação para features de usuário (visão 360)...")
        
        user_window = Window.partitionBy("user_id").orderBy("session_start_time")
        

        sessions_with_lag = session_features_df.withColumn(
            "previous_session_start", lag("session_start_time", 1).over(user_window)
        )
        
        user_summary_df = sessions_with_lag.groupBy("user_id").agg(
            count("session_id").alias("total_sessions"),
            sum("total_events").alias("total_events_lifetime"),
            avg("session_duration_seconds").alias("avg_session_duration_seconds"),
            sum(when(col("funnel_stage") == "completed_purchase", 1).otherwise(0)).alias("total_purchase_sessions"),

            avg(unix_timestamp(col("session_start_time")) - unix_timestamp(col("previous_session_start"))).alias("avg_time_between_sessions_sec")
        ).withColumn("avg_time_between_sessions_hours", col("avg_time_between_sessions_sec") / 3600)
        
        self.logger.info("Agregação por usuário concluída.")
        return user_summary_df

    def run(self):
        """Orquestra a execução completa do pipeline analítico."""
        self.logger.info("--- Iniciando Pipeline Avançado de Análise de Sessões ---")
        
        cleaned_df = self._load_and_clean_data()
        sessionized_events_df = self._sessionize_events(cleaned_df)
        

        sessionized_events_df.cache()
        self.logger.info(f"DataFrame de eventos com sessão cacheado. Total de eventos: {sessionized_events_df.count()}")
        

        session_features_df = self._aggregate_to_session_features(sessionized_events_df)
        session_features_output_path = self.config["data"]["output_path"]["session_features"]
        session_features_df.write.mode("overwrite").parquet(session_features_output_path)
        self.logger.info(f"Tabela de features de sessão salva em: {session_features_output_path}")


        user_summary_df = self._aggregate_to_user_features(session_features_df)
        user_summary_output_path = self.config["data"]["output_path"]["user_summary"]
        user_summary_df.write.mode("overwrite").parquet(user_summary_output_path)
        self.logger.info(f"Tabela de resumo de usuário salva em: {user_summary_output_path}")

        self.logger.info("--- Amostra dos Resultados Finais ---")
        self.logger.info("Tabela de Features por Sessão:")
        session_features_df.show(10, truncate=False)
        self.logger.info("Tabela de Resumo por Usuário:")
        user_summary_df.show(10, truncate=False)
        
        sessionized_events_df.unpersist()
        self.logger.info("--- Pipeline concluído com sucesso ---")

    def stop(self):
        self.logger.info("Finalizando sessão Spark.")
        self.spark.stop()

if __name__ == "__main__":
    CONFIG_FILE = "config.yaml"
    
    if not os.path.exists(CONFIG_FILE):
        print(f"Erro: Arquivo de configuração '{CONFIG_FILE}' não encontrado.", file=sys.stderr)
        sys.exit(1)
        
    pipeline = None
    try:
        pipeline = AdvancedRetailAnalyticsPipeline(config_path=CONFIG_FILE)
        pipeline.run()
    except Exception as e:
        print(f"Ocorreu um erro fatal durante a execução do pipeline.", file=sys.stderr)
        logging.getLogger(__name__).critical("Erro fatal no pipeline.", exc_info=True)
        sys.exit(1)
    finally:
        if pipeline:
            pipeline.stop()
