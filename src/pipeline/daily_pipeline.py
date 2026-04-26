from src.ingestion.oil_price_ingest import main as oil_price_ingest_main
from src.ingestion.ingest import main as rss_ingest_main
from src.processing.article_processor import main as article_processor_main
from src.processing.aggregate_daily_news import main as aggregate_daily_news_main
from src.processing.build_model_features import build_model_features
from src.monitoring.retraining_check import main as retraining_check_main
from src.monitoring.auto_retrain import auto_retrain


def main() -> None:
    print("\n=== Daily Pipeline Start ===")

    print("\n[1/7] Ingesting oil prices...")
    oil_price_ingest_main()

    print("\n[2/7] Ingesting RSS articles...")
    rss_ingest_main()

    print("\n[3/7] Processing articles...")
    article_processor_main()

    print("\n[4/7] Aggregating daily news...")
    aggregate_daily_news_main()

    print("\n[5/7] Building model features...")
    build_model_features()

    print("\n[6/7] Checking retraining need...")
    retraining_check_main()

    print("\n[7/7] Auto-retraining if needed...")
    auto_retrain()

    print("\n=== Daily Pipeline Complete ===")


if __name__ == "__main__":
    main()