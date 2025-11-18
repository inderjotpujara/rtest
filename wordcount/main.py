"""PySpark Word Count - Classic distributed word counting."""
import os
import logging
import structlog
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower, col, regexp_replace, count as spark_count

structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO)
)

logger = structlog.get_logger()


def create_spark_session(app_name: str) -> SparkSession:
    """Create Spark session based on environment."""
    is_bolt = os.getenv("IS_BOLT", "false").lower() == "true"
    
    if is_bolt:
        logger.info("Creating Spark Connect session for Bolt environment")
        return SparkSession.builder \
            .appName(app_name) \
            .remote("sc://localhost:15002") \
            .getOrCreate()
    else:
        logger.info("Creating standard SparkSession for production environment")
        return SparkSession.builder \
            .appName(app_name) \
            .getOrCreate()


def get_sample_text() -> list:
    """Generate sample text data for word counting."""
    return [
        "Apache Spark is a unified analytics engine for large-scale data processing",
        "Spark provides high-level APIs in Java, Scala, Python and R",
        "PySpark is the Python API for Apache Spark",
        "Word count is a classic example of distributed computing with Spark",
        "Spark can process data from various sources including HDFS, S3, and databases",
        "The DataFrame API makes Spark programming easier and more intuitive",
        "Spark supports batch processing, streaming, machine learning, and graph processing",
        "PySpark applications can run on Kubernetes clusters for container orchestration",
        "Spark Connect enables remote connectivity to Spark clusters",
        "Data processing at scale requires distributed computing frameworks like Spark"
    ]


def perform_word_count(spark: SparkSession, input_data: list) -> None:
    """Execute word count operation on input data."""
    logger.info("Starting word count operation", input_lines=len(input_data))
    
    # Create DataFrame from input data
    df = spark.createDataFrame([(line,) for line in input_data], ["line"])
    logger.info("Created DataFrame from input data", row_count=df.count())
    
    # Tokenize: split lines into words
    words_df = df.select(
        explode(split(col("line"), r"\s+")).alias("word")
    )
    
    # Clean: remove punctuation, convert to lowercase, filter empty strings
    clean_words_df = words_df.select(
        lower(regexp_replace(col("word"), r"[^\w]", "")).alias("word")
    ).filter(col("word") != "")
    
    logger.info("Tokenized and cleaned words", word_count=clean_words_df.count())
    
    # Count word frequencies
    word_counts_df = clean_words_df.groupBy("word") \
        .agg(spark_count("*").alias("count")) \
        .orderBy(col("count").desc())
    
    # Display results
    total_unique_words = word_counts_df.count()
    logger.info("Word count completed", unique_words=total_unique_words)
    
    print("\n" + "="*60)
    print("WORD COUNT RESULTS")
    print("="*60)
    print(f"Total unique words: {total_unique_words}")
    print(f"Total word occurrences: {clean_words_df.count()}")
    print("\nTop 20 most frequent words:")
    print("-"*60)
    
    word_counts_df.show(20, truncate=False)
    
    # Show some statistics
    print("\nWord frequency distribution:")
    print("-"*60)
    word_counts_df.groupBy("count") \
        .agg(spark_count("*").alias("words_with_this_frequency")) \
        .orderBy(col("count").desc()) \
        .show(10, truncate=False)


def main():
    """Main application entry point."""
    logger.info("Starting PySpark Word Count Application")
    
    try:
        # Create Spark session
        spark = create_spark_session("WordCount")
        logger.info("Spark session created successfully")
        
        # Check for input file or use sample data
        input_file = os.getenv("INPUT_FILE", "data/sample.txt")
        
        if os.path.exists(input_file):
            logger.info("Reading input from file", file_path=input_file)
            with open(input_file, 'r') as f:
                input_data = [line.strip() for line in f if line.strip()]
        else:
            logger.info("Using sample text data")
            input_data = get_sample_text()
        
        # Perform word count
        perform_word_count(spark, input_data)
        
        logger.info("Word count application completed successfully")
        
    except Exception as e:
        logger.error("Application failed", error=str(e), exc_info=True)
        raise
    finally:
        if 'spark' in locals():
            spark.stop()
            logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
