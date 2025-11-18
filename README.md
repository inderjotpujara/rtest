# PySpark Word Count Application

A classic distributed word counting application built with PySpark 4.0.0, demonstrating fundamental Spark operations in a containerized environment.

## Features

- Environment-aware Spark session creation (Bolt/Production)
- DataFrame API for distributed processing
- Text tokenization and cleaning
- Word frequency analysis
- Spark Connect compatibility (no sparkContext access)
- Structured logging with configurable levels
- Sample data included for immediate execution

## Architecture

The application uses PySpark DataFrame transformations:
1. **Read**: Load text data from file or sample
2. **Tokenize**: Split text into individual words
3. **Clean**: Normalize (lowercase, remove punctuation)
4. **Aggregate**: Count word frequencies using groupBy
5. **Sort**: Order by frequency descending
6. **Display**: Show results with statistics

## Usage

### Run with sample data
```bash
python3 -m wordcount
```

### Run with custom input file
```bash
INPUT_FILE=/path/to/your/file.txt python3 -m wordcount
```

## Environment Variables

- `IS_BOLT`: Set to "true" for Spark Connect mode (default: "false")
- `INPUT_FILE`: Path to input text file (default: "data/sample.txt")

## Output

The application displays:
- Total unique words
- Total word occurrences
- Top 20 most frequent words
- Word frequency distribution statistics

## Requirements

- PySpark 4.0.0 with Spark Connect support
- Python 3.11+
- Container runtime (Docker/Kubernetes)
