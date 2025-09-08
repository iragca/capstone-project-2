# Training

The training is done in `hatebert_training.py`
This project trains a machine learning model for hate speech detection using BERT-based architectures. The goal is to classify text data for hate speech and related categories.

Most important modules needed for training is found in src/data

- DatasetLoader
- Preprocessor
- GraphBuilder





## Data Sources & Preprocessing

- Datasets: Located in `data/processed/` or usually taken from the [PocketBase](../data_engineering/Database.md)

- Preprocessing: Data cleaning, tokenization, and formatting are performed in 