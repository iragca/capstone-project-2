<h1 align="center"> Predicting user interaction with extremist conversations </h1>
<h3 align="center"> Capstone Project 2 </h3>

This project aims to predict user interaction with extremist conversations, particularly in X/Twitter. We model the problem as a network of user-tweet interaction. As such, we trained GNNs to predict the probabilities of a user interacting with extremist tweets. The following image is a screenshot of the [MVP Streamlit app](https://capstone-mvp.gari-homelab.party/) where you can start running inference and test out the model.

<img width="957" height="955" alt="2025-08-18_08-53" src="https://github.com/user-attachments/assets/e38b69de-931a-4bed-b948-b850c777c228" />

Prequisites of running the app.

- Accessing to the PocketBase with all the data
- Project dependencies (to install, read more below in [Development](#development)


## How to use this repo

```
.
├── src
│    ├── architectures           # Class specifications for GNN architectures
│    ├── data                    # Classes for transforming and housing data
│    ├── db                      # PocketBase abstraction
│    ├── matplotlib              # Tools for visualization
│    ├── models                  # Custom models for added functionality, custom data models for data validation
│    ├── scraper                 # Scrapers to get data
│    ├── ui                      # Utility functions for the Streamlit UI
│    └── utils                   # Utility functions
├── main.py                      # Compilation of small scripts (Usage: uv run main.py <function-name> <args/kwargs>)
├── mvp.py                       # (MPV v1.0) CLI implementation of the MVP
└── streamlit_mvp.py             # (MPV v2.0) Streamlit implementation of the MVP (Usage: uv run streamlit run streamlit_mvp.py)
```

## Development

To install dependencies run:

- `uv sync`
- `uv run main.py install-torch-geometric-dependencies`

We are only working with [uv](https://docs.astral.sh/uv/getting-started/installation/) so if you don't have it, please install it using `pip install uv`.

### main.py

This is mostly just a combination of multiple data engineering scripts facilitating scraping, moving data, preprocessing tasks. Akin to a "command center/panel".
It also includes helper scripts like `multiple-training` for manual grid hyperparameter gridsearch, `install-torch-geometric-dependencies` necessary
for the libraries Pytorch Geometric and friends. This might need clean up 🧹.

### Training

The training is done in `hatebert_training.py`

Most important modules needed for training is found in src/data

- DatasetLoader
- Preprocessor
- GraphBuilder

#### MLOps

To maintain a streamlined supervision of training runs, we use [mlflow](https://mlflow.org/). Which allows us to view, compare and revisit past runs. The runs include data about the hyperparameters, model artifacts, and system resources used. All this data can be download in `csv` format if needed. 

The only [mlflow instance](https://mlflow.gari-homelab.party/) we use is on a home server ran by one of the members to minimize cloud costs.


### Using MVPs

### MVP 1.0: CLI Tool

This MVP showcases the core idea of how inference will work in future MVPs and the final production build.

To run inference, simply run `uv run mvp.py --user <user_id>` or `uv run mvp.py --username <username>`. The results will be printed as a `list` of `tuple`'s that contain the PocketBase `Record` object alongside its score/probability, i.e `(<Record: 7a13kcka0>, 0.52019314)`. 


### MVP 2.0: Streamlit

This MVP showcases the implementation of the core inference functionality with a basic user interface.

To first run inference, we need to run the Streamlit server first by running `uv run streamlit run streamlit_mvp.py`. This requires working connection to the PocketBase database.

### MVP 3.0

Coming soon.

## Contributors

| User Pic                                                                     | Name / Username                | Email                          | Role                             |
| ---------------------------------------------------------------------------- | ------------------------------ | ------------------------------ | -------------------------------- |
| <img src="https://avatars.githubusercontent.com/u/156993659?v=4" width="40"> | Karylle dela Cruz / [@kardcy](https://github.com/kardcy)        | -- | Data Annotation, Project Lead, Paper |
| <img src="https://avatars.githubusercontent.com/u/187070330?v=4" width="40"> | Chris Irag / [@iragca](https://github.com/iragca)               | chrisandrei.irag@1.ustp.edu.ph | Database Admin, Developer, Paper |
| -- | Dane Casiño           | -- | Data Annotation, Paper |
| -- | Usher Raymond         | -- | Paper |
