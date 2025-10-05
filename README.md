<h1 align="center"> Predicting user interaction with extremist conversations </h1>
<h3 align="center"> Capstone Project 2 </h3>

This project aims to predict user interaction with extremist conversations, particularly in X/Twitter. We model the problem as a network of user-tweet interaction. As such, we trained GNNs to predict the probabilities of a user interacting with extremist tweets. The following image is a screenshot of the [MVP Streamlit app](https://capstone-mvp.gari-homelab.party/) where you can start running inference and test out the model.

<img width="957" height="955" alt="2025-08-18_08-53" src="https://github.com/user-attachments/assets/e38b69de-931a-4bed-b948-b850c777c228" />

Prequisites of running the app.

- Accessing to the PocketBase with all the data
- Project dependencies (to install, read more below in [Development](#development)

## Main Documentation

The main documentation is found [here](https://iragca.github.io/capstone-project-2/).


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
├── mvps
│    ├── mvp.py                     # (MPV v1.0) CLI implementation of the MVP
│    ├── streamlit_mvp.py   
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

## Contributors

| User Pic                                                                     | Name / Username                                          | Email                          | Role                                 |
| ---------------------------------------------------------------------------- | -------------------------------------------------------- | ------------------------------ | ------------------------------------ |
| <img src="https://avatars.githubusercontent.com/u/156993659?v=4" width="40"> | Karylle dela Cruz / [@kardcy](https://github.com/kardcy) | --                             | Data Annotation, Project Lead, Paper |
| <img src="https://avatars.githubusercontent.com/u/187070330?v=4" width="40"> | Chris Irag / [@iragca](https://github.com/iragca)        | chrisandrei.irag@1.ustp.edu.ph | Database Admin, Developer, Paper     |
| --                                                                           | Dane Casiño                                              | --                             | Data Annotation, Paper               |
| --                                                                           | Usher Raymond                                            | --                             | Data Annotation, Paper                        |
