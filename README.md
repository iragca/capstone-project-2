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

## Contributors

| User Pic                                                                     | Name / Username                | Email                          | Role                             |
| ---------------------------------------------------------------------------- | ------------------------------ | ------------------------------ | -------------------------------- |
| <img src="https://avatars.githubusercontent.com/u/187070330?v=4" width="40"> | Chris Irag / [@iragca](https://github.com/iragca)          | chrisandrei.irag@1.ustp.edu.ph | Data Gathering, Developer, UI/UX |
