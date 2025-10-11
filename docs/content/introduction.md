# Project Setup

This documentation is intended for users who want to work with this repo and reproduce results of our paper.

To start working with this project repo make sure to do the steps specified in this page.

## Install [uv](https://docs.astral.sh/uv/)

...if you haven't already. **uv** is the best dependency manager in my own opinion.

### Standalone Installer

Run this in your terminal, or Powershell Prompt if on Windows. You can do so by using the standalone installer or by using pip.

`````{tab-set}
````{tab-item} macOS and Linux

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

````

````{tab-item} Windows

```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

````
`````

### Install using Pip

```bash
pip install uv
```

## Install dependencies

```bash
uv sync
uv run main.py install-torch-geometric-dependencies
```

`uv sync` installs project dependencies as specified in the [`pyproject.toml`](https://github.com/iragca/capstone-project-2/blob/master/pyproject.toml).

The second line installs [PyTorch Geoemetric](https://pytorch-geometric.readthedocs.io/en/latest/) dependencies, `torch-sparse` and `torch-scatter`.

## Environment Variables

Better to set these environment variables now to only think about them once.
The `.env` file should be in root repo folder, if it doesn't exist, make one.

```bash
X_USERNAME=
X_PASSWORD=
X_TOTP= # This the code for your Two Factor Authentication Token, not your TOTP

# Database
POCKETBASE_EMAIL=
POCKETBASE_PASSWORD=
POCKETBASE_URL=https://capstone.gari-homelab.party

# Scraper variables
X_RAPIDAPI_KEY=
OLD_BIRD_CONTINUATION_TOKEN=
OLD_BIRD_USERS_CONTINUATION_TOKEN=

# MLOps
MLFLOW_RECORD_ENV_VARS_IN_MODEL_LOGGING=false
MLFLOW_TRACKING_URI=https://mlflow.gari-homelab.party
```
