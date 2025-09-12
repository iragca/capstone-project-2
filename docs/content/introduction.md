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
