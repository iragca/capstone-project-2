# Training Script

The training is done in [`model_training.py`](https://github.com/iragca/capstone-project-2/blob/master/model_training.py).

The three main classes we need are:

- [`DatasetLoader`](../modeling/Data_Loading.md): For loading the data
- [`Preprocessor`](../modeling//Preprocessing.md): Preprocessing the data
- [`GraphBuilder`](../modeling/Graph_Building.md): Builds a graph using the data as input.

## Arguments

These arguments are available for configuration as decided by the user.

| Argument            | Type    | Default                         | Description                                                     |
| ------------------- | ------- | ------------------------------- | --------------------------------------------------------------- |
| `--epochs`          | `int`   | `50`                            | Number of epochs to train. Must be > 0.                         |
| `--hidden_dim`      | `int`   | `128`                           | Hidden dimension of the model. Must be > 0.                     |
| `--threshold`       | `float` | `0.5`                           | Threshold for classification. Must be between 0 and 1.          |
| `--device`          | `str`   | `cuda` if available, else `cpu` | Device to use for training (`cpu` or `cuda`).                   |
| `--num_layers`      | `int`   | `8`                             | Number of layers in the model.                                  |
| `--heterogeneous`   | `flag`  | `False`                         | If set, use a heterogeneous graph instead of a homogeneous one. |
| `--directed`        | `flag`  | `False`                         | If set, build a directed graph instead of an undirected one.    |
| `--c`               | `flag`  | `False`                         | Configuration/development/testing flag for the script.          |
| `--save-model`      | `flag`  | `False`                         | If set, save the best model after training.                     |
| `--graphsage`       | `flag`  | `False`                         | If set, use GraphSAGE convolutional layers instead of GCN.      |
| `--experiment-name` | `str`   | `Production`                    | Sets the experiment name to be used in the MLflow dashboard.    |

### Single Experiment

To run a single experiment run:

```bash
uv run model_training.py --epochs 200 --hidden_dim 24 --graphsage --save-model
```

### Multiple Trials

If you want run multiple trials of the same configuration, it is best to use `multiple_trials.py`

```bash
uv run multiple_trials.py
```
<video controls width="600">
  <source src="../../_static/videos/multi-runs.mp4" type="video/mp4">
  Your browser does not support the video tag.
</video>

The CLI will prompt you of your available choices. The yellow text means it is the default, replace the default values with your own if needed. Then simply press `Enter`.


## Choosing features to use

To choose features, simply add / comment out (remove) the name of the column in the corresponding list as input to the {class}`src.models.Features` class.

Example:

```python
node_features = Features(
        tweet=[
            "favorite_count",
            # "retweet_count",
            # "bookmark_count",
            "reply_count",
            "quote_count",
            # "views",
            "source",
            "is_hateful",
        ],
        user=[
            # "favourites_count",
            # "follower_count",
            # "following_count",
            # "number_of_tweets",
            # "listed_count",
            # "is_blue_verified",
            "friends",
        ],
    )
```

This shows that the features used for the {ref}`tweet` node types are

- favorite_count
- reply_count
- quote_count
- source
- is_hateful

and for {ref}`user` is

- friends
