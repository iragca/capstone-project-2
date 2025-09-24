import polars as pl
from mlflow.entities import Experiment, Metric, Run
from mlflow.tracking import MlflowClient
from tqdm import tqdm

from src.config import Settings


class ExperimentService:
    """
    Service class for retrieving and processing MLflow experiment runs.

    This class provides utilities to:
    - Fetch MLflow runs by experiment name or run ID.
    - Retrieve performance metrics as raw MLflow objects or Polars DataFrames.
    - Enforce schemas for experiment metrics and hyperparameters using Polars.
    """

    def __init__(
        self, tracking_uri: str = Settings.MLFLOW_TRACKING_URI.value
    ) -> list[Metric] | pl.DataFrame:
        """
        Initialize the RunService with an MLflow tracking URI.

        Parameters
        ----------
        tracking_uri : str, optional
            The MLflow tracking server URI. Defaults to the value in Settings.
        """
        self.client = MlflowClient(tracking_uri=tracking_uri)

    def get_run_performance_as_list(self, run_id: str) -> list[Metric]:
        """
        Retrieve all metrics for a given run as raw MLflow Metric objects.

        Parameters
        ----------
        run_id : str
            The unique identifier of the MLflow run.

        Returns
        -------
        list of Metric
            List of MLflow `Metric` objects containing history across steps.
        """
        return [
            metric_record
            for metric_name in self.metrics_schema.keys()
            for metric_record in self.client.get_metric_history(run_id, metric_name)
        ]

    def get_run_performance_as_df(self, run_id: str) -> pl.DataFrame:
        """
        Retrieve all metrics for a given run as a Polars DataFrame.

        Parameters
        ----------
        run_id : str
            The unique identifier of the MLflow run.

        Returns
        -------
        pl.DataFrame
            DataFrame with columns:
            - run_id
            - timestamp
            - step
            - metric
            - value
        """
        rows = []
        for metric_name in self.metrics_schema.keys():
            history = self.client.get_metric_history(run_id, metric_name)
            rows.extend(
                {
                    "run_id": run_id,
                    "timestamp": metric_record.timestamp,
                    "step": metric_record.step,
                    "metric": metric_name,
                    "value": float(metric_record.value),
                }
                for metric_record in history
            )
        return pl.DataFrame(rows, schema=self.run_schema)

    def get_experiment_runs_as_list(
        self,
        experiment_name: str,
        _full_experiment_name: str = None,
    ) -> list[Run]:
        """
        Retrieve all runs for a given experiment as a list of Run objects.

        Parameters
        ----------
        experiment_name : str
            The name of the experiment (without CAPSTONE prefix).
        _full_experiment_name : str, optional
            Override the prefixed name and use a fully-qualified experiment name.

        Returns
        -------
        list of Run
            List of MLflow `Run` objects for the experiment.
        """
        experiment = self._resolve_experiment(experiment_name, _full_experiment_name)
        return self.client.search_runs(experiment_ids=[experiment.experiment_id])

    def get_experiment_runs_as_df(
        self,
        experiment_name: str,
        _full_experiment_name: str = None,
    ) -> pl.DataFrame:
        """
        Retrieve all runs for a given experiment as a Polars DataFrame.

        Parameters
        ----------
        experiment_name : str
            The name of the experiment (without CAPSTONE prefix).
        _full_experiment_name : str, optional
            Override the prefixed name and use a fully-qualified experiment name.

        Returns
        -------
        pl.DataFrame
            DataFrame with metrics, hyperparameters, and run IDs.
        """
        runs = self.get_experiment_runs_as_list(experiment_name, _full_experiment_name)
        return self._to_dataframe(runs)

    def _to_dataframe(self, runs: list[Run]) -> pl.DataFrame:
        """
        Convert a list of MLflow runs into a Polars DataFrame.

        Parameters
        ----------
        runs : list of Run
            MLflow run objects to convert.

        Returns
        -------
        pl.DataFrame
            DataFrame with experiment metrics, hyperparameters, and run IDs.

        Raises
        ------
        ValueError
            If no runs are provided.
        """
        if not runs:
            raise ValueError("No runs found for the specified experiment.")
        all_rows_df = pl.DataFrame(schema=self.experiment_schema)

        for run in runs:
            params: dict = run.data.params
            metrics: dict = run.data.metrics
            df = pl.DataFrame(
                {
                    "run_id": run.info.run_id,
                    # Test metrics
                    "TEST: ROC-AUC": metrics.get("TEST: ROC-AUC", None),
                    "TEST: F1 Score": metrics.get("TEST: F1 Score", None),
                    "TEST: Precision": metrics.get("TEST: Precision", None),
                    "TEST: Recall": metrics.get("TEST: Recall", None),
                    "TEST: AUC": metrics.get("TEST: AUC", None),
                    # General metrics
                    "Loss": metrics.get("Loss", None),
                    # Train metrics
                    "TRAIN: AUC": metrics.get("TRAIN: AUC", None),
                    "TRAIN: F1 Score": metrics.get("TRAIN: F1 Score", None),
                    "TRAIN: Precision": metrics.get("TRAIN: Precision", None),
                    "TRAIN: ROC-AUC": metrics.get("TRAIN: ROC-AUC", None),
                    "TRAIN: Recall": metrics.get("TRAIN: Recall", None),
                    # Validation metrics
                    "VAL: AUC": metrics.get("VAL: AUC", None),
                    "VAL: F1 Score": metrics.get("VAL: F1 Score", None),
                    "VAL: Precision": metrics.get("VAL: Precision", None),
                    "VAL: ROC-AUC": metrics.get("VAL: ROC-AUC", None),
                    "VAL: Recall": metrics.get("VAL: Recall", None),
                    # Hyperparameters
                    "epochs": int(params.get("epochs", None)),
                    "hidden_dim": int(params.get("hidden_dim", None)),
                    "threshold": float(params.get("threshold", None)),
                    "device": params.get("device", None),
                    "num_layers": int(params.get("num_layers", None)),
                    "homogeneous": bool(params.get("homogeneous", None)),
                    "directed": bool(params.get("directed", None)),
                    "graphsage": bool(params.get("graphsage", None)),
                    "experiment_name": params.get("experiment_name", None),
                }
            )
            all_rows_df = pl.concat([all_rows_df, df], how="vertical")
        return all_rows_df

    def all_runs_history(self, experiment_name: str) -> pl.DataFrame:
        """
        Retrieve metric histories for all runs in an experiment as a single DataFrame.

        Parameters
        ----------
        experiment_name : str
            The name of the MLflow experiment.

        Returns
        -------
        pl.DataFrame
            A Polars DataFrame containing the combined metric history of all runs.
        """
        experiment_df = self.get_experiment_runs_as_df(experiment_name)

        histories = []
        for row in tqdm(
            experiment_df.iter_rows(),
            desc="Fetching Run Histories",
            unit="run",
            total=experiment_df.height,
        ):
            run_id = row[0]  # first column is run_id
            histories.append(self.get_run_performance_as_df(run_id=run_id))

        return (
            pl.concat(histories, how="vertical")
            if histories
            else pl.DataFrame(schema=self.run_schema)
        )

    def _resolve_experiment(
        self, experiment_name: str, _full_experiment_name: str = None
    ) -> Experiment:
        """
        Resolve and validate an experiment by name.

        Parameters
        ----------
        experiment_name : str
            The name of the experiment (without CAPSTONE prefix).
        _full_experiment_name : str, optional
            Override the prefixed name and use a fully-qualified experiment name.

        Returns
        -------
        Experiment
            The MLflow Experiment object.

        Raises
        ------
        ValueError
            If the experiment cannot be found.
        """
        name = _full_experiment_name or f"[CAPSTONE-2] {experiment_name}"
        experiment = self.client.get_experiment_by_name(name)
        if experiment is None:
            raise ValueError(f"Experiment '{name}' does not exist.")
        return experiment

    @property
    def base_schema(self) -> dict[str, type]:
        """Base schema including run ID."""
        return {"run_id": str}

    @property
    def metrics_schema(self) -> dict[str, type]:
        """Schema for train/val/test metrics and loss values."""
        return {
            "TEST: ROC-AUC": float,
            "TEST: F1 Score": float,
            "TEST: Precision": float,
            "TEST: Recall": float,
            "TEST: AUC": float,
            "Loss": float,
            "TRAIN: AUC": float,
            "TRAIN: F1 Score": float,
            "TRAIN: Precision": float,
            "TRAIN: ROC-AUC": float,
            "TRAIN: Recall": float,
            "VAL: AUC": float,
            "VAL: F1 Score": float,
            "VAL: Precision": float,
            "VAL: ROC-AUC": float,
            "VAL: Recall": float,
        }

    @property
    def hyperparams_schema(self) -> dict[str, type]:
        """Schema for run hyperparameters."""
        return {
            "epochs": int,
            "hidden_dim": int,
            "threshold": float,
            "device": str,
            "num_layers": int,
            "homogeneous": bool,
            "directed": bool,
            "graphsage": bool,
            "experiment_name": str,
        }

    @property
    def run_schema(self) -> dict[str, type]:
        """Schema for run-level metric history records."""
        return {
            **self.base_schema,
            "timestamp": int,
            "step": int,
            "metric": str,
            "value": float,
        }

    @property
    def experiment_schema(self) -> dict[str, type]:
        """Schema for experiment-level aggregated runs."""
        return {
            **self.base_schema,
            **self.metrics_schema,
            **self.hyperparams_schema,
        }
