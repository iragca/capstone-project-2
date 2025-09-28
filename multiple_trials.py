import subprocess

import questionary
import torch
from pushbullet import Pushbullet

from src.config import Settings

try:
    # ==== typed args ====
    trials = questionary.text(
        "Number of trials?", default="50", validate=lambda x: x.isdigit()
    ).unsafe_ask()

    experiment_name = questionary.select(
        "Choose an experiment:",
        choices=[
            "Random Experiment",
            "Hidden Size",
            "Dir vs Undir",
            "GraphSAGE vs GCN",
            "Num Layers",
            "F1 Score threshold",
            "Other"
        ],
        default="Random Experiment",
    ).unsafe_ask()

    if experiment_name == "Other":
        experiment_name = questionary.text(
            "Enter a custom experiment name:"
        ).unsafe_ask()

    epochs = questionary.text(
        "Number of epochs?", default="500", validate=lambda x: x.isdigit()
    ).unsafe_ask()

    hidden_dim = questionary.text(
        "Hidden dimension?", default="24", validate=lambda x: x.isdigit()
    ).unsafe_ask()

    num_layers = questionary.text(
        "Number of layers?", default="8", validate=lambda x: x.isdigit()
    ).unsafe_ask()

    learning_rate = questionary.text(
        "Learning rate?", default="0.1", validate=lambda x: float(x) > 0
    ).unsafe_ask()

    threshold = questionary.text(
        "Threshold (0–1)?", default="0.5", validate=lambda x: 0 <= float(x) <= 1
    ).unsafe_ask()

    device = questionary.select(
        "Device?",
        choices=["cuda" if torch.cuda.is_available() else "cpu", "cpu"],
        default="cuda" if torch.cuda.is_available() else "cpu",
    ).unsafe_ask()

    # ==== flag args ====
    directed = questionary.confirm("Use directed graph?").unsafe_ask()
    graphsage = questionary.confirm("Use GraphSAGE instead of GCN?").unsafe_ask()

    if graphsage:
        agg_method = questionary.select(
            "GraphSAGE Aggregation method?",
            choices=["mean", "pool", "max", "lstm"],
            default="mean",
        ).unsafe_ask()
    save_model = questionary.confirm("Save best model?", default=False).unsafe_ask()

    # ==== build command ====
    base_command = [
        "uv",
        "run",
        "model_training.py",
        "--experiment-name",
        experiment_name,
        "--epochs",
        epochs,
        "--hidden_dim",
        hidden_dim,
        "--num_layers",
        num_layers,
        "--threshold",
        threshold,
        "--device",
        device,
    ]

    if True: 
        # always include this flag because there is a skill issue 
        # with implementing the HeteroGNN architecture
        base_command.append("--homogeneous")
    if directed:
        base_command.append("--directed")
    if graphsage:
        base_command.append("--graphsage")
    if save_model:
        base_command.append("--save-model")

    # ==== run trials ====
    for trial in range(int(trials)):
        print(f"🔁 Running trial {trial + 1}/{trials}...")
        subprocess.run(base_command, check=True)

    api_key = Settings.PUSHBULLET_API_KEY.value
    if api_key != "":
        pushbullet = Pushbullet(api_key)
        pushbullet.push_note(
            f"✅ Experiment '{experiment_name}' Completed",
            f"All {trials} trials have been completed successfully!",
        )
except KeyboardInterrupt:
    print("\n❌ Process interrupted by user. Exiting...")
    exit(0)
except Exception as e:
    print(f"\n❌ An error occurred: {e}")
    exit(1)
