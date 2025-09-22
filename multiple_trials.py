import subprocess

import questionary
import torch

try:
    # ==== typed args ====
    trials = questionary.text(
        "Number of trials?", default="50", validate=lambda x: x.isdigit()
    ).unsafe_ask()

    experiment_name = questionary.text(
        "Experiment name?", default="Random Experiment"
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

    threshold = questionary.text(
        "Threshold (0–1)?", default="0.5", validate=lambda x: 0 <= float(x) <= 1
    ).unsafe_ask()

    device = questionary.select(
        "Device?",
        choices=["cuda" if torch.cuda.is_available() else "cpu", "cpu"],
        default="cuda" if torch.cuda.is_available() else "cpu",
    ).unsafe_ask()

    # ==== flag args ====
    homogeneous = questionary.confirm("Use homogeneous graph?").unsafe_ask()
    directed = questionary.confirm("Use directed graph?").unsafe_ask()
    save_model = questionary.confirm("Save best model?", default=False).unsafe_ask()
    graphsage = questionary.confirm("Use GraphSAGE instead of GCN?").unsafe_ask()

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

    if homogeneous:
        base_command.append("--homogeneous")
    if directed:
        base_command.append("--directed")
    if save_model:
        base_command.append("--save-model")
    if graphsage:
        base_command.append("--graphsage")

    # ==== run trials ====
    for trial in range(int(trials)):
        print(f"🔁 Running trial {trial + 1}/{trials}...")
        subprocess.run(base_command, check=True)
except KeyboardInterrupt:
    print("\n❌ Process interrupted by user. Exiting...")
    exit(0)
