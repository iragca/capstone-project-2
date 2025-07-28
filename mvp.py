from pprint import pprint

import torch
from deepsnap.dataset import GraphDataset

from src.architectures import HomoGNN
from src.config import PROJECT_ROOT
from src.data import DatasetLoader, GraphBuilder, InferenceResults, Preprocessor
from src.db import PBWarehouse
from src.models import Features

model_path = PROJECT_ROOT / "best_model.pth"

model = HomoGNN(input_size=5, hidden_size=32)
model.load_state_dict(torch.load(model_path))
dataset_loader = DatasetLoader(PBWarehouse())
data = dataset_loader.load_dataset()
preprocessor = Preprocessor(data=data)
data = preprocessor.preprocess()

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

gb = GraphBuilder(data=data, node_features=node_features)

graph = gb.create_graph()

dataset = GraphDataset([graph], task="link_pred", edge_train_mode="disjoint")

model.eval()
with torch.no_grad():
    node_embeddings, edge_label_index = model(dataset[0])
    results = InferenceResults(
        graph=graph, node_embeddings=node_embeddings, edge_label_index=edge_label_index
    )

    random_user = graph.get_random_user()
    topk_results = results.get_top_k_similar_nodes_linked_to_user(
        user_id=random_user, descending=True, label=0
    )

    print(f"Top similar nodes linked to user {random_user}:")
    pprint(topk_results)
