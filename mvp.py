import argparse
from asyncio import run
from pprint import pprint

import torch
from deepsnap.dataset import GraphDataset

from src.architectures import HomoGNN
from src.config import PROJECT_ROOT
from src.data import DatasetLoader, GraphBuilder, InferenceResults, Preprocessor
from src.db import PBWarehouse
from src.models import Features, Graph, User
from src.scraper import TweetyScraper


async def main():
    parser = argparse.ArgumentParser(description="Run the MVP script.")
    parser.add_argument(
        "--user", type=int, help="User ID to get similar nodes for.", default=None
    )
    parser.add_argument(
        "--username", type=str, help="Username to get user info for.", default=None
    )
    args = parser.parse_args().__dict__

    if not args["username"] and not args["user"]:
        raise ValueError("Either User ID or Username must be provided.")

    model_path = PROJECT_ROOT / "best_model.pth"

    pb = PBWarehouse()
    scraper = TweetyScraper(previous_session=True)

    model = HomoGNN(input_size=5, hidden_size=32)
    model.load_state_dict(torch.load(model_path))
    dataset_loader = DatasetLoader(pb)
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

    graph: Graph = gb.create_graph()

    if pb.does_user_exist(user_id=args["user"], username=args["username"]):
        if args["user"] is not None:
            user_record = pb.get_user_by_id(args["user"])
        else:
            user_record = pb.get_user_by_username(args["username"])
        user = User(**user_record.__dict__)

    else:
        print(
            f"User {args['user']} or {args['username']} does not exist in the database. Fetching user info..."
        )
        user: User = await scraper.get_user_info(args["user"], args["username"])
        pb.ingest_user(user)
        print(f"User {user.user_id} fetched successfully.")
    
    print("Adding user to the graph...")
    user_vector = gb.get_features(user.model_dump(), "user")
    graph.add_node(
        user.user_id,
        node_label=3,
        node_feature=user_vector,
        node_type="user",
    )

    dataset = GraphDataset([graph], task="link_pred", edge_train_mode="disjoint")

    model.eval()
    with torch.no_grad():
        node_embeddings, edge_label_index = model(dataset[0])
        results = InferenceResults(
            graph=graph,
            node_embeddings=node_embeddings,
            edge_label_index=edge_label_index,
        )

        topk_results = results.get_top_k_similar_nodes_linked_to_user(
            user_id=user.user_id, descending=True, label=0
        )

        print(f"Top similar nodes linked to user {user.user_id}:")
        pprint(topk_results)


if __name__ == "__main__":
    run(main())
