from asyncio import run
from typing import List, Optional, Union

import torch
from deepsnap.dataset import GraphDataset

from src.architectures import HomoGNN
from src.data import InferenceResults
from src.db import PBWarehouse
from src.models import DiGraph, Graph, Tweet, TweetLinkProbability, User
from src.scraper import TweetyScraper


class InferenceEngine:
    def __init__(
        self,
        model: HomoGNN,
        pb: PBWarehouse,
        scraper: TweetyScraper,
        graph: Union[DiGraph, Graph],
    ):
        self.model = model
        self.pb = pb
        self.scraper = scraper
        self.graph = graph

    def __call__(
        self,
        username: str,
        user_id: Optional[str] = None,
        top_k: int = 10,
        strict_matching: bool = False,
        descending: bool = True,
    ) -> List[TweetLinkProbability]:
        user_exists = self.pb.does_user_exist(
            username=username, user_id=user_id, strict=strict_matching
        )

        if user_exists:
            if user_id is not None:
                user_record = self.pb.get_user_by_id(user_id)
            else:
                user_record = self.pb.get_user_by_username(
                    username, strict=strict_matching
                )

            user = User(**user_record.__dict__)

        else:
            user = run(
                self.scraper.get_user_info(int(user_id) if user_id else None, username)
            )

            if user is None:
                return None
            self.pb.ingest_user(user)

        return self.inference(user, descending=descending, top_k=top_k)

    def inference(
        self, user: User, descending: bool = True, top_k: int = 10
    ) -> List[TweetLinkProbability]:
        user_vector = self.gb.get_features(user.model_dump(), "user")
        DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")

        self.graph.add_node(
            int(user.user_id),
            node_label=3,
            node_feature=user_vector,
            node_type="user",
        )

        dataset = GraphDataset(
            [self.graph], task="link_pred", edge_train_mode="disjoint"
        )

        self.model.eval()
        with torch.no_grad():
            node_embeddings, edge_label_index = self.model(dataset[0].to(DEVICE))
            results = InferenceResults(
                graph=self.graph,
                node_embeddings=node_embeddings.cpu(),
                edge_label_index=edge_label_index.cpu(),
            )

            topk_results = results.get_top_k_similar_nodes_linked_to_user(
                user_id=int(user.user_id), descending=descending, label=0, k=top_k
            )

            return_data = []
            for node_id, probability in topk_results:
                tweet = Tweet(**self.pb.get_tweet_by_id(node_id).__dict__)
                tweet.user_id = User(**self.pb.get_user_by_id(tweet.user_id).__dict__)

                return_data.append(TweetLinkProbability(tweet=tweet, score=probability))
            return return_data
