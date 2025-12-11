from dataclasses import dataclass
from typing import List

from mosaicolabs.helpers import unpack_topic_full_path


@dataclass
class QueryResponseItem:
    sequence: str
    topics: List[str]

    def __post_init__(self):
        """
        Returned topics are the full resource names, e.g. 'sequence_name/the/topic/name'.
        Retrieve the topic name only, i.e. '/the/topic/name'
        """
        tnames = []
        for top in self.topics:
            seq_topic_tuple = unpack_topic_full_path(top)
            if not seq_topic_tuple:
                raise ValueError(f"Invalid topic name in response {top}")
            _, tname = seq_topic_tuple
            tnames.append(tname)
        # reset topic names
        self.topics = tnames
