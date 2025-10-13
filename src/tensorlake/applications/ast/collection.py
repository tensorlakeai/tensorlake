from typing import List

from .ast_node import ASTNodeMetadata


class CollectionMetadata(ASTNodeMetadata):
    # Ordered IDs of child nodes comprising the resolved list of values.
    nids: List[str]
