from abc import ABC, abstractmethod
from typing import TypeVar

from sqllineage.utils.entities import EdgeTuple

T = TypeVar("T")


class GraphOperator(ABC):
    """
    Base class used to operator the graph structure that holder leverages to store lineage information
    """

    @abstractmethod
    def add_vertex_if_not_exist(self, vertex: T, **props) -> None:
        """
        when vertex already exists, props will be updated
        """
        raise NotImplementedError

    @abstractmethod
    def retrieve_vertices_by_props(self, **props) -> list[T]:
        """
        when props is empty, return all vertices
        """
        raise NotImplementedError

    @abstractmethod
    def retrieve_source_vertices(self) -> list[T]:
        """
        source vertices are defined as vertices with no incoming edges and at least one outgoing edge
        """
        raise NotImplementedError

    @abstractmethod
    def retrieve_target_vertices(self) -> list[T]:
        """
        target vertices are defined as vertices with no outgoing edges and at least one incoming edge
        """
        raise NotImplementedError

    @abstractmethod
    def retrieve_selfloop_vertices(self) -> list[T]:
        """
        self-loop vertices are defined as vertices with edges pointing to themselves
        """
        raise NotImplementedError

    @abstractmethod
    def update_vertices(self, *vertices: T, **props) -> None:
        raise NotImplementedError

    @abstractmethod
    def drop_vertices(self, *vertices: T) -> None:
        """
        drop vertices from the graph if exists
        """
        raise NotImplementedError

    @abstractmethod
    def add_edge_if_not_exist(
        self, src_vertex: T, tgt_vertex: T, label: str, **props
    ) -> None:
        """
        when vertex already exists, props will be updated
        """
        raise NotImplementedError

    @abstractmethod
    def retrieve_edges_by_label(self, label: str) -> list[EdgeTuple]:
        raise NotImplementedError

    @abstractmethod
    def retrieve_edges_by_vertex(
        self, vertex: T, direction: str, label: str | None = None
    ) -> list[EdgeTuple]:
        raise NotImplementedError

    @abstractmethod
    def drop_edge(self, src_vertex: T, tgt_vertex: T) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_sub_graph(self, *vertices: T) -> "GraphOperator":
        raise NotImplementedError

    @abstractmethod
    def merge(self, other: "GraphOperator") -> None:
        raise NotImplementedError

    @abstractmethod
    def list_lineage_paths(self, src_vertex: T, tgt_vertex: T) -> list[list[T]]:
        """
        list all lineage paths (acyclic) in the graph from src_vertex to tgt_vertex.
        """
        raise NotImplementedError

    @abstractmethod
    def to_cytoscape(
        self, compound: bool = False
    ) -> list[dict[str, dict[str, object]]]:
        """
        Convert graph to cytoscape format to be used in visualization.

        compound nodes is used to group nodes together to their parent.
        See https://js.cytoscape.org/#notation/compound-nodes for reference.
        """
        raise NotImplementedError
