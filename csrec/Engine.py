__author__ = 'mario'

from abc import ABCMeta, abstractmethod

class EngineBase():
    """
    Engine behind the recommender.
    We approach the recommendation problem as bipartite graph problem, where
    the two disjointed sets are called *info* and *entities*.

    Info-nodes provide information about the entity, to which they are related.

    For instance, if entities are book-shop's customers, info can be books, but
    also authors, publishers etc.

    What the recommender actually needs is an engine which accept info/entities'
    values and returns new relationships. It does that in two ways:

        * Find new possible relationships:
            1. info -> recommended entity
            2. entity -> recommended info

        * Collapse one dimension of the graph
            1. entity -> similar entity
            2. info -> similar info

    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def add_relationship(self, info_id, entity_id, weight=1.):
        """
        Add an existing relationship between info_id and entity_id
        (e.g. user rated 5 an item)

        :param info_id: id
        :param entity_id: id
        :param weight: a number
        :return:
        """
        pass


    @abstractmethod
    def reduce_graph(self, dimension):
        """
        Using the favourite algorithm, reduce the graph (e.g. cooccurrence of items
        weighted with log-likelihood ratio)

        :param dimension in ['info', 'entity']
        :return: nothing, but updates/creates the similarity matrix (e.g. cooccurrence, llr etc)
        """
        pass


    @abstractmethod
    def collapse_nodes(self, dimension, id_old, id_new):
        """
        All relationship of id_old are moved to id_new.

            . If id_old does not exist does nothing (warning)
            . If id_new does not exist just rename

        :param dimension:  in ['info', 'entity']
        :param id_old
        :param id_new
        :return: nothing, update the similarity matrix
        """

