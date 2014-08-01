from collections import defaultdict
import pandas as pd
import numpy as np
from time import time
import logging

class Recommender(object):
    """
    Cold Start Recommender
    """
    def __init__(self, mongo_host=None, mongo_db_name=None, mongo_replica_set=None,
                 default_rating=3, 
                 log_level=logging.ERROR):
        if mongo_host is not None:
            assert (mongo_db_name != None)
            if mongo_replica_set is not None:
                mongo_client = MongoReplicaSetClient(mongo_host, replicaSet=mongo_replica_set)
                self.db = mongo_client[mongo_db_name]
                # reading --for producing recommendations-- could be even out of sync.
                # this can be added if most replicas are in-memory
                # self.db.read_preference = ReadPreference.SECONDARY_PREFERRED
            else:
                mongo_client = MongoClient(mongo_host)
                self.db = mongo_client[mongo_db_name]
            # If these tables do not exist, it might create problems
            if not self.db['user_ratings'].find_one():
                self.db['user_ratings'].insert({})
            if not self.db['item_ratings'].find_one():
                self.db['item_ratings'].insert({})
        else:
            self.db = None
        self.info_used = set() # Info used in addition to item_id. Only for in-memory testing, otherwise there is utils collection in the MongoDB
        self.default_rating = default_rating  # Rating inserted by default
        self._items_cooccurence = pd.DataFrame  # cooccurrence of items
        self._categories_cooccurence = {} # cooccurrence of categories
        self.cooccurence_updated = 0.0  # Time of update
        self.item_ratings = defaultdict(dict)  # matrix of ratings for a item (inmemory testing)
        self.user_ratings = defaultdict(dict)  # matrix of ratings for a user (inmemory testing)
        self.items = defaultdict(dict)  # matrix of item's information {item_id: {"Author": "AA. VV."....}
        # categories --same as above, but separated as they are not always available
        self.tot_categories_user_ratings = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))  # sum of all ratings  (inmemory testing)
        self.tot_categories_item_ratings = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))  # ditto
        self.n_categories_user_ratings = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))  # number of ratings  (inmemory testing)
        self.n_categories_item_ratings = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))  # ditto
        self.items_by_popularity = []
        self.items_by_popularity_updated = 0.0  # Time of update
        # Loggin stuff

        self.logger = logging.getLogger('CSREC')
        self.logger.setLevel(log_level)
        self.logger.debug("============ Creating a Recommender Instance ================")

    def _coll_name(self, k, typ):
        """
        e.g. user_author_ratings
        """
        return str(typ) + '_' + str(k) + '_ratings'


    def _create_cooccurence(self):
        """
        Create or update the co-occurence matrix
        :return:
        """
        df_tot_cat_item = {}
        if not self.db:
            # Items' vectors
            df_item = pd.DataFrame(self.item_ratings).fillna(0).astype(int)
            # Categories' vectors
            if len(self.info_used) > 0:
                info_used = self.info_used
                for i in info_used:
                    df_tot_cat_item[i] = pd.DataFrame(self.tot_categories_item_ratings[i]).fillna(0).astype(int)
        else:  # read if from Mongodb
            # here we *must* use user_ratings, so indexes are the users, columns the items...
            df_item = pd.DataFrame.from_records(list(self.db['user_ratings'].find())).set_index('_id').fillna(0).astype(int)
            info_used = self.db['utils'].find_one({"_id": 1}, {'info_used': 1, "_id": 0}).get('info_used', [])
            self.logger.info("[_create_cooccurence] Found info_used in db.utils: %s", info_used)
            if len(info_used) > 0:
                for i in info_used:
                    user_coll_name = self._coll_name(i, 'user')
                    if self.db['tot_' + user_coll_name].find_one():
                        df_tot_cat_item[i] = pd.DataFrame.from_records(list(self.db['tot_' + user_coll_name].find())).set_index('_id').fillna(0).astype(int)
        df_item = (df_item / df_item).replace(np.inf, 0)  # normalize to one to build the co-occurence
        self._items_cooccurence = df_item.T.dot(df_item)
        if len(info_used) > 0:
            for i in info_used:
                if type(df_tot_cat_item.get(i)) == pd.DataFrame:
                    df_tot_cat_item[i] = (df_tot_cat_item[i] / df_tot_cat_item[i]).replace(np.inf, 0)
                    self._categories_cooccurence[i] = df_tot_cat_item[i].T.dot(df_tot_cat_item[i])
        self.cooccurence_updated = time()


    def _sync_user_item_ratings(self):
        """
        It might happen that the user_ratings and the item_ratings
        are not aligned. It shouldn't, but with users can be profiled,
        then reconciled with session_id etc, it happened...
        THIS SHOULD BE LOGGED AS ERROR!
        :return:
        """
        #Doing that only for the mongodb case..
        self.logger.warning("[_sync_user_item_ratings] Syncronyzing item_ratings with user_ratings data")
        if self.db:
            self.db['item_ratings'].drop()
            for user_date in self.db['user_ratings'].find():  # don't put {_id: 0!}
                for item_id, rating in user_date.iteritems():
                    if item_id != "_id":
                        self.db['item_ratings'].update(
                            {"_id": item_id},
                            {"$set": {user_date["_id"]: rating}},
                            upsert=True
                        )
            for coll in self.db.collection_names():
                if '_item_' in coll:
                    user_coll = coll.replace('_item_', '_user_')
                    self.db[coll].drop()
                    for user_date in self.db[user_coll].find():  # don't put {_id: 0!}
                        for info_id, value in user_date.iteritems():
                            if info_id != "_id":
                                self.db[coll].update(
                                    {"_id": info_id},
                                    {"$set": {user_date["_id"]: value}},
                                    upsert=True
                                )



    def insert_item(self, item, _id="_id"):
        """
        Insert the whole document either in self.items or in db.items
        :param item: {_id: item_id, cat1: ...} or {item_id_key: item_id, cat1: ....}
        :return: None
        """
        if not self.db:
            self.items[item[_id]] = item
        else:
            for k, v in item.items():
                if k is not "_id":
                    self.db["items"].update({"_id": item[_id]},
                                            {"$set": {k: v}},
                                            upsert=True)


    def reconcile_ids(self, id_old, id_new):
        """
        Create id_new if not there, add data of id_old into id_new.
        Compute the co-occurence matrix.
        NB id_old is removed!
        :param id_new:
        :param id_old:
        :return: None
        """
        id_new = str(id_new).replace(".", "")
        id_old = str(id_old).replace(".", "")
        if not self.db:
            # user-item
            for key, value in self.user_ratings[id_old].items():
                self.user_ratings[id_new][key] = self.user_ratings[id_old][key]
            self.user_ratings.pop(id_old)

            for k, v in self.item_ratings.items():
                if v.has_key(id_old):
                    v[id_new] = v.pop(id_old)
            # user-categories
            if len(self.info_used) > 0:
                for i in self.info_used:
                    for key, value in self.tot_categories_user_ratings[i][id_old].items():
                        self.tot_categories_user_ratings[i][id_new][key] = self.tot_categories_user_ratings[i][id_old][key]
                    self.tot_categories_user_ratings[i].pop(id_old)

                    for k, v in self.tot_categories_item_ratings[i].items():
                        if v.has_key(id_old):
                            v[id_new] = v.pop(id_old)

                    for key, value in self.n_categories_user_ratings[i][id_old].items():
                        self.n_categories_user_ratings[i][id_new][key] = self.n_categories_user_ratings[i][id_old][key]
                    self.n_categories_user_ratings[i].pop(id_old)

                    for k, v in self.n_categories_item_ratings[i].items():
                        if v.has_key(id_old):
                            v[id_new] = v.pop(id_old)
        else:  # work on mongo...
            user_ratings = self.db['user_ratings'].find_one({"_id": id_old}, {"_id": 0})
            if user_ratings:
                for key, value in user_ratings.items():
                    self.db['user_ratings'].update(
                        {"_id": id_new},
                        {"$set": {key: value}},
                        upsert=True
                    )
                    self.logger.info("[reconcile_ids] %s : %s for new user_id %s", key, value, id_new)
                self.logger.info("[reconcile_ids] Removing %s from user_rating", id_old)
                self.db['user_ratings'].remove({"_id": id_old})

            self.db['item_ratings'].update(
                {id_old: {"$exists": True}},
                {"$rename": {id_old: id_new}},
                multi=True
            )

            info_used = self.db['utils'].find_one({"_id": 1}, {'info_used': 1, "_id": 0}).get('info_used', [])
            self.logger.debug("[reconcile_ids] info_used %s", info_used)
            if len(info_used) > 0:
                for k in info_used:
                    users_coll_name = self._coll_name(k, 'user')
                    items_coll_name = self._coll_name(k, 'item')
                    # tot and n user ratings....
                    tot_user_ratings = self.db['tot_' + users_coll_name].find_one({"_id": id_old}, {"_id": 0})
                    if tot_user_ratings:
                        for key, value in tot_user_ratings.items():
                            self.db['tot_' + users_coll_name].update(
                                {"_id": id_new},
                                {"$set": {key: value}},
                                upsert=True
                            )
                        self.db['tot_' + users_coll_name].remove({"_id": id_old})
                    n_user_ratings = self.db['n_' + users_coll_name].find_one({"_id": id_old}, {"_id": 0})
                    if n_user_ratings:
                        for key, value in n_user_ratings.items():
                            self.db['n_' + users_coll_name].update(
                                {"_id": id_new},
                                {"$set": {key: value}},
                                upsert=True
                            )
                        self.db['n_' + users_coll_name].remove({"_id": id_old})

                    self.db['tot_' + items_coll_name].update(
                        {id_old: {"$exists": True}},
                        {"$rename": {"id_new": "id_old"}},
                        multi=True
                    )

                    self.db['n_' + items_coll_name].update(
                        {id_old: {"$exists": True}},
                        {"$rename": {"id_new": "id_old"}},
                        multi=True
                    )
        self._create_cooccurence()


    def compute_items_by_popularity(self, max_items=10, fast=False):
        """
        As per name, get self.
        :return: list of popular items, 0=most popular
        """
        if fast and (time() - self.most_popular_items_updated) < 3600:
            return self.items_by_popularity

        if not self.db:
            df_item = pd.DataFrame(self.item_ratings).fillna(0).astype(int).sum()
        else:  # Mongodb
            df_item = pd.DataFrame.from_records(list(self.db['user_ratings'].find())).set_index('_id').sum()

        df_item.sort(ascending=False)
        pop_items = list(df_item.index)
        if len(pop_items) >= max_items:
            self.items_by_popularity = pop_items
        else:
            all_items = set([ d["_id"] for d in self.db['items'].find({}, {"_id": 1})])
            self.items_by_popularity = pop_items + list( all_items - set(pop_items) )

    def get_similar_item(self, item_id, user_id=None, algorithm='simple'):
        """
        Simple: return the row of the co-occurence matrix ordered by score or,
        if user_id is not None, multiplied times the user_id rating
        (not transposed!) so to weigh the similarity score with the
        rating of the user
        :param item_id: Id of the item
        :param user_id: Id of the user
        :param algorithm: keep it simple...
        :return:
        """
        user_id = str(user_id).replace('.', '')
        pass


    def remove_rating(self, user_id, item_id):
        """
        Remove ratings from item and user. This cannot be undone for categories
        (only thing we could do is subtracting the average value from sum and n-1)
        :param user_id:
        :param item_id:
        :return:
        """
        user_id = str(user_id).replace('.', '')
        if not self.db:
            self.user_ratings[user_id].pop(item_id, None)
            self.item_ratings[item_id].pop(user_id, None)
            self.items[item_id] = {}  # just insert the bare id. quite useless because it is a defaultdict, but in case .keys() we can count the # of items
        else:
            self.db['user_ratings'].remove(
                {"_id": user_id, item_id: {"$exists": True}})

            self.db['item_ratings'].remove(
                {"_id": item_id, user_id: {"$exists": True}})


    def insert_rating(self, user_id, item_id, rating=3, item_info=[], only_info=False):
        """
        item is treated as item_id if it is not a dict, otherwise we look
        for a key called item_id_key if it is a dict.

        item_info can be any further information given with the dict item.
        e.g. author, category etc

        NB NO DOTS IN user_id, or they will taken away. Fields in mongodb cannot have dots..

        If only_info==True, only the item_info's are put in the co-occurence, not item_id.
         This is necessary when we have for instance a "segmentation page" where we propose
         well known items to get to know the user. If s/he select "Harry Potter" we only want
         to retrieve the info that s/he likes JK Rowling, narrative, magic etc

        :param user_id: id of user. NO DOTS, or they will taken away. Fields in mongodb cannot have dots.
        :param item: is either id or a dict with item_id_key
        :param rating: float parseable
        :param item_info: any info given with dict(item), e.g. ['author', 'category', 'subcategory']
        :param only_info: not used yet
        :return: [recommended item_id_values]
        """
        # If only_info==True, only the item_info's are put in the co-occurence, not item_id.
        # This is necessary when we have for instance a "segmentation page" where we propose
        # well known items to get to know the user. If s/he select "Harry Potter" we only want
        # to retrieve the info that s/he likes JK Rowling, narrative, magic etc

        # Now fill the dicts or the Mongodb collections if available
        user_id = str(user_id).replace('.', '')
        if not self.db:   # fill dicts and work only in memory
            if self.items.get(item_id):
                item = self.items.get(item_id)
                # Do categories only if the item is stored
                if len(item_info) > 0:
                    for k,v in item.items():
                        if k in item_info:
                            self.info_used.add(k)
                            # we cannot set the rating, because we want to keep the info
                            # that a user has read N books of, say, the same author,
                            # category etc.
                            # We could sum all the ratings and count the a result as "big rating".
                            # Reading N books of author A and rating them 5 would be the same as reading
                            # 5*N books of author B and rating them 1.
                            # Still:
                            # 1) we don't want ratings for category to skyrocket, so we have to take the average
                            # 2) if a user changes their idea on rating a book, it should not add up. Average
                            #   is not perfect, but close enough. Take total number of ratings and total rating
                            self.tot_categories_user_ratings[k][user_id][v] += int(rating)
                            self.n_categories_user_ratings[k][user_id][v] += 1
                            # for the co-occurence matrix is not necessary to do the same for item, but better do it
                            # in case we want to compute similarities etc using categories
                            self.tot_categories_item_ratings[k][v][user_id] += int(rating)
                            self.n_categories_item_ratings[k][v][user_id] += 1
            else:
                self.insert_item({"_id": item_id})
            # Do item always, at least is for categories profiling
            if not only_info:
                self.user_ratings[user_id][item_id] = float(rating)
                self.item_ratings[item_id][user_id] = float(rating)
        # MongoDB
        else:
            # If the item is not stored, we don't have its categories
            # Therefore do categories only if the item is found stored
            if self.db['items'].find_one({"_id": item_id}):
                item = self.db['items'].find_one({"_id": item_id})
                if len(item_info) > 0:
                    self.logger.debug('[insert_rating] Looking for the following info: %s', item_info)
                    for k, v in item.items():
                        if k in item_info and v is not None:  # sometimes the value IS None
                            self.logger.debug("[insert_rating] Adding %sto info_used and create relative collections", k)

                            users_coll_name = self._coll_name(k, 'user')
                            items_coll_name = self._coll_name(k, 'item')

                            self.db['utils'].update({"_id": 1},
                                                    {"$addToSet": {'info_used': k}},
                                                    upsert=True)
                            # see comments above
                            self.db['tot_' + users_coll_name].update({'_id': user_id},
                                                                     {'$inc': {v: float(rating)}},
                                                                      upsert=True)
                            self.db['n_' + users_coll_name].update({'_id': user_id},
                                           {'$inc': {v: 1}},
                                            upsert=True)
                            self.db['tot_' + items_coll_name].update({'_id': v},
                                           {'$inc': {user_id: float(rating)}},
                                            upsert=True)
                            self.db['n_' + items_coll_name].update({'_id': v},
                                           {'$inc': {user_id: 1}},
                                            upsert=True)
                            self.db['items'].update(
                                {"_id": item_id},
                                {"$set": {k: v}},
                                upsert=True
                            )
            else:
                self.insert_item({"_id": item_id})  # Obviously there won't be categories...

            if not only_info:
                self.db['user_ratings'].update(
                    {"_id": user_id},
                    {"$set": {item_id: float(rating)}},
                    upsert=True
                )
                self.db['item_ratings'].update(
                    {"_id": item_id},
                    {"$set": {user_id: float(rating)}},
                    upsert=True
                )


    def get_recommendations(self, user_id, max_recs=50, fast=False, algorithm='item_based'):
        """
        algorithm item_based:
            - Compute recommendation to user using item co-occurence matrix (if the user
            rated any item...)
            - If there are less than max_recs recommendations, the remaining
            items are given according to popularity. Scores for the popular ones
            are given as
                            score[last recommended]*index[last recommended]/n
            where n is the position in the list.
            - Recommended items above receive a further score according to categories
        :param user_id: the user id as in the mongo collection 'users'
        :param max_recs: number of recommended items to be returned
        :param fast: Compute the co-occurence matrix only if it is one hour old or
                     if matrix and user vector have different dimension
        :return: list of recommended items
        """
        user_id = str(user_id).replace('.', '')
        df_tot_cat_user = {}
        df_n_cat_user = {}
        rec = pd.Series()
        item_based = False  # has user rated some items?
        info_based = []  # user has rated the category (e.g. the category "author" etc)
        if not self.db:
            if self.user_ratings.get(user_id):  # compute item-based rec only if user has rated smt
                item_based = True
                #Just take user_id for the user vector
                df_user = pd.DataFrame(self.user_ratings).fillna(0).astype(int)[[user_id]]
            info_used = self.info_used
            if len(info_used) > 0:
                for i in info_used:
                    if self.tot_categories_user_ratings[i].get(user_id):
                        info_based.append(i)
                        df_tot_cat_user[i] = pd.DataFrame(self.tot_categories_user_ratings[i]).fillna(0).astype(int)[[user_id]]
                        df_n_cat_user[i] = pd.DataFrame(self.n_categories_user_ratings[i]).fillna(0).astype(int)[[user_id]]
        else:  # Mongodb
            # Did the user rate anything?
            if self.db['user_ratings'].find_one({"_id": user_id}):
                item_based = True
                try:
                    df_user = pd.DataFrame.from_records(list(self.db['item_ratings'].find())).set_index('_id').fillna(0).astype(int)[[user_id]]
                except:
                    self.logger.warning("[get_recommendations. item and user ratings colls not synced")
                    self._sync_user_item_ratings()
                    df_user = pd.DataFrame.from_records(list(self.db['item_ratings'].find())).set_index('_id').fillna(0).astype(int)[[user_id]]
            info_used = self.db['utils'].find_one({"_id": 1}, {'info_used': 1, "_id": 0}).get('info_used', [])
            self.logger.debug("[get_recommendations] info_used: %s", info_used)
            if len(info_used) > 0:
                for i in info_used:
                    item_coll_name = self._coll_name(i, 'item')
                    user_coll_name = self._coll_name(i, 'user')
                    if self.db['tot_' + user_coll_name].find_one({"_id": user_id}):
                        info_based.append(i)
                        df_tot_cat_user[i] = pd.DataFrame.from_records(list(self.db['tot_' + item_coll_name].find())).set_index('_id').fillna(0).astype(int)[[user_id]]
                        df_n_cat_user[i] = pd.DataFrame.from_records(list(self.db['n_' + item_coll_name].find())).set_index('_id').fillna(0).astype(int)[[user_id]]
                        self.logger.debug("[get_recommendations]. df_tot_cat_user[%s]:%s\n", i, df_tot_cat_user[i])
        if item_based:
            try:
                # this might fail for fast in case a user has rated an item
                # but the co-occurence matrix has not been updated
                # therefore the matrix and the user-vector have different
                # dimension
                if not fast or (time() - self.cooccurence_updated > 3600):
                    self._create_cooccurence()
                rec = self._items_cooccurence.T.dot(df_user[user_id])
                self.logger.debug("[get_recommendations] Rec: %s", rec)
            except:
                self.logger.debug("[get_recommendations] 1st rec production failed, calling _create_cooccurence.")
                try:
                    self._create_cooccurence()
                    rec = self._items_cooccurence.T.dot(df_user[user_id])
                    self.logger.debug("[get_recommendations] Rec: %s", rec)
                except:
                    self.logger.warning("[get_recommendations] user_ and item_ratings seem not synced")
                    self._sync_user_item_ratings()
                    self._create_cooccurence()
                    rec = self._items_cooccurence.T.dot(df_user[user_id])
                    self.logger.debug("[get_recommendations] Rec: %s", rec)

            # Add to rec items according to popularity
            rec.sort(ascending=False)

            if len(rec) < max_recs:
                self.compute_items_by_popularity(fast=fast)
                for v in self.items_by_popularity:
                    if len(rec) == max_recs:
                        break
                    elif v not in rec.index:
                        n = len(rec)
                        rec.set_value(v, rec.values[n - 1]*n/(n+1.))  # supposing score goes down according to Zipf distribution
        else:
            self.compute_items_by_popularity(fast=fast)
            for i, v in enumerate(self.items_by_popularity):
                if len(rec) == max_recs:
                    break
                rec.set_value(v, self.max_rating / (i+1.))  # As comment above, starting from max_rating
        self.logger.debug("[get_recommendations] Rec after item_based or not: %s", rec)

        # Now, the worse case we have rec=popular with score starting from max_rating
        # and going down as 1/i (this is item_based == False)

        global_rec = rec.copy()
        if len(info_used) > 0:
            cat_rec = {}
            for cat in info_based:
                user_vec = df_tot_cat_user[cat][user_id] / df_n_cat_user[cat][user_id].replace(0, 1)
                # print "DEBUG get_recommendations. user_vec:\n", user_vec
                try:
                    cat_rec[cat] = self._categories_cooccurence[cat].T.dot(user_vec)
                    cat_rec[cat].sort(ascending=False)
                    #self.logger.debug("[get_recommendations] cat_rec (try):\n %s", cat_rec)
                except:
                    self._create_cooccurence()
                    cat_rec[cat] = self._categories_cooccurence[cat].T.dot(user_vec)
                    cat_rec[cat].sort(ascending=False)
                    #self.logger.debug("[get_recommendations] cat_rec (except):\n %s", cat_rec)
                for k, v in rec.iteritems():
                    #self.logger.debug("[get_recommendations] rec_item_id: %s", k)
                    try:
                        if not self.db:
                            item_info_value = self.items[k][cat]
                        else:
                            item_info_value = self.db['items'].find_one({"_id": k}, {"_id": 0, cat: 1}).get(cat)
                        #self.logger.debug("DEBUG get_recommendations. item value for %s: %s", cat, item_info_value)
                        # In case the info value is not in cat_rec (as it can obviously happen
                        # because a rec'd item coming from most popular can have the value of
                        # an info (author etc) which is not in the rec'd info
                        if item_info_value:
                            global_rec[k] = v + cat_rec.get(cat, []).get(item_info_value, 0)
                    except Exception, e:
                        self.logger.error("item %s, category %s", k, cat)
                        logging.exception(e)
        global_rec.sort(ascending=False)
        self.logger.debug("[get_recommendations] global_rec:\n %s", global_rec)

        if item_based:
            # If the user has rated all items, return an empty list
            rated = df_user[user_id] != 0
            self.logger.debug("Rated: %s", rated)
            return [i for i in global_rec.index if not rated.get(i, False)][:max_recs]
        else:
            return list(global_rec.index)[:max_recs]


    def get_user_info(self, user_id):
        """
        Return user's rated items: {'item1': 3, 'item3': 1...}
        :param user_id:
        :return:
        """
        if self.db:
            r = self.db['user_ratings'].find_one({"_id": user_id}, {"_id": 0})
            return r if r else {}
        else:
            return self.user_ratings[user_id]
