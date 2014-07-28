======================
Cold Start Recommender
======================

Cold Start Recommender (CSR) is an easy to set up, fast, greedy
recommender.

We developed because we could not find any recommender we could
use for providing recommendations to a project with the following
characteristics:

* Cold. No previous data on Items nor Users available. This means we could
not cluster Users in any way (sex, age etc), nor use any content-related
information to start with content-based recommendations

* Fast. Any information on Users and Item should be stored and used immediately. 
A rating by any User should improve recommendations for such User, but also for other Users.
This means no batch computations.

* Ready to use. It should provide a RESTful API to POST information and GET recommendations.

    #!/usr/bin/env python

    from coldstartrec import ColdStartRecommender
    import random
    import math

    engine = ColdStartRecommender()

    # Insert Item with it properties (e.g. author, category...)
    engine.insert_item({'_id': 'an_item', 'author': 'The Author'})
    # Insert rating, indicating wich property of the Item should be used for producing recs
    engine.insert_rating(user_id='a_user;, item_id='an_item', rating=4, item_info=['author'])
    # Insert rating, indicating that only the property should be used for recs (e.g. initial users' profiling)
    engine.insert_rating(user_id='another_user', item_id='an_item', rating=3, item_info=['author'], only_info=True)


