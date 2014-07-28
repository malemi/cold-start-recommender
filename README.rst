======================
Cold Start Recommender
======================

Cold Start Recommender (CSRec) is an easy to set up, fast, greedy
recommender.

We developed because we could not find any recommender we could
use for providing recommendations to a project with the following
characteristics:

* Cold. No previous data on Items nor Users available. This means we could
not cluster Users in any way (sex, age etc), nor use any content-related
information to start with content-based recommendations

* Fast. Any information on Users and Item should be stored and used immediately. A rating by any User should improve recommendations for such User, but also for other Users. This means no batch computations.

* Ready to use. It will provide a RESTful API to POST information and GET recommendations.

    from csrec.Recommender import Recommender

    engine = Recommender()

    # Insert Item with it properties (e.g. author, category...)

    engine.insert_item({'_id': 'an_item', 'author': 'The Author'})

    # Insert rating, indicating wich property of the Item should be used for producing recs

    engine.insert_rating(user_id='a_user;, item_id='an_item', rating=4, item_info=['author'])

    # Insert rating, indicating that only the property should be used for recs (e.g. initial users' profiling)

    engine.insert_rating(user_id='another_user', item_id='an_item', rating=3, item_info=['author'], only_info=True)


Features
========

Persistence
-----------

You can use CSRec in-memory, but, of course, only for testing. CSrec
at the moment can easily use MongoDB for persistence or, even better a
MongoDB replica set.

Why using a replica set? Because you can have the primary DB in
memory, and two other secondaries on disk. If the primary goes down,
you still can use CSRec at lower performances, but without any data
loss.

Examples:

	engine = Recommender()  # Start in-memory recommender for testing
	engine = Recommender(mongo_host='localhost', mongo_db_name='my_cold_rec')  # ...with MongoDB, collections are created automatically
	engine = Recommender(mongo_host='localhost', mongo_db_name='my_cold_rec', mongo_replica_set='recommender_replica')  # as above, with replica
	

The Cold Start Problem
----------------------

The Cold Start Problem originates from the fact that collaborative
filtering recommenders needs data to build recommendations. Typically,
if Users who liked item 'A' also liked item 'B', the recommender would
recommend 'B' to a user who just liked 'A'. But if you have no
previous rating by any User, you cannot make any recommendation.

CSRec tackles the issue in various ways:

1. It allows profiling with well-known Items without biasing the
results. For instance, if a call to insert_rating is done in this way:

   engine.insert_rating(user_id='another_user', item_id='an_item', rating=3, item_info=['author'], only_info=True)

CSRec will only register that 'another_user' likes a certain author,
but not that s/he might like 'an_item'. This is of fundamental
importance when profiling Users with a "profiling page" provided soon
after log-in. If you ask Users if they like "Harry Potter" instead of
"The Better Angel of Our Nature", and many chose Harry Potter, you
don't want to make the Item "Harry Potter" even more popular than what
alread is. You might only want to record that the user likes books for
children sold as literature for adults.

CSRec does that because, unless you are Amazon or a similar brand, the
co-occurence matrix is often too sparse to build any decent
recommendation. In this way you start building multiple, denser,
co-occurence matrices and use them from the beginning.

2. Any information is used. You decide which information you should
record about a User rating an Item. This is similar to the previous
point, but you also register the item_id.

3. Any information is used *immediately*. The co-occurence matrix is
updated as soon as a rating is inserted.

4. Anonimous Users (e.g. random visitors of a website before sign up)
might give information, which would be recorded through their session
ID. After sign up/ sign in the information can be reconciled
--information relative to the session ID is moved into the
correspondent user ID entry.

Mix Recommended with Popular Items
----------------------------------

What about users who would only receive a couple of recommended items?
No problem! We'll fill the list with most popular items who were not
recommended (nor rated by such user).

Algorithms
----------

At the moment CSRec only provides purely item-based recommendations
(co-occurence matrix dot the User's ratings array). In this way we can
provide recommendations in less than 200msec for a matrix of about
10,000 items.

 
