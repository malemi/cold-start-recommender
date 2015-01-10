from locust import HttpLocust, Locust, TaskSet, task
import random

random.seed(0)

class RecommTaskSet(TaskSet):
    min_wait = 2000
    max_wait = 5000

    @task(10)
    def insert_items(self):
        """
        insert item and rating
        :return:
        """
        item_num = random.randint(0,10000)
        author_num = random.randint(0,1000)
        categ_num = random.randint(0,20)
        num_of_tags = random.randint(0,5)

        user_num = random.randint(0,10000)
        user_rating_num = random.randint(0,6)

        item_name = 'item_' + str(item_num)
        item_author = 'author_' + str(author_num)
        item_categ = 'categ_' + str(categ_num)
        item_tags = [ 'tag_' + str(i) for i in range(0, num_of_tags)]

        user_id = 'user_' + str(user_num)

        self.client.post("/insertitem",data={'id':item_name,'author':item_author, 'cathegory':item_categ,'tags':item_tags})
        self.client.post("/insertrating",data={'item':item_name,'user':user_id,'rating':user_rating_num})
        response = self.client.get("/recommend",data={'user':user_id, 'fast':True})
        #print "Response status code:", response.status_code
        #print "Response content:", response.content

class RecommTest(HttpLocust):
    task_set = RecommTaskSet


