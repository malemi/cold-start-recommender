from distutils.core import setup

setup(name='cold-start-recommender',
      description='In-memory recommender for recommendations produced on-the-fly',
      author='Mario Alemi',
      author_email='mario.alemi@gmail.com',
      version='0.3.15',
      py_modules=['csrec.Recommender', 'tools.Singleton'],
      url='https://github.com/elegans-io/cold-start-recommender',
      license='LICENSE.txt',
      scripts=['bin/recommender_api.py'],
      )
