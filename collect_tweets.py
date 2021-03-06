from datetime import datetime, timedelta
import logging
import time
from prometheus_client import Counter
from sqlalchemy.orm import sessionmaker
from db import connect_to_db, SearchTerm
from redis_eq import connect_to_redis
from twitter_eq import Twitter, Search

API_RATE_LIMIT = 450
API_WINDOW_PERIOD = 15
SECONDS_WAITING_FOR_RATE_LIMIT = Counter('seconds_waiting_for_rate_limit', '# of queries made to Twitter')


def get_sql_session():
    """
    Creates and returns a SQLAlchemy session() object
    """
    session = sessionmaker(connect_to_db())
    return session()


def check_search_terms(redis_client):
    """
    The application needs a list of terms to search, and expects them to be in Redis.
    If there are no terms in Redis, the function calls to populate them
    """
    if redis_client.scard('search_terms') == 0:
        populate_search_terms(redis_client)


def populate_search_terms(redis_client):
    """
    Pulls a list of search terms from a SQL database and stores them in Redis.
    If you're forking this application, it may be easier to retrieve the terms from an environment variable.
    """
    logging.info('Populating search terms into redis from SQL', extra={'category': 'search_terms'})
    sql_session = get_sql_session()
    search_terms = sql_session.query(SearchTerm.term).all()
    redis_client.sadd('search_terms', *[x.term for x in search_terms])


def check_limit(redis_client):
    """
    Checks redis to see whether we have hit the Twitter API query limit
    Theoretically, the python-twitter client can do this, but there is currently a bug in it
    """
    if redis_client.llen('query_counter') >= API_RATE_LIMIT:
        left_val = redis_client.lpop('query_counter')
        parsed_left_val = float(left_val.decode('utf-8'))
        current_api_window = (datetime.utcnow() - timedelta(minutes=API_WINDOW_PERIOD)).timestamp()
        if parsed_left_val > current_api_window:
            redis_client.lpush('query_counter', left_val)
            return False
    return True


def get_next_query(redis_client):
    """
    Finds and returns the next search term that the application should query.
    It does this by querying Redis and sorting the search_terms by score
    Each search_term's score is set after a query, and depends on how many Tweets the application needs to collect
    """
    return redis_client.sort('search_terms', by='*->score')[0].decode('utf-8')


def collect_tweets(redis_client, twitter_client, search_term):
    """
    For a given search term, creates a Search object and runs all its functions necessary to search and store
    """
    search = Search(redis_client, twitter_client, search_term)
    search.get_term_state()
    search.parse_term_state()
    search.set_query_string()
    search.set_execution_time()
    search.execute_query()
    search.incr_query_counters()
    search.set_newest_id()
    search.set_oldest_id()
    search.set_scenario()
    search.set_term_state()
    search.store_results()
    search.set_score()
    search.log_state()


def initiate_collection():
    redis_client = connect_to_redis()
    twitter_client = Twitter().client
    check_search_terms(redis_client)
    is_sleeping = False
    while True:
        if check_limit(redis_client):
            if is_sleeping:
                is_sleeping = False
                logging.info('Ingesting tweets', extra={'category': 'rate_limit'})
            search_term = get_next_query(redis_client)
            collect_tweets(redis_client, twitter_client, search_term)
        else:
            if not is_sleeping:
                logging.info('Twitter API rate limit hit', extra={'category': 'rate_limit'})
                is_sleeping = True
            SECONDS_WAITING_FOR_RATE_LIMIT.inc()
            time.sleep(5)
