from datetime import datetime
import json
import logging
import os
from urllib.parse import quote_plus
from prometheus_client import Counter
import twitter

TWITTER_QUERIES = Counter('twitter_queries', '# of queries made to Twitter')
SCENARIOS = {
    'base': {
        'oldest_id': 'newest_id',
        'newest_id': 'newest_id',
        'last_success': 'newest_id',
        'success': 'true'
    },
    'current_fail_last_success': {
        'oldest_id': 'oldest_id',
        'newest_id': 'previous_newest_id',
        'last_success': 'newest_id',
        'success': 'false'
    },
    'current_fail_last_fail': {
        'oldest_id': 'oldest_id',
        'newest_id': 'previous_newest_id',
        'last_success': 'last_success',
        'success': 'false'
    },
    'current_success_last_success': {
        'oldest_id': 'oldest_id',
        'newest_id': 'newest_id',
        'last_success': 'newest_id',
        'success': 'true'
    },
    'current_success_last_fail': {
        'oldest_id': 'oldest_id',
        'newest_id': 'last_success',
        'last_success': 'previous_newest_id',
        'success': 'true'
    }
}


class Twitter(object):
    def __init__(self):
        self.client = None
        self.auth()

    def auth(self):
        """
        Connects to Twitter
        """
        self.client = twitter.Api(
            consumer_key=os.getenv('TWITTER_CONSUMER_KEY'),
            consumer_secret=os.getenv('TWITTER_CONSUMER_SECRET'),
            access_token_key=os.getenv('TWITTER_ACCESS_TOKEN_KEY'),
            access_token_secret=os.getenv('TWITTER_ACCESS_TOKEN_SECRET'),
            application_only_auth=True
        )


class Search(object):
    """
    This class searches Twitter hashtags for a given string and stores the results in Redis
    Args:
        redis_client (redis object): Redis DB client
        twitter_client (python-twitter object): Twitter API client
        search_term (str): Term we are querying twitter for
    """

    def __init__(self, redis_client, twitter_client, search_term):
        self.redis_client = redis_client
        self.twitter_client = twitter_client
        self.search_term = search_term
        self.previous_newest_id = None
        self.previous_oldest_id = None
        self.last_success = None
        self.success = None
        self.newest_id = None
        self.oldest_id = None
        self.scenario = None

        self.new_term_state = {}
        self.execution_time = None
        self.results = None
        self.query_string = None

    def set_query_string(self):
        """
        Sets the query string for the Twitter client search
        """
        self.query_string = f'q=%23{quote_plus(self.search_term)}&result_type=recent&count=100'
        if self.previous_newest_id:
            if self.previous_oldest_id <= self.previous_newest_id or self.success:
                self.query_string += f'&since_id={self.previous_newest_id}'
            else:
                self.query_string += f'&max_id={self.previous_oldest_id}'
                self.query_string += f'&since_id={self.previous_newest_id}'

    def set_execution_time(self):
        #  We store execution time in Redis so that we can ensure we don't exceed the Twitter API rate limits
        self.execution_time = datetime.utcnow().timestamp()

    def execute_query(self):
        self.results = self.twitter_client.GetSearch(raw_query=self.query_string)

    def incr_query_counters(self):
        TWITTER_QUERIES.inc()
        self.redis_client.rpush('query_counter', self.execution_time)

    def set_newest_id(self):
        if self.results:
            self.newest_id = getattr(self.results[0], 'id')

    def set_oldest_id(self):
        if self.results:
            self.oldest_id = getattr(self.results[-1], 'id')

    def parse_term_state_datum(self, datum):
        """
        Data retrieved from Redis is of byte type and we need to converted it to integer or None
        """
        if datum:
            if datum != b'null':
                datum = int(datum.decode('utf-8'))
            else:
                datum = None
        return datum

    def get_term_state(self):
        term_state = self.redis_client.hmget(self.search_term, 'newest_id', 'oldest_id', 'last_success', 'success')
        self.previous_newest_id, self.previous_oldest_id, self.last_success, self.success = term_state

    def parse_term_state(self):
        self.previous_newest_id = self.parse_term_state_datum(self.previous_newest_id)
        self.previous_oldest_id = self.parse_term_state_datum(self.previous_oldest_id)
        self.last_success = self.parse_term_state_datum(self.last_success)
        if not self.success:
            self.success = True
        else:
            self.success = True if self.success == 'true' else False

    def set_scenario(self):
        """
        Based on the Tweets we found in our query, we need to acknowledge which scenario our current search state is in
        """
        if not self.previous_newest_id or not self.oldest_id:
            self.scenario = 'base'
        elif (self.oldest_id > self.previous_newest_id) and self.success:
            self.scenario = 'current_fail_last_success'
        elif (self.oldest_id > self.previous_newest_id) and not self.success:
            self.scenario = 'current_fail_last_fail'
        elif (self.oldest_id <= self.previous_newest_id) and self.success:
            self.scenario = 'current_success_last_success'
        elif (self.oldest_id <= self.previous_newest_id) and not self.success:
            self.scenario = 'current_success_last_fail'

    def set_term_state(self):
        for key, value in SCENARIOS[self.scenario].items():
            if key == 'success':
                self.new_term_state[key] = value
            else:
                new_value = getattr(self, value)
                if not new_value:
                    new_value = 'null'
                self.new_term_state[key] = new_value
        self.redis_client.hmset(self.search_term, self.new_term_state)

    def set_score(self):
        """
        This function sets the score for our search term in Redis
        If we found all the recently-posted Tweets in this query
            Set the score to 99 . . . 99. This ensures that this term is pulled last from the queue
        If we did not find all the recently-posted Tweets,
            Set the score to the int of the last fully-collected period for this term (self.last_success)
        This ensures that the search terms we are farthest behind on are queried first.
        """
        if self.success:
            # TODO: Is this hack okay?
            self.redis_client.hset(self.search_term, 'score', 9999999999999999999)
        else:
            if not self.last_success:
                self.redis_client.hset(self.search_term, 'score', 9999999999999999999)
            else:
                self.redis_client.hset(self.search_term, 'score', self.last_success)
        self.redis_client.sadd('search_terms', self.search_term)

    def store_results(self):
        """
        Pushes each result onto a Redis queue
        TODO: Why iterate and do this one by one.  Can I push it all as a list?
        """
        for result in self.results:
            result = result.AsDict()
            result['search_term'] = self.search_term
            self.redis_client.rpush('tweets', json.dumps(result, ensure_ascii=True, sort_keys=True))

    def log_state(self):
        """
        This function logs the important attributes of the object
        """
        bad_vars = ['results', 'redis_client', 'twitter_client']
        class_vars = [attr for attr in dir(self) if
                      not callable(getattr(self, attr)) and not attr.startswith("__") and attr not in bad_vars]
        class_data = {}
        for class_var in class_vars:
            class_data[class_var] = getattr(self, class_var)

        logging.info(f'Logging search object data for execution at {self.execution_time}', extra=class_data)
