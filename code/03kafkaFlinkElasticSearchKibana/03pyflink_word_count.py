from pyflink.table.expressions import col, lit
from pyflink.common import Row
from pyflink.table.udf import udf, udtf, ScalarFunction
from nltk.corpus import stopwords # pip install nltk (natural language took kit for NLP natural laguage processing)
# stopwords -> the, is, in, at...
import codecs # NLTK codecs manage text encoding/decoding to ensure proper handling of multilingual files and prevent encoding errors in NLP tasks.
import re # NLTK (regular expressions) are used to match, search, and manipulate text patterns
import string # text data that can be processed, tokenized, or analyzed, serving as input for various NLP operations like tokenization, parsing, or classification.

from pyflink.table import (
    DataTypes, TableEnvironment, EnvironmentSettings
)

import nltk
nltk.download ('stopwords')
# ^ using python console

def word_count_stream_processing():
    # Create a TableEnvironment
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)

    # Specify connector and format jars
    t_env.get_config().get_configuration().set_string(
        "pipeline.jars",
        "file:///tmp/scripts/jars/flink-python-1.17.2.jar;"
        "file:///tmp/scripts/jars/flink-sql-connector-kafka-1.17.1.jar;" # mind -> ;
        "file:///tmp/scripts/jars/flink-sql-connector-elasticsearch7-3.0.1-1.17.jar"
    )

    # Define source and sink DDLs
    source_ddl = """
    CREATE TABLE source_table(
        id_str VARCHAR,
        username VARCHAR,
        tweet VARCHAR,
        location VARCHAR,
        created_at VARCHAR,
        retweet_count BIGINT,
        followers_count BIGINT,
        lang VARCHAR,
        coordinates VARCHAR
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'tweets-sim',
        'properties.bootstrap.servers' = 'docker-kafka-1:29092',
        'properties.group.id' = 'test_3',
        'scan.startup.mode' = 'latest-offset',
        'format' = 'json'
    )
    """

    sink_ddl = """
    CREATE TABLE sink_table(
            word VARCHAR,
            number BIGINT
    ) WITH (        
        'connector' = 'elasticsearch-7',
        'index' = 'kafka_flink_elasticsearch_word_count',
        'hosts' = 'http://elasticsearch:9200',
        'format' = 'json'
    )
    """

    # Execute source and sink DDLs
    t_env.execute_sql(source_ddl)
    t_env.execute_sql(sink_ddl)

    # Read from source table
    source_table = t_env.from_path('source_table')

    print("\nKafka source table Schema")
    source_table.print_schema()

    # Preprocess the tweet text
    # @ It is used for decorators, which modify the behavior of functions or classes.
    @udf(result_type=DataTypes.STRING()) # not like a lambda code06.py
    def preprocess_text(review):
        review = codecs.decode(review, 'unicode_escape')  # remove unicode escape characters
        #review = review[2:-1]
        review = re.sub('((www\.[^\s]+)|(https?://[^\s]+))', 'URL', review)
        review = re.sub('[^\x00-\x7f]', '', review)
        review = re.sub('@[^\s]+', 'USER', review)
        review = re.sub('RT', '', review)
        review = review.lower().replace("ё", "е")
        review = re.sub('[^a-zA-Zа-яА-Я1-9]+', ' ', review)
        review = re.sub(' +', ' ', review)
        return review.strip()

    processed_table = source_table.select(preprocess_text(col('tweet')))

    print("\nData cleaning and processing ...")

    # Split lines into words
    # User Defined table function decorator
    # takes an input row and returns multiple output rows, which is useful for operations such as splitting strings into several results.
    @udtf(result_types=[DataTypes.STRING()]) # each row will be String
    def split(line: Row): #a Row can contain multiples COL
        # but we are only interested on the one and only 0 col
        for s in line[0].split(): # split the line in words by space char
            yield Row(s)

    # apply split for all tweets and then flat to get  ROWs with a single word each
    word_table = processed_table.flat_map(split).alias('word') # name the only column to 'word'
    print("\n\n Splitting lines to words ...")

    # Normalize the word by removing punctuation
    @udf(result_type=DataTypes.STRING())
    def normalize(word: str):
        # maketrans prepares the dictionary translation using a set of puntuaction string.punctuation: !"#$%&'()*+,-./:;<=>?@[\]^_{|}~`)
        # replacing sign by '' (the '' also will be replaced by '')
        return word.translate(str.maketrans('', '', string.punctuation))

    normalized_table = word_table.select(normalize(col('word')).alias('word')) #map to remove punctuation
    
    print("Removing stop words (the, is, a, an, ...)")
    # Initialize the stop word resource with NLTK

    # This class inherits from ScalarFunction, which is the base class for defining a scalar UDF in PyFlink. 
    # A scalar UDF transforms one input into one output.
    class RemoveStopWord(ScalarFunction): 
        stop_words = set(stopwords.words('english')) #creating a set from english stop words

        # defines the logic that will be applied to each word. 
        # In Flink, the eval method is what gets executed to process the input data.
        def eval(self, word: str):
            # If the word is not a stopword, it returns True; otherwise, it returns False
            return word not in self.stop_words

    remove_stop_words_udf = udf(RemoveStopWord(), result_type=DataTypes.BOOLEAN())
    #filtered table without stopwords
    filtered_table = normalized_table.filter(remove_stop_words_udf(col('word')))

    # Compute the word count using Table API
    result = filtered_table.group_by(col('word')) \
        .select(col('word'), lit(1).count.alias('number')) # count aggregation operation
        # lit(1) creates a constant literal value of 1 for each row
        # new column named 'number'
    try:
        result.execute_insert('sink_table').wait()
        print("Processing complete!")
    except Exception as e:
        print("Error head")
        print(e)
        print("Error tail")

word_count_stream_processing()