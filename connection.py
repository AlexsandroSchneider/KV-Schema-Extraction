import redis
from configparser import ConfigParser

def load_config(filename='./config.ini', section='redis'):
    parser = ConfigParser()
    parser.read(filename)

    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return config

def redis_connection():
    config = load_config()
    """ Connect to the PostgreSQL database server """
    try:
        with redis.Redis(**config) as conn:
            print('Connected to the Redis server:', conn.ping())
            return conn
        
    except (redis.ConnectionError, Exception) as error:
        print(error)