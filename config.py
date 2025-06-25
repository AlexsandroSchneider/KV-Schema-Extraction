import redis
from configparser import ConfigParser

def _load_config():
    config = ConfigParser()
    config.read('config.ini')
    return config

def get_redis_connection():
    config = _load_config()
    params = {
        'host': config.get('redis_connection', 'host', fallback='localhost'),
        'port': config.getint('redis_connection', 'port', fallback=6379),
        'decode_responses': config.getboolean('redis_connection', 'decode_responses', fallback=True)
    }
    
    try:
        conn = redis.Redis(**params)
        print(f"Connected to Redis: {params['host']}:{params['port']}")
        return conn
    except Exception as e:
        print(f"Redis connection failed: {e}")
        exit(1)

def get_extractor_config():
    config = _load_config()
    return {
        'database': config.getint('extractor', 'database', fallback=0),
        'batch_size': config.getint('extractor', 'batch_size', fallback=1000),
        'export_variations': config.getboolean('extractor', 'export_variations', fallback=False),
    }