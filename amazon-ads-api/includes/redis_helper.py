import os
import redis


class MyRedis(object):

    def __init__(self):
        host = os.getenv("REDIS_HOST")
        pwd = os.getenv("REDIS_PASSWORD")
        port = os.getenv("REDIS_PORT")
        use_tls = os.getenv("REDIS_TLS", "false").lower() == "true"
        self.client = redis.StrictRedis(host=host, port=port, password=pwd, ssl=use_tls)

    def scan_keys(self, keyword):
        keys = self.client.keys(keyword)
        keys = [each_key.decode('utf-8') for each_key in keys]
        return keys

    def set_value(self, key, value, ttl=None):
        if ttl is None:
            self.client.set(key, value)
        else:
            self.client.setex(key, ttl, value)

    def get_value(self, key):
        return self.client.get(key)

    def redis_hset(self, redis_key, fields, values):
        field_values = dict(zip(fields, values))
        for field in field_values:
            value = field_values[field]
            self.client.hset(redis_key, field, value)
        return True

    def get_hset_keys(self, date_range='', campaign_types='', redis_key=''):
        redis_key = self.build_key(date_range=date_range, campaign_types=campaign_types, redis_key=redis_key)
        keys = self.client.hkeys(redis_key)
        keys = [each_key.decode('utf-8') for each_key in keys]
        return keys

    def get_hset_values(self, date_range='', campaign_types='', redis_key=''):
        redis_key = self.build_key(date_range=date_range, campaign_types=campaign_types, redis_key=redis_key)
        vals = self.client.hvals(redis_key)
        vals = [each_val.decode('utf-8') for each_val in vals]
        return vals

    def get_hset_value(self, date_range='', campaign_types='', account_id='', channel_ids='', redis_key='',
                       redis_field=''):
        redis_key = self.build_key(date_range=date_range, campaign_types=campaign_types, redis_key=redis_key)
        if redis_field == '':
            channel_ids = ",".join(map(str, channel_ids)) if isinstance(channel_ids, list) else channel_ids
            redis_field = "{}:{}".format(account_id, channel_ids)
        return self.client.hget(redis_key, redis_field)

    def hdel_keys(self, redis_key, hkeys):
        self.client.hdel(redis_key, *hkeys)

    def insert_value_in_set(self, set_key, value):
        self.client.sadd(set_key, value)

    def get_set_members(self, set_key):
        members = self.client.smembers(set_key)
        members = [each_val.decode('utf-8') for each_val in members]
        return members

    def remove_set_members(self, set_key, value):
        return self.client.srem(set_key, value)

    def remove_key(self, key):
        result = self.client.delete(key)

        if result == 1:
            return True
        else:
            return False
