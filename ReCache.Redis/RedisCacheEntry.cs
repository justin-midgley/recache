using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Text;

namespace ReCache.Redis
{
    class RedisCacheEntry<TValue> : ICacheEntry<TValue>
    {
		private readonly RedisKey _key;
		private readonly IDatabase _database;
		private readonly TimeSpan _expiryTimeout;

		public TValue CachedValue => GetCachedValue();

		public DateTime TimeLoaded => DateTime.UtcNow;
		public DateTime TimeLastAccessed => DateTime.UtcNow;

		public RedisCacheEntry(RedisKey key, IDatabase database, TimeSpan expiryTimeout)
		{
			_key = key;
			_database = database;
			_expiryTimeout = expiryTimeout;
		}
		
		private TValue GetCachedValue()
		{
			string stringValue = _database.StringGet(_key);
			TValue value = RedisKeyValueStore<object, TValue>.DeserializeValue(stringValue);

			return value;
		}

		public void ResetExpiryTimeout()
		{
			_database.KeyExpire(_key, _expiryTimeout);
		}
	}
}
