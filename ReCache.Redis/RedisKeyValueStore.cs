using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Reflection;
using Newtonsoft.Json;
using StackExchange.Redis;
using static System.FormattableString;

namespace ReCache.Redis
{
	public class RedisKeyValueStore<TKey, TValue> : IKeyValueStore<TKey, TValue>
	{
		private static readonly Type[] _supportedPrimitiveTypes =
		{
			typeof(Int16),
			typeof(UInt16),
			typeof(Int32),
			typeof(UInt32),
			typeof(Int64),
			typeof(UInt64),
			typeof(Single),
			typeof(Double),
			typeof(Decimal),
			typeof(string),
			typeof(DateTime),
			typeof(TimeSpan)
		};

		private readonly bool _isKeySupportedPrimitiveType = false;
		private readonly IServer _server;
		private readonly IDatabase _database;
		private readonly string _keyPrefix;
		private readonly TimeSpan _keyExpiryTimeout;
		private readonly Converter<string, TKey> _stringToTKeyConverter;

		public IEnumerable<KeyValuePair<TKey, ICacheEntry<TValue>>> Entries => GetEntries();

		public RedisKeyValueStore(IServer server, int db, TimeSpan keyExpiryTimeout)
			: this(server, db, keyExpiryTimeout, null, null)
		{
		}

		public RedisKeyValueStore(IServer server, int db, TimeSpan keyExpiryTimeout, string keyPrefix)
			: this(server, db, keyExpiryTimeout, keyPrefix, null)
		{
		}

		public RedisKeyValueStore(IServer server, int db, TimeSpan keyExpiryTimeout, string keyPrefix, Converter<string, TKey> stringToTKeyConverter)
		{
			if (_supportedPrimitiveTypes.Contains(typeof(TKey)))
			{
				_isKeySupportedPrimitiveType = true;
			}

			if (!_isKeySupportedPrimitiveType && stringToTKeyConverter == null)
			{
				throw new ArgumentException("stringToKeyConverter must be provided for non-primitive TKey.");
			}

			if (server == null)
				throw new ArgumentNullException(nameof(server));

			IEnumerable<MethodInfo> methodInfos = typeof(TKey).GetMethods().Where(m => m.Name.Equals("ToString", StringComparison.InvariantCulture));
			if (!methodInfos.Any(m => m.GetBaseDefinition() == m))
			{
				throw new ArgumentException("ToString must be overridden in the TKey class to return unique keys for Redis.");
			}

			_server = server;
			_database = server.Multiplexer.GetDatabase(db);
			_keyPrefix = keyPrefix ?? "";
			_keyExpiryTimeout = keyExpiryTimeout;
			_stringToTKeyConverter = stringToTKeyConverter;
		}

		public ICacheEntry<TValue> AddOrUpdateEntry(TKey key, TValue addValue, Func<TKey, TValue, TValue> updateValueFactory)
		{
			if (key == null)
				throw new ArgumentNullException(nameof(key));
			if (updateValueFactory == null)
				throw new ArgumentNullException(nameof(updateValueFactory));

			TValue setValue;

			ICacheEntry<TValue> entry;
			if (TryGetEntry(key, out entry))
			{
				setValue = updateValueFactory(key, entry.CachedValue);
			}
			else
			{
				setValue = addValue;
			}

			if (!_database.StringSet(GetRedisKey(key), SerializeValue(setValue), _keyExpiryTimeout))
			{
				entry = null;
			}

			return entry;
		}

		public bool TryAdd(TKey key, TValue value)
		{
			if (key == null)
				throw new ArgumentNullException(nameof(key));

			ICacheEntry<TValue> temp;
			if (!TryGetEntry(key, out temp))
			{
				return _database.StringSet(GetRedisKey(key), SerializeValue(value), _keyExpiryTimeout);
			}

			return false;
		}

		public bool TryGetEntry(TKey key, out ICacheEntry<TValue> entry)
		{
			if (key == null)
				throw new ArgumentNullException(nameof(key));

			RedisKey redisKey = GetRedisKey(key);

			if (_database.KeyExists(redisKey))
			{
				entry = new RedisCacheEntry<TValue>(redisKey, _database, _keyExpiryTimeout);
				return true;
			}

			entry = null;
			return false;
		}

		public bool TryRemoveEntry(TKey key, out ICacheEntry<TValue> entry)
		{
			if (key == null)
				throw new ArgumentNullException(nameof(key));

			if (TryGetEntry(key, out entry))
			{
				return _database.KeyDelete(GetRedisKey(key));
			}

			return false;
		}

		private RedisKey GetRedisKey(TKey key)
		{
			return _keyPrefix + key.ToString();
		}

		public static string SerializeValue(TValue value)
		{
			return JsonConvert.SerializeObject(value);
		}

		internal static TValue DeserializeValue(string value)
		{
			return JsonConvert.DeserializeObject<TValue>(value);
		}

		private IEnumerable<KeyValuePair<TKey, ICacheEntry<TValue>>> GetEntries()
		{
			ConnectionMultiplexer connection = _server.Multiplexer;

			foreach (EndPoint endPoint in connection.GetEndPoints())
			{
				IServer server = connection.GetServer(endPoint);

				foreach (RedisKey redisKey in server.Keys(_database.Database, Invariant($"{_keyPrefix}*")))
				{
					string keyString = redisKey.ToString();

					keyString = keyString.Remove(0, _keyPrefix.Length);

					TKey key = ConvertStringToKey(keyString);

					ICacheEntry<TValue> entry;
					if (this.TryGetEntry(key, out entry))
					{
						yield return new KeyValuePair<TKey, ICacheEntry<TValue>>(key, entry);
					}
				}
			}
		}

		private TKey ConvertStringToKey(string keyString)
		{
			if (_isKeySupportedPrimitiveType)
			{
				Type keyType = typeof(TKey);

				// TODO: this hasn't been tested yet
				return (TKey)Convert.ChangeType(keyString, keyType);
			}

			return _stringToTKeyConverter(keyString);
		}

		public void InvalidateAll(Func<TKey, bool> invalidateFunc)
		{
			// Nothing to do here
		}

		public int FlushInvalidatedEntries(int maximumCacheSizeIndicator, DateTime someTimeAgo, Func<TKey, bool> invalidateFunc)
		{
			// Nothing to do here
			return 0;
		}
	}
}