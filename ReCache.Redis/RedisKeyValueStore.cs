using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using StackExchange.Redis;
using Newtonsoft.Json;
using System.Reflection;

namespace ReCache.Redis
{
	public class RedisKeyValueStore<TKey, TValue> : IKeyValueStore<TKey, TValue>
	{
		private readonly IServer _server;
		private readonly IDatabase _database;
		private readonly string _keyPrefix;
		private readonly TimeSpan _itemExpiry;

		public IEnumerable<KeyValuePair<TKey, TValue>> Entries => GetEntries();

		public RedisKeyValueStore(IServer server, int db, string keyPrefix, TimeSpan itemExpiry)
		{
			// TODO: constructor overloading + relevant argument validation
			MethodInfo methodInfo = typeof(TKey).GetMethod("ToString");
			if (methodInfo.GetBaseDefinition() == methodInfo)
			{
				throw new ArgumentException("ToString must be overridden in the TKey class to return unique keys for Redis.");
			}

			if (server == null)
				throw new ArgumentNullException(nameof(server));
			
			_server = server;
			_database = server.Multiplexer.GetDatabase(db);
			_keyPrefix = keyPrefix;
			_itemExpiry = itemExpiry;
		}

		public TValue AddOrUpdate(TKey key, TValue addValue, Func<TKey, TValue, TValue> updateValueFactory)
		{
			if (key == null)
				throw new ArgumentNullException(nameof(key));
			if (updateValueFactory == null)
				throw new ArgumentNullException(nameof(updateValueFactory));

			TValue setValue;

			TValue tValue;
			if (TryGetValue(key, out tValue))
			{
				setValue = updateValueFactory(key, tValue);
			}
			else
			{
				setValue = addValue;
			}

			_database.StringSet(key.ToString(), SerializeValue(setValue), _itemExpiry);

			return setValue;
		}

		public bool TryAdd(TKey key, TValue value)
		{
			if (key == null)
				throw new ArgumentNullException(nameof(key));

			TValue temp;
			if (!TryGetValue(key, out temp))
			{
				return _database.StringSet(key.ToString(), SerializeValue(value), _itemExpiry);
			}

			return false;
		}

		public bool TryGetValue(TKey key, out TValue value)
		{
			if (key == null)
				throw new ArgumentNullException(nameof(key));

			if (_database.KeyExists(key.ToString()))
			{
				string stringValue = _database.StringGet(key.ToString());
				value = DeserializeValue(stringValue);
				return true;
			}

			value = default(TValue);
			return false;
		}

		public bool TryRemove(TKey key, out TValue value)
		{
			if (key == null)
				throw new ArgumentNullException(nameof(key));

			if (TryGetValue(key, out value))
			{
				return _database.KeyDelete(key.ToString());
			}

			value = default(TValue);
			return false;
		}

		private string SerializeValue(TValue value)
		{
			return JsonConvert.SerializeObject(value);
		}

		private TValue DeserializeValue(string value)
		{
			return JsonConvert.DeserializeObject<TValue>(value);
		}

		private IEnumerable<KeyValuePair<TKey, TValue>> GetEntries()
		{
			throw new NotImplementedException();
		}
	}
}
