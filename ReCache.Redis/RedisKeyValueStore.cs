using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace ReCache.Redis
{
    public class RedisKeyValueStore<TKey, TValue> : IKeyValueStore<TKey, TValue>
    {
		public TValue AddOrUpdate(TKey key, TValue addValue, Func<TKey, TValue, TValue> updateValueFactory)
		{
			throw new NotImplementedException();
		}

		public bool TryAdd(TKey key, TValue value)
		{
			throw new NotImplementedException();
		}

		public bool TryGetValue(TKey key, out TValue value)
		{
			throw new NotImplementedException();
		}

		public bool TryRemove(TKey key, out TValue value)
		{
			throw new NotImplementedException();
		}

		public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
		{
			throw new NotImplementedException();
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			throw new NotImplementedException();
		}
	}
}
