using System;
using System.Collections.Generic;

namespace ReCache
{
	[System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1710:IdentifiersShouldHaveCorrectSuffix")]
	public interface IKeyValueStore<TKey, TValue>
	{
		IEnumerable<KeyValuePair<TKey, ICacheEntry<TValue>>> Entries { get; }

		ICacheEntry<TValue> AddOrUpdateEntry(TKey key, TValue addValue, Func<TKey, TValue, TValue> updateValueFactory);

		bool TryAdd(TKey key, TValue value);

		bool TryGetEntry(TKey key, out ICacheEntry<TValue> entry);

		bool TryRemoveEntry(TKey key, out ICacheEntry<TValue> entry);

		int FlushInvalidatedEntries(int maximumCacheSizeIndicator, DateTime someTimeAgo, Func<TKey, bool> invalidateFunc);

		void InvalidateAll(Func<TKey, bool> invalidateFunc);
	}
}