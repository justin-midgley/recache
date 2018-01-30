using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace ReCache
{
	/* Read the following link and understand how ConcurrentDictionary works before modifying this class.
	 * http://arbel.net/2013/02/03/best-practices-for-using-concurrentdictionary/
	 */

	/// <summary>
	/// The InMemoryKeyValueStore is implemented on ConcurrentDictionary<TKey, TValue>.
	/// </summary>
	/// <typeparam name="TKey"></typeparam>
	/// <typeparam name="TValue"></typeparam>
	public class InMemoryKeyValueStore<TKey, TValue> : IKeyValueStore<TKey, TValue>
	{
		private readonly ConcurrentDictionary<TKey, InMemoryCacheEntry<TValue>> _entries;

		public IEnumerable<KeyValuePair<TKey, ICacheEntry<TValue>>> Entries => _entries.Select(x => new KeyValuePair<TKey, ICacheEntry<TValue>>(x.Key, x.Value));

		public InMemoryKeyValueStore()
		{
			_entries = new ConcurrentDictionary<TKey, InMemoryCacheEntry<TValue>>();
		}

		public InMemoryKeyValueStore(IEqualityComparer<TKey> comparer)
		{
			if (comparer == null)
				throw new ArgumentNullException(nameof(comparer));

			_entries = new ConcurrentDictionary<TKey, InMemoryCacheEntry<TValue>>(comparer);
		}

		// Summary:
		//     Attempts to get the value associated with the specified key.
		//
		// Parameters:
		//   key:
		//     The key of the value to get.
		//
		//   value:
		//     When this method returns, contains the object 
		//     that has the specified key, or the default value of the type if the operation
		//     failed.
		//
		// Returns:
		//     true if the key was found.
		//     otherwise, false.
		//
		// Exceptions:
		//   T:System.ArgumentNullException:
		//     key is null.
		public bool TryGetEntry(TKey key, out ICacheEntry<TValue> entry)
		{
			if (key == null)
				throw new ArgumentNullException(nameof(key));

			InMemoryCacheEntry<TValue> cacheEntry;
			bool result = _entries.TryGetValue(key, out cacheEntry);

			entry = cacheEntry;

			return result;
		}

		// Summary:
		//     Adds a key/value pair.
		//     if the key does not already exist, or updates a key/value pair 
		//     by using the specified function if the key already exists.
		//
		// Parameters:
		//   key:
		//     The key to be added or whose value should be updated
		//
		//   addValue:
		//     The value to be added for an absent key
		//
		//   updateValueFactory:
		//     The function used to generate a new value for an existing key based on the key's
		//     existing value
		//
		// Returns:
		//     The new value for the key. This will be either be addValue (if the key was absent)
		//     or the result of updateValueFactory (if the key was present).
		//
		// Exceptions:
		//   T:System.ArgumentNullException:
		//     key or updateValueFactory is null.
		//
		//   T:System.OverflowException:
		//     The dictionary already contains the maximum number of elements (System.Int32.MaxValue).
		public ICacheEntry<TValue> AddOrUpdateEntry(TKey key, TValue addValue, Func<TKey, TValue, TValue> updateValueFactory)
		{
			if (key == null)
				throw new ArgumentNullException(nameof(key));
			if (updateValueFactory == null)
				throw new ArgumentNullException(nameof(updateValueFactory));

			InMemoryCacheEntry<TValue> entry = new InMemoryCacheEntry<TValue> { CachedValue = addValue };

			Func<TKey, InMemoryCacheEntry<TValue>, InMemoryCacheEntry<TValue>> updateEntryFactory = (k, e) =>
			{
				return new InMemoryCacheEntry<TValue> { CachedValue = updateValueFactory(k, e.CachedValue) };
			};

			return _entries.AddOrUpdate(key, entry, updateEntryFactory);
		}

		//
		// Summary:
		//     Attempts to remove and return the value that has the specified key.
		//
		// Parameters:
		//   key:
		//     The key of the element to remove and return.
		//
		//   value:
		//     When this method returns, contains the object removed.
		//     or the default value of the TValue type if key does not exist.
		//
		// Returns:
		//     true if the object was removed successfully; otherwise, false.
		//
		// Exceptions:
		//   T:System.ArgumentNullException:
		//     key is null.
		public bool TryRemoveEntry(TKey key, out ICacheEntry<TValue> entry)
		{
			if (key == null)
				throw new ArgumentNullException(nameof(key));

			InMemoryCacheEntry<TValue> cacheEntry;
			bool result = _entries.TryRemove(key, out cacheEntry);

			entry = cacheEntry;

			return result;
		}

		//
		// Summary:
		//     Attempts to add the specified key and value.
		//
		// Parameters:
		//   key:
		//     The key of the element to add.
		//
		//   value:
		//     The value of the element to add. The value can be null for reference types.
		//
		// Returns:
		//     true if the key/value pair was added
		//     successfully; false if the key already exists.
		//
		// Exceptions:
		//   T:System.ArgumentNullException:
		//     key is null.
		//
		//   T:System.OverflowException:
		//     The dictionary already contains the maximum number of elements (System.Int32.MaxValue).
		public bool TryAdd(TKey key, TValue value)
		{
			if (key == null)
				throw new ArgumentNullException(nameof(key));

			var entry = new InMemoryCacheEntry<TValue>();
			entry.CachedValue = value;

			return _entries.TryAdd(key, entry);
		}

		public int FlushInvalidatedEntries(int maximumCacheSizeIndicator, DateTime someTimeAgo, Func<TKey, bool> invalidateFunc)
		{
			// Firsh flush stale entries.
			var remainingEntries = new List<KeyValuePair<TKey, ICacheEntry<TValue>>>();
			// Enumerating over the ConcurrentDictionary is thread safe and lock free.
			foreach (var pair in this.Entries)
			{
				var key = pair.Key;
				var entry = pair.Value;
				if (entry.TimeLoaded < someTimeAgo)
				{
					// Entry is stale, remove it.
					if (!invalidateFunc(key))
						remainingEntries.Add(pair);
				}
				else
					remainingEntries.Add(pair);
			}

			// Now flush anything exceeding the max size, starting with the oldest entries first.
			if (remainingEntries.Count > maximumCacheSizeIndicator)
			{
				int numberOfEntriesToTrim = remainingEntries.Count - maximumCacheSizeIndicator;
				var keysToRemove = remainingEntries
					.OrderBy(p => p.Value.TimeLoaded)
					.ThenBy(p => p.Value.TimeLastAccessed)
					.Take(numberOfEntriesToTrim)
					.ToList();

				foreach (var entry in keysToRemove)
				{
					invalidateFunc(entry.Key);
					remainingEntries.Remove(entry);
				}
			}

			return remainingEntries.Count;
		}

		public void InvalidateAll(Func<TKey, bool> invalidateFunc)
		{
			// Clear() acquires all internal locks simultaneously, so will cause more contention.
			//_cachedEntries.Clear();

			foreach (var pair in this.Entries)
				invalidateFunc(pair.Key);
		}
	}
}
