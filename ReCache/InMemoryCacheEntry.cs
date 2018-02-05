using System;

namespace ReCache
{
	public class InMemoryCacheEntry<TValue> : ICacheEntry<TValue>
	{
		private TValue _cachedValue;

		public TValue CachedValue
		{
			get
			{
				this.TimeLastAccessed = DateTime.UtcNow;
				return _cachedValue;
			}
			set
			{
				_cachedValue = value;
			}
		}

		public DateTime TimeLoaded { get; private set; } = DateTime.UtcNow;
		public DateTime TimeLastAccessed { get; private set; }

		public void ResetExpiryTimeout()
		{
			this.TimeLoaded = DateTime.UtcNow;
		}
	}
}