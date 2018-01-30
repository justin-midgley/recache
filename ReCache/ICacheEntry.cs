using System;
using System.Collections.Generic;
using System.Text;

namespace ReCache
{
    public interface ICacheEntry<TValue>
    {
		TValue CachedValue { get; }
		DateTime TimeLoaded { get; }
		DateTime TimeLastAccessed { get; }

		void ResetExpiryTimeout();
	}
}
