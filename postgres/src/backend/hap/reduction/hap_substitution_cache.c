/*-------------------------------------------------------------------------
 *
 * hap_substitution_cache.c
 *	  Substition cache with Hidden Attribute Partitioning (HAP)
 *
 *
 * IDENTIFICATION
 *	  src/backend/hap/planner/hap_reduction.c
 *-------------------------------------------------------------------------
 */
#ifdef HAP_REDUCTION
#include "postgres.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/resowner_private.h"

#include "hap/hap_reduction.h"

/* Meta data for substitution cache */
static HapSubstitutionCacheMeta SubstitutionCacheMeta;

/*
 * InitSubstitutionCacheMeta
 */
void 
InitSubstitutionCacheMeta(void)
{
	SubstitutionCacheMeta.cache_capacity = 0;
	SubstitutionCacheMeta.cache_nums = 0;
	SubstitutionCacheMeta.caches = NULL;
}

/*
 * FreeSubstitutionCacheMeta
 */
void
FreeSubstitutionCacheMeta(void)
{
	if (SubstitutionCacheMeta.cache_capacity == 0)
	{
		Assert(SubstitutionCacheMeta.caches == NULL);
		return;
	}	

	for (int i = 0; i < SubstitutionCacheMeta.cache_capacity; ++i)
	{
		if (SubstitutionCacheMeta.caches[i])
		{
			FreeSubstitutionCache(SubstitutionCacheMeta.caches[i]);

			SubstitutionCacheMeta.caches[i] = NULL;
		}
	}

	Assert(SubstitutionCacheMeta.cache_nums == 0);

	pfree(SubstitutionCacheMeta.caches);

	SubstitutionCacheMeta.caches = NULL;
	SubstitutionCacheMeta.cache_capacity = 0;
}

/*
 * EnlargeSubstitutionCache
 */
static void
EnlargeSubstitutionCache(void)
{
	int i;
	int oldCapacity, newCapacity;
	HapSubstitutionCache **newCaches;
	MemoryContext oldcxt = MemoryContextSwitchTo(TopMemoryContext);

	/* Calculate new capacity. */
	oldCapacity = SubstitutionCacheMeta.cache_capacity;
	newCapacity = oldCapacity == 0 ? 16 : oldCapacity * 2;

	/* Allocate new array. */
	newCaches = (HapSubstitutionCache **) 
					palloc0(sizeof(HapSubstitutionCache *) * newCapacity);

	for (i = 0; i < newCapacity; ++i)
	{
		if (SubstitutionCacheMeta.caches && SubstitutionCacheMeta.caches[i])
			/* Copy existing pointers. */
			newCaches[i] = SubstitutionCacheMeta.caches[i];
		else
			/* Init remaining elements of array. */
			newCaches[i] = NULL;
	}

	/* Old resource cleanup. */
	if (SubstitutionCacheMeta.caches)
		pfree(SubstitutionCacheMeta.caches);

	SubstitutionCacheMeta.cache_capacity = newCapacity;
	SubstitutionCacheMeta.caches = newCaches;

	MemoryContextSwitchTo(oldcxt);
}

/*
 * GetSubstitutionCache
 */
inline HapSubstitutionCache *
GetSubstitutionCache(Index idx)
{
	if (SubstitutionCacheMeta.caches[idx])
		return SubstitutionCacheMeta.caches[idx];

	return NULL;
}


/*
 * AllocateSubstitutionCache
 */
HapSubstitutionCache *
AllocateSubstitutionCache(void)
{
	HapSubstitutionCache *newCache = NULL;
	MemoryContext oldcxt = MemoryContextSwitchTo(TopMemoryContext);

	ResourceOwnerEnlargeSubstitutionCache(CurrentResourceOwner);

	/* If cache gets overflow, we enlarge cache array with doubling. */
	if (SubstitutionCacheMeta.cache_capacity <= SubstitutionCacheMeta.cache_nums)
		EnlargeSubstitutionCache();
	
	/* Find allocatable element, iterating cache array. */
	for (int i = 0; i < SubstitutionCacheMeta.cache_capacity; ++i)
	{
		if (SubstitutionCacheMeta.caches[i] == NULL)
		{
			newCache = (HapSubstitutionCache *)
										palloc0(sizeof(HapSubstitutionCache));

			newCache->cache_idx = i;
			newCache->start_bit = 0;
			newCache->end_bit = 0;
			newCache->start_byte = 0;
			newCache->end_byte = 0;
			newCache->bit_size = 0;
			newCache->encoding_data_store = NULL;
			newCache->mask = 0;
			newCache->rshift = 0;
			newCache->is_fast_decoding = false;

			SubstitutionCacheMeta.cache_nums += 1;
			SubstitutionCacheMeta.caches[i] = newCache;

			ResourceOwnerRememberSubstitutionCache(CurrentResourceOwner, newCache);
			MemoryContextSwitchTo(oldcxt);
			return newCache;
		}
	}

	/* dead zone */
	Assert(false);

	return NULL; /* quiet compiler */
}

/*
 * FreeSubstitutionCache
 */
void
FreeSubstitutionCache(HapSubstitutionCache *cache)
{
	Index cache_idx = cache->cache_idx;

	/* Resource cleanup */
	if (cache->encoding_data_store)
		pfree(cache->encoding_data_store);

	pfree(SubstitutionCacheMeta.caches[cache_idx]);

	SubstitutionCacheMeta.caches[cache_idx] = NULL;
	SubstitutionCacheMeta.cache_nums -= 1;

	ResourceOwnerForgetSubstitutionCache(CurrentResourceOwner, cache);
}
#endif /* HAP_REDUCTION */
