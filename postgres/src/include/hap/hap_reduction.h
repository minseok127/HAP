/*-------------------------------------------------------------------------
 *
 * hap_reduction.h
 *	  POSTGRES query reduction with HAP planner.
 *
 *
 * src/include/hap/hap_reduction.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef HAP_REDUCTION_H
#define HAP_REDUCTION_H

#include "postgres.h"
#include "access/attnum.h"
#include "hap/hap_encoding.h"
#include "nodes/pathnodes.h"

/* GUC parameter */
extern PGDLLIMPORT bool enable_hap_reduction; /* default: true */

/*
 * HapSubstitutionInfo
 *		The original information that we need to substitute is set as the 
 *		relation information during the finding phase. When reducing, we set the
 *		information of the target table. Once all finding and reducing phases 
 *		are complete, we substitute the var node with the function node using 
 *		this substituion info.
 */
typedef struct HapSubstitutionInfo
{
	/*
	 * Information of origin table
	 */
	Index origin_relid; 		/* relation index */
	AttrNumber origin_attno;	/* attribute number */
	Oid origin_vartype;			/* vartype */
	Oid origin_varcollid;		/* varcollid */
	int16_t origin_descid;		/* HAP desc id of origin table */
	Oid encoding_table_oid;		/* encoding table oid */

	/*
	 * Information of substitutor
	 */
	HapHiddenAttrDesc *target_desc; /* target table's HAP desc */
	Index target_relid;			/* target table's relation index */
} HapSubstitutionInfo;


/*
 * HapSubstitutionCache
 *		We populate the information that will be used during function execution.
 *		This struct is utilized as a cache.
 */
typedef struct HapSubstitutionCache 
{
	Index cache_idx;			/* index in SubstitutionMeta.caches */
	int start_bit;				/* start bit within hidden attribute */
	int end_bit;				/* end bit within hidden attribute */
	int start_byte;				/* start byte within hidden attribute */
	int end_byte;				/* end byte within hidden attribute */
	int bit_size;				/* bit size (= domain) */
	char *encoding_data_store; /* key-value store for original data */
	int data_size;				/* original value size from tuple */

	/* Info for fast decoding. */
	bool is_fast_decoding;		/* it is used for fast decoding. */
	uint64_t mask;				/* mask for fast decoding */
	int rshift;					/* rshift for fast decoding */
} HapSubstitutionCache;

#define CacheGetDataStorePointer(cache) \
			((char *) (((HapSubstitutionCache*) cache)->encoding_data_store))
#define CacheGetDataPointer(cache, idx) \
			(CacheGetDataStorePointer(cache) + \
			(((HapSubstitutionCache *) cache)->data_size * idx))

#define BitOffsetToByteOffset(bitoffset) (bitoffset >> 3)
#define BitsInByte (8)

/*
 * HapSubstitutionCacheMeta
 *		We maintain a global array for the substitution cache. This information
 *		is stored as metadata for the substitution cache. The metadata struct 
 *		contains the number of caches, capacity, and a pointer to the struct for 
 *		caching.
 */
typedef struct HapSubstitutionCacheMeta 
{
	int cache_nums;					/* current # of cache */
	int cache_capacity;				/* capacity of cache */
	HapSubstitutionCache **caches;	/* array of pointer */
} HapSubstitutionCacheMeta;


/*
 * HapSubstitutionContext
 *		This context is utilized as a context by the expression tree walker. 
 *		Before iterating through the expression tree, we establish a 
 *		substitution cache and a substitution info to facilitate modifications 
 *		to elements within the tree.
 */
typedef struct HapSubstitutionContext
{
	HapSubstitutionCache *cache;	/* substitution cache used in walker */
	HapSubstitutionInfo *info;		/* substitution info used in walker */
	Node *parent_node;				/* parent node in walker */
} HapSubstitutionContext;


/*
 * HapForeignKeyInfo
 *		We check implicit foreign key relationship using this struct. 
 */
typedef struct HapForeignKeyInfo {
	Oid reloid; 		/* relation oid */
	AttrNumber attno;	/* relation's attribute number */
} HapForeignKeyInfo;

/* Public Functions */

/* hap_reduction.c */
extern List *HapReducePlan(PlannerInfo* root, List *joinlist);
extern bool FindRedundantRelations(PlannerInfo* root);
extern List *RemoveRedundantRelations(PlannerInfo *root, List *joinlist);
extern FuncExpr *MakeFuncExprForSubstitution(HapSubstitutionInfo *info, 
												HapSubstitutionCache *cache);

/* hap_substitution_cache.c */
extern void InitSubstitutionCacheMeta(void);
extern void FreeSubstitutionCacheMeta(void);
extern HapSubstitutionCache *GetSubstitutionCache(Index idx);
extern HapSubstitutionCache *AllocateSubstitutionCache(void);
extern void FreeSubstitutionCache(HapSubstitutionCache *cache);

#endif	/* HAP_REDUCTION_H */
