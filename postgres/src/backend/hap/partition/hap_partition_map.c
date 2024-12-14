/*-------------------------------------------------------------------------
 *
 * hap_partition_map.c
 *	  Partition map's implementation.
 *
 *
 * IDENTIFICATION
 *	  src/backend/hap/planner/hap_partition_map.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/skey.h"
#include "access/table.h"
#include "access/tableam.h"
#include "catalog/index.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "executor/tuptable.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "utils/array.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

#include "hap/hap_encoding.h"
#include "hap/hap_executor.h"
#include "hap/hap_partition.h"
#include "hap/hap_planner.h"

/*
 * HapPreProcessPartKey
 *		Modify the value to include information about the index of array.
 *
 * The given keys contain only encoding values without index information on the
 * array.
 *
 * When searching for the GIN index later, we use not only encoding value but
 * also hidden attribute descriptor id to distinguish the same encoding values.
 *
 * So add descriptor id information to the keys before inserting into the
 * partmap.
 */
static void
HapPreProcessPartKey(HapPartCreateStmt *stmt, Datum *keys)
{
	ListCell *lc;
	int i = 0;

	foreach(lc, stmt->partkey_list)
	{
		HapPartKey *partkey = lfirst(lc);

		keys[i++] |= (partkey->target_descid << HAP_PARTMAP_DESC_SHIFT);
	}
}

/*
 * HapConvertPartKeyToArrayType
 *		Convert the given key to the ArrayType.
 *
 * Partition map's hidden partition key attribute uses int[] which is
 * represented as a ArrayType. 
 *
 * Convert raw Datum array to the ArrayType.
 */
static inline ArrayType *
HapConvertPartKeyToArrayType(Datum *keys, int len)
{
	return construct_array(keys, len, INT4OID, 4, true, TYPALIGN_INT);
}

/*
 * HapInsertPartMapIndexTuple
 *		Insert index entries for partmap tuple.
 *
 * This is effectively a cut-down version of ExecInsertIndexTuples().
 */
static void
HapInsertPartMapIndexTuple(ResultRelInfo *indstate, HeapTuple heapTuple)
{
	int			i;
	int			numIndexes;
	RelationPtr relationDescs;
	Relation	heapRelation;
	TupleTableSlot *slot;
	IndexInfo **indexInfoArray;
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS];

	/*
	 * HOT update does not require index inserts. But with asserts enabled we
	 * want to check that it'd be legal to currently insert into the
	 * table/index.
	 */
#ifndef USE_ASSERT_CHECKING
	if (HeapTupleIsHeapOnly(heapTuple))
		return;
#endif

	/*
	 * Get information from the state structure.  Fall out if nothing to do.
	 */
	numIndexes = indstate->ri_NumIndices;
	if (numIndexes == 0)
		return;
	relationDescs = indstate->ri_IndexRelationDescs;
	indexInfoArray = indstate->ri_IndexRelationInfo;
	heapRelation = indstate->ri_RelationDesc;

	/* Need a slot to hold the tuple being examined */
	slot = MakeSingleTupleTableSlot(RelationGetDescr(heapRelation),
									&TTSOpsHeapTuple);
	ExecStoreHeapTuple(heapTuple, slot, false);

	/*
	 * for each index, form and insert the index tuple
	 */
	for (i = 0; i < numIndexes; i++)
	{
		IndexInfo  *indexInfo;
		Relation	index;

		indexInfo = indexInfoArray[i];
		index = relationDescs[i];

		/* If the index is marked as read-only, ignore it */
		if (!indexInfo->ii_ReadyForInserts)
			continue;

		/*
		 * Expressional and partial indexes on system catalogs are not
		 * supported, nor exclusion constraints, nor deferred uniqueness
		 */
		Assert(indexInfo->ii_Expressions == NIL);
		Assert(indexInfo->ii_Predicate == NIL);
		Assert(indexInfo->ii_ExclusionOps == NULL);
		Assert(index->rd_index->indimmediate);
		Assert(indexInfo->ii_NumIndexKeyAttrs != 0);

		/* see earlier check above */
#ifdef USE_ASSERT_CHECKING
		if (HeapTupleIsHeapOnly(heapTuple))
		{
			Assert(!ReindexIsProcessingIndex(RelationGetRelid(index)));
			continue;
		}
#endif							/* USE_ASSERT_CHECKING */

		/*
		 * FormIndexDatum fills in its values and isnull parameters with the
		 * appropriate values for the column(s) of the index.
		 */
		FormIndexDatum(indexInfo,
					   slot,
					   NULL,	/* no expression eval to do */
					   values,
					   isnull);

		/*
		 * The index AM does the rest.
		 */
		index_insert(index,		/* index relation */
					 values,	/* array of index Datums */
					 isnull,	/* is-null flags */
					 &(heapTuple->t_self),	/* tid of heap tuple */
					 heapRelation,
					 index->rd_index->indisunique ?
					 UNIQUE_CHECK_YES : UNIQUE_CHECK_NO,
					 false,
					 indexInfo);
	}

	ExecDropSingleTupleTableSlot(slot);
}


/*
 * Insert a tuple into the partmap and its indices.
 */
static void
HapInsertPartMapTuple(Relation partmaprel, HeapTuple tuple)
{
	ResultRelInfo resultRelInfo = { 0, };

	/* Open hash index and gin index */
	resultRelInfo.ri_RelationDesc = partmaprel;
	ExecOpenIndices(&resultRelInfo, false);

	/* Insert to the heap */
	simple_heap_insert(partmaprel, tuple);

	/* Insert to the indcies */
	HapInsertPartMapIndexTuple(&resultRelInfo, tuple);

	/* Done */
	ExecCloseIndices(&resultRelInfo);
}

/*
 * HapInsertPartKeyToPartMap
 *		Insert new partmap tuple.
 *
 * Partition map has two attributes, hidden partition key array and partition
 * oid. Make TupleTableSlot using them, and insert it to the partition map. 
 */
void
HapInsertPartKeyToPartMap(HapPartCreateStmt *stmt, Oid partmapid,
						  Datum *keys, Oid partid)
{
	Relation partmaprel = table_open(partmapid, RowExclusiveLock);
	Datum *values = (Datum *) palloc0(2 * sizeof(Datum));
	bool *isnull = (bool *) palloc0(2 * sizeof(bool));
	ArrayType *keyarr;
	HeapTuple tuple;

	/* Make key array to the ArrayType */
	HapPreProcessPartKey(stmt, keys);
	keyarr = HapConvertPartKeyToArrayType(keys,
										  list_length(stmt->partkey_list));

	/* Make tuple */
	values[0] = keyarr;
	values[1] = partid;
	tuple = heap_form_tuple(RelationGetDescr(partmaprel), values, isnull);
	
	/* Insert */
	HapInsertPartMapTuple(partmaprel, tuple);

	/* Done */
	heap_freetuple(tuple);
	pfree(values);
	pfree(isnull);

	table_close(partmaprel, RowExclusiveLock);
}

#ifdef HAP
/*
 * HapGetPartIdHash
 *		Find partition id using hash index of partition map.
 *
 * Partition map has a hash index with a hidden attribute array as a search key.
 * Using this hash index, find partition oid.
 */
Oid
HapGetPartIdHash(Relation rel, Datum *keys, int len)
{
	SysScanDesc scandesc;
	ScanKeyData scankey;
	Relation partmaprel;
	ArrayType *keyarr;
	HeapTuple tuple;
	Datum values[2];
	bool isnull[2];
	bool shouldFree;
	Oid partid;

	if (!rel->rd_hap_partitioned)
		return InvalidOid;

	partmaprel = table_open(rel->rd_hap_partmap_id, AccessShareLock);

	/* Set scan key */
	keyarr = HapConvertPartKeyToArrayType(keys, len);

	ScanKeyInit(&scankey,
				1, /* hidden attribute */
				HTEqualStrategyNumber, /* HASH */
				F_ARRAY_EQ,
				PointerGetDatum(keyarr));

	scandesc = systable_beginscan(partmaprel, rel->rd_hap_partmap_hash_idx_id,
								  true, NULL, 1, &scankey);

	if (!index_getnext_slot(scandesc->iscan, ForwardScanDirection,
							scandesc->slot))
		elog(ERROR, "invalid partition key search");

	tuple = ExecFetchSlotHeapTuple(scandesc->slot, false, &shouldFree);
	Assert(!shouldFree);

	/* Deform partmap tuple */
	heap_deform_tuple(tuple, RelationGetDescr(partmaprel), values, isnull);

	/* Partmap's second attribute is partition oid */
	partid = DatumGetObjectId(values[1]);

	/* Done */
	systable_endscan(scandesc);
	table_close(partmaprel, AccessShareLock);

	pfree(keyarr);

	return partid;
}

/*
 * Make OpEpxr about partition map key.
 */
static Expr *
HapMakePartMapKeyExprInternal(Expr *hapexpr,
							  Oid array_contain_oprid, Oid array_overlap_oprid)
{
	Datum keys[HAP_MAX_PARTKEY_COUNT];
	OpExpr *opexpr;
	Oid opno, opfuncid;
	ArrayType *keyarr;
	Const *partkeycon;
	Var *var;

	if (IsA(hapexpr, OpExpr))
	{
		HapHiddenAttrOpExpr *hapopexpr = (HapHiddenAttrOpExpr *) hapexpr;
		int16 descid = hapopexpr->desc->descid;

		keys[0] = (hapopexpr->value | (descid << HAP_PARTMAP_DESC_SHIFT));
		keyarr = HapConvertPartKeyToArrayType(keys, 1);
		
		opno = array_contain_oprid;
		opfuncid = get_opcode(opno);
	}
	else if (IsA(hapexpr, ScalarArrayOpExpr))
	{
		HapHiddenAttrScalarArrayOpExpr *haparrayopexpr
				= (HapHiddenAttrScalarArrayOpExpr *) hapexpr;
		int len = list_length(haparrayopexpr->value_list);
		int16 descid = haparrayopexpr->desc->descid;

		for (int i = 0; i < len; i++)
		{
			int value = list_nth_int(haparrayopexpr->value_list, i);
			keys[i] = (value | (descid << HAP_PARTMAP_DESC_SHIFT));
		}

		keyarr = HapConvertPartKeyToArrayType(keys, len);

		opno = array_overlap_oprid;
		opfuncid = get_opcode(opno);
	}
	else
		elog(ERROR, "invalid hidden attribute expr type");

	var = makeVar(1, 1, INT4ARRAYOID, -1, 0, 0);
	partkeycon = makeConst(INT4ARRAYOID, -1, 0, -1, keyarr, false, false);

	opexpr = make_opclause(opno, BOOLOID, false, var, partkeycon, 0, 0);
	opexpr->opfuncid = opfuncid;

	return opexpr;
}

/*
 * Make boolean expression about partition map key.
 */
static Expr *
HapMakePartMapKeyExprSub(Expr *hapexpr,
						 Oid array_contain_oprid, Oid array_overlap_oprid)
{
	ListCell *temp;

	if (is_orclause(hapexpr))
	{
		List *orlist = NIL;

		foreach(temp, ((BoolExpr *) hapexpr)->args)
		{
			Expr *newexpr = HapMakePartMapKeyExprSub(hapexpr,
													 array_contain_oprid,
											 		 array_overlap_oprid);

			orlist = lappend(orlist, newexpr);
		}

		return makeBoolExpr(OR_EXPR, orlist, -1);
	}
	else if (is_andclause(hapexpr))
	{
		List *andlist = NIL;

		foreach(temp, ((BoolExpr *) hapexpr)->args)
		{
			Expr *newexpr = HapMakePartMapKeyExprSub(hapexpr,
													 array_contain_oprid,
											 		 array_overlap_oprid);

			andlist = lappend(andlist, newexpr);
		}

		return makeBoolExpr(AND_EXPR, andlist, -1);
	}
	else
		return HapMakePartMapKeyExprInternal(hapexpr,
											 array_contain_oprid,
											 array_overlap_oprid);
}

/*
 * HapMakePartMapKeyExpr
 *		Make a single expression about partition map key.
 * 
 * Each hidden_attribute_expr can be casted to the HapHiddenAttrOpExpr or
 * HapHiddenAttrScalarArrayOpExpr or HapHiddenAttrBoolExpr.
 *
 * Using this, make a partition map key expression.
 */
static Expr *
HapMakePartMapKeyExpr(Expr *hapexpr,
					  Oid array_contain_oprid, Oid array_overlap_oprid)
{
	if (is_orclause(hapexpr))
		return HapMakePartMapKeyExprSub(hapexpr,
										array_contain_oprid,
										array_overlap_oprid);

	Assert(!is_andclause(hapexpr));

	return HapMakePartMapKeyExprInternal(hapexpr,
										 array_contain_oprid,
										 array_overlap_oprid);
}

/*
 * HapCreatePartMapBitmapScanPlan
 *		Create BitmapHeapScan plan node to scan partition map.
 *
 * To search partition map's GIN index, we have to create BitmapHeapScan node.
 *
 * Transform hidden attribute expressions to the given array operator
 * expressions and create plan node.
 */
static BitmapHeapScan *
HapCreatePartMapBitmapScanPlan(Oid idxid, List *propagated_exprs,
							   Oid array_contain_oprid, Oid array_overlap_oprid)
{
	BitmapIndexScan *idx_scan_node = makeNode(BitmapIndexScan);
	BitmapHeapScan *heap_scan_node = makeNode(BitmapHeapScan);
	Plan *idx_scan_plan = &idx_scan_node->scan.plan;
	Plan *heap_scan_plan = &heap_scan_node->scan.plan;
	List *qpqual, *keyqual, *indexqual;
	Var *target_partid_var;
	TargetEntry *tle;
	ListCell *lc;

	qpqual = keyqual = indexqual = NIL;

	foreach(lc, propagated_exprs)
	{
		Expr *oldexpr = lfirst(lc);
		Expr *newexpr = HapMakePartMapKeyExpr(oldexpr,
											  array_contain_oprid,
											  array_overlap_oprid);

		if (IsA(newexpr, BoolExpr))
			qpqual = lappend(qpqual, newexpr);
		else
			keyqual = lappend(keyqual, newexpr);
	}

	/* Change Var to fit index scan's format */
	foreach(lc, keyqual)
	{
		OpExpr *opexpr = lfirst(lc);
		OpExpr *newopexpr = copyObject(opexpr);
		Var *newvar = linitial(newopexpr->args);

		newvar->varno = INDEX_VAR;
		indexqual = lappend(indexqual, newopexpr);
	}

	/* Set index scan plan */
	idx_scan_plan->targetlist = NIL;
	idx_scan_plan->qual = NIL;
	idx_scan_plan->lefttree = NULL;
	idx_scan_plan->righttree = NULL;
	idx_scan_node->scan.scanrelid = 1;
	idx_scan_node->indexid = idxid;
	idx_scan_node->indexqual = indexqual;
	idx_scan_node->indexqualorig = keyqual;

	/* Make TargetEntry for heap scan */
	target_partid_var = makeVar(1, 2, OIDOID, -1, 0, 0);
	tle = makeTargetEntry(target_partid_var, 1, NULL, 0);

	/* Set heap scan plan */
	heap_scan_plan->targetlist = lappend(NIL, tle);
	heap_scan_plan->qual = qpqual;
	heap_scan_plan->lefttree = idx_scan_node;
	heap_scan_node->scan.scanrelid = 1;
	heap_scan_node->bitmapqualorig = keyqual;

	return heap_scan_node;
}

/*
 * HapSearchPartMapGinIdx
 *		Make bitmap scan plan about partition map and execute it.
 *
 * Transform propagated hidden attribute quals to the partition map's format.
 * For example, if the expression is opexpr, it can be transformed to the '@>'
 * operation on partition map's hidden attribute array column. If the expression
 * is arrayopexpr, it can be transformed to the '&&' operation on partitoin
 * map's hidden attribute array column.
 *
 * Then we can use these transformed expressions as a search key on GIN index of
 * partition map. Using these keys, create bitmap scan plan and execute it.
 */
static List *
HapSearchPartMapGinIdx(Oid partmapid, Oid idxid, List *propagated_exprs)
{
	String *array_contain_oprname = makeString("@>");
	String *array_overlap_oprname = makeString("&&");
	Oid array_contain_oprid, array_overlap_oprid;
	List *partid_list = NIL;
	BitmapHeapScanState *ps;
	BitmapHeapScan *plan;
	EState *estate;

	array_contain_oprid = OpernameGetOprid(lappend(NIL, array_contain_oprname),
										   INT4ARRAYOID, INT4ARRAYOID);
	array_overlap_oprid = OpernameGetOprid(lappend(NIL, array_overlap_oprname),
										   INT4ARRAYOID, INT4ARRAYOID);

	if (!array_contain_oprid || !array_overlap_oprid)
		elog(ERROR, "Install intarray extension first");

	/* Prepare execution */
	estate = HapInitLightWeightExecutor(lappend_oid(NIL, partmapid),
										lappend_int(NIL, AccessShareLock));

	/* Make a plan */
	plan = HapCreatePartMapBitmapScanPlan(idxid, propagated_exprs,
										  array_contain_oprid,
										  array_overlap_oprid);
	ps = ExecInitNode((Plan *) plan, estate, EXEC_FLAG_SKIP_TRIGGERS);

	/* Get the partition ids */
	for (;;)
	{
		TupleTableSlot *tts = ExecProcNode(ps);

		if (TupIsNull(tts))
			break;

		partid_list = lappend_oid(partid_list, *tts->tts_values);
	}

	/* Done */
	HapDoneLightWeightExecutor(estate, ps);

	return partid_list;
}

/*
 * HapGetPartIdListGin
 *		Search gin index of partition map and get partition oid list.
 *
 * Search gin index using the propagated expressions and get the correspond
 * partition oids.
 */
List *
HapGetPartIdListGin(Oid relid, List *propagated_exprs)
{
	Relation rel = table_open(relid, AccessShareLock);
	Oid partmapid, idxid;
	List *partid_list;

	if (!rel->rd_hap_partitioned)
	{
		table_close(rel, AccessShareLock);
		return NIL;
	}

	partmapid = rel->rd_hap_partmap_id;
	idxid = rel->rd_hap_partmap_gin_idx_id;

	/* Search partition oids */
	partid_list = HapSearchPartMapGinIdx(partmapid, idxid, propagated_exprs);

	/* Done */
	table_close(rel, AccessShareLock);

	return partid_list;
}

#else /* ! HAP */

/* Dummy */
Oid
HapGetPartIdHash(Relation rel, Datum *keys, int len)
{
	return InvalidOid;
}

/* Dummy */
List *
HapGetPartIdListGin(Oid relid, List *propagated_exprs)
{
	return NIL;
}

#endif /* HAP */
