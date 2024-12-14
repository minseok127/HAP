/*-------------------------------------------------------------------------
 *
 * hap_partition_key.c
 *	  Partition key related implementations.
 *
 *
 * IDENTIFICATION
 *	  src/backend/hap/planner/hap_partition_key.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/table.h"
#include "catalog/namespace.h"
#include "catalog/pg_operator.h"
#include "nodes/bitmapset.h"
#include "nodes/plannodes.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

#include "hap/hap_catalog.h"
#include "hap/hap_encoding.h"
#include "hap/hap_executor.h"
#include "hap/hap_partition.h"

/*
 * HapMakePartKeySets
 *		Combine partition keys that share propagation path.
 *
 * In order to figure out the partition key values that actually exist, we need
 * to figure out how the partition keys that share the propagattion path are
 * formed.
 *
 * For partition keys that share the same path, we can get the values that
 * actually exist by scanning the table which has the largest number of hidden
 * attributes.
 *
 * Note that having more hidden attributes means shorter propagation path.
 */
static void
HapMakePartKeySets(HapPartCreateStmt *stmt)
{
	List *partkey_list = stmt->partkey_list;
	Bitmapset *partkeys = NULL;
	int i = -1, set_idx = 0;

	/* Form partition key combinations */
	partkeys = bms_add_range(partkeys, 0, list_length(partkey_list) - 1);
	while ((i = bms_next_member(partkeys, i)) >= 0)
	{
		HapPartKeySet *set = palloc0(sizeof(HapPartKeySet));
		HapPartKey *partkey = list_nth(partkey_list, i);
		int j = i, key_idx = 0;

		set->partkey_list = lappend(set->partkey_list, partkey);
		set->scan_dimension_relid = partkey->dimension_relid;

		partkey->set_idx = set_idx;
		partkey->key_idx = key_idx;

		/* See the next partkeys */
		while ((j = bms_next_member(partkeys, j)) >= 0)
		{
			HapPartKey *partkey2 = list_nth(partkey_list, j);
			int nmatched = 0, len1, len2;
			ListCell *lc;
			
			/* Check whether the propagation path is shared or not */
			foreach(lc, partkey->propagate_oid_list)
			{
				Oid relid = lfirst_oid(lc);

				if (list_member_oid(partkey2->propagate_oid_list, relid))
					nmatched++;
			}

			len1 = list_length(partkey->propagate_oid_list);
			len2 = list_length(partkey2->propagate_oid_list);

			/* Both partition keys share the same path */
			if (len1 == nmatched || len2 == nmatched)
			{
				set->partkey_list = lappend(set->partkey_list, partkey2);

				partkey2->set_idx = set_idx;
				partkey2->key_idx = ++key_idx;

				/* partkey2 has more hidden attribute, scan it */
				if (len1 > len2)
					set->scan_dimension_relid = partkey2->dimension_relid;

				/* Key cannot exists in multiple combinations, so hide it */
				bms_del_member(partkeys, j);
			}
		}

		stmt->partkey_set_list = lappend(stmt->partkey_set_list, set);
		set_idx++;
	}
}

/*
 * HapGetHiddenAttrDescListOfPartKeySet
 *		Get the hidden attribute descriptor list for the scanning dimension
 *		table of the partition key set.
 *
 * Each of the partition keys belonging to the partition key set represents an
 * encoded dimension attribute. Make these into hidden attribute descriptors
 * corresponds to the scanning table (scan_dimension_relid).
 *
 * By interpreting the hidden attribute of scan_dimension_relid using these
 * descriptors, we can find out the actual values of the partition keys.
 */
static List *
HapGetHiddenAttrDescListOfPartKeySet(HapPartKeySet *set)
{
	List *hidden_attribute_desc_list = NIL;
	ListCell *lc;

	foreach(lc, set->partkey_list)
	{
		HapPartKey *partkey = lfirst(lc);
		HapHiddenAttrDesc *desc;

		/*
		 * If the dimension table representing this partition key is not the
		 * target to read for this partition key set, we must express the hidden
		 * attribute indicated by the partition key as the hidden attribute of
		 * the table to be read.
		 */
		if (partkey->dimension_relid != set->scan_dimension_relid)
		{
			List *tmp_oid_list = NIL;
			ListCell *lc2;
			int16 descid;

			foreach(lc2, partkey->propagate_oid_list)
			{
				Oid relid = lfirst_oid(lc2);

				tmp_oid_list = lappend_oid(tmp_oid_list, relid);

				if (relid == set->scan_dimension_relid)
					break;
			}

			descid = HapGetDescendantHiddenAttrDescid(tmp_oid_list,
													  partkey->dimension_descid,
													  false);
			desc = HapMakeHiddenAttrDesc(set->scan_dimension_relid, descid);
		}
		else
		{
			/* Just use descriptor id as is */
			desc = HapMakeHiddenAttrDesc(partkey->dimension_relid,
										 partkey->dimension_descid);
		}

		hidden_attribute_desc_list = lappend(hidden_attribute_desc_list, desc);
	}

	return hidden_attribute_desc_list;
}

/*
 * HapCreatePartKeySetSeqScanPlan
 *		Create sequential sacn plan for the dimension table.
 *
 * Create a sequential scan plan that reads the hidden attribute of the target
 * dimension table and extracts the encoding value via the
 * hap_extract_encoding_value().
 *
 * Note that hap_extract_encoding_value()'s oid is 4552. See the pg_proc.dat
 */
static SeqScan *
HapCreatePartKeySetSeqScanPlan(HapPartKeySet *set,
							   List *hidden_attribute_desc_list)
{
	SeqScan *seqscan = makeNode(SeqScan);
	int resno = 1, ressortgroupref = 1;
	List *tlist = NIL;
	ListCell *lc;

	/* Make a target list (SELECT) */
	foreach(lc, hidden_attribute_desc_list)
	{
		HapHiddenAttrDesc *desc = lfirst(lc);
		TargetEntry *t = makeNode(TargetEntry);
		FuncExpr *funcexpr = makeNode(FuncExpr);
		Var *arg1 = makeNode(Var);
		Const *arg2 = makeNode(Const);
		Const *arg3 = makeNode(Const);

		/* Function expression representing hap_extract_encoding_value() */
		funcexpr->funcid = 4552;
		funcexpr->funcresulttype = INT4OID;
		funcexpr->location = -1;
		funcexpr->args = NIL;

		/* scan_dimension_relation._hap_hidden_attribute */
		arg1->varno = 1;
		arg1->varattno = get_attnum(set->scan_dimension_relid,
								    "_hap_hidden_attribute");
		arg1->vartype = BYTEAOID;
		arg1->vartypmod = -1;
		arg1->varlevelsup = 0;
		arg1->varnosyn = 1;
		arg1->varattnosyn = 1;
		arg1->location = -1;

		/* startbit */
		arg2->consttype = INT4OID;
		arg2->consttypmod = -1;
		arg2->constcollid = 0;
		arg2->constlen = 4;
		arg2->constvalue = Int32GetDatum(desc->startbit);
		arg2->constisnull = false;
		arg2->constbyval = true;
		arg2->location = -1;

		/* bitsize */
		arg3->consttype = INT4OID;
		arg3->consttypmod = -1;
		arg3->constcollid = 0;
		arg3->constlen = 4;
		arg3->constvalue = Int32GetDatum(desc->bitsize);
		arg3->constisnull = false;
		arg3->constbyval = true;
		arg3->location = -1;

		/* Add the arguments to the function (hap_extract_encoding_value) */
		funcexpr->args = lappend(funcexpr->args, arg1);
		funcexpr->args = lappend(funcexpr->args, arg2);
		funcexpr->args = lappend(funcexpr->args, arg3);

		/* Set target entry */
		t->expr = funcexpr;
		t->resno = resno++;
		t->ressortgroupref = ressortgroupref++;

		tlist = lappend(tlist, t);
	}

	seqscan->scan.plan.targetlist = tlist;
	seqscan->scan.scanrelid = 1;

	return seqscan;
}

/*
 * HapCreateGroupSortTargetList
 *		Create target list for GROUP BY / ORDER BY plans
 *
 * Group and sort plans have target entries as Var and these vars points scan
 * node's target entries.
 */
static List *
HapCreateGroupSortTargetList(int len)
{
	int resno = 1, ressortgroupref = 1, varattno = 1;
	List *tlist = NIL;

	for (int i = 0; i < len; i++)
	{
		TargetEntry *t = makeNode(TargetEntry);
		Var *var = makeNode(Var);

		/* Set Var */
		var->varno = -2;
		var->varattno = varattno++;
		var->vartype = INT4OID;
		var->vartypmod = -1;
		var->location = -1;

		/* Set target entry */
		t->expr = var;
		t->resno = resno++;
		t->ressortgroupref = ressortgroupref++;

		tlist = lappend(tlist, t);
	}

	return tlist;
}

/*
 * HapCreatePartKeySetSortPlan
 *		Create ORDER BY plan.
 *
 * Create sort plan. It sorts integer values that are the result of scan node.
 */
static Sort *
HapCreatePartKeySetSortPlan(HapPartKeySet *set, SeqScan *seqscan)
{
	int len = list_length(set->partkey_list);
	Sort *sort = makeNode(Sort);

	sort->plan.targetlist = HapCreateGroupSortTargetList(len);
	sort->plan.lefttree = seqscan;
	sort->numCols = len;
	sort->sortColIdx = palloc0(len * sizeof(AttrNumber));
	sort->sortOperators = palloc0(len * sizeof(Oid));
	sort->collations = palloc0(len * sizeof(Oid));
	sort->nullsFirst = palloc0(len * sizeof(bool));

	for (int i = 0; i < len; i++)
	{
		sort->sortColIdx[i] = i + 1;
		sort->sortOperators[i] = Int4LessOperator;
	}

	return sort;
}

/*
 * HapCreatePartKeySetGroupPlan
 *		Create GROUP BY plan.
 *
 * Create group plan. It groups integer values that are the result of sort node.
 */
static Group *
HapCreatePartKeySetGroupPlan(HapPartKeySet *set, Sort *sort)
{
	int len = list_length(set->partkey_list);
	Group *group = makeNode(Group);

	group->plan.targetlist = HapCreateGroupSortTargetList(len);
	group->plan.lefttree = sort;
	group->numCols = len;
	group->grpColIdx = palloc0(len * sizeof(AttrNumber));
	group->grpOperators = palloc0(len * sizeof(Oid));
	group->grpCollations = palloc0(len * sizeof(Oid));

	for (int i = 0; i < len; i++)
	{
		group->grpColIdx[i] = i + 1;
		group->grpOperators[i] = Int4EqualOperator;
	}

	return group;
}

/*
 * HapCreateFindingPartKeySetValuesPlan
 *		Create a plan which finds valid hidden attribute encoding value
 *		of the partition key set.
 *
 * This function creates the following plan:
 *
 * GroupBy (GROUP BY col1, col2 ...) <- root
 *   |
 * Sort (ORDER BY col1, col2, ...)
 *   |
 * SeqScan (SELECT hap_extract_encoding_value(...) ... FROM dimension table)
 *
 * This plan corresponds to the query that appears in the comments of
 * HapFindPartKeySetValues().
 */
static Plan *
HapCreateFindingPartKeySetValuesPlan(HapPartKeySet *set,
									 List *hidden_attribute_desc_list)
{
	SeqScan *seqscan = HapCreatePartKeySetSeqScanPlan(set,
													  hidden_attribute_desc_list);

	Sort *sort = HapCreatePartKeySetSortPlan(set, seqscan);

	Group *group = HapCreatePartKeySetGroupPlan(set, sort);

	return group;
}

/*
 * HapFindPartKeySetValues
 *		Get the actual values of the hidden attributes that the given
 *		partition key set can have.
 *
 * This function is equivalent to executing a query like this:
 *
 * SELECT hap_extract_encoding_value(_hap_hidden_attribute, ...) as col1, ...
 * FROM dimension table (cf. it relid is scan_dimension_relid)
 * GROUP BY col1, ...
 * ORDER BY col1, ...
 *
 * Each columns corresponds to the each encoding values extracted from the
 * hidden attribute by combination's hidden attribute descriptors. The result
 * will show us possible hidden attribute value combinations.
 */
static void
HapFindPartKeySetValues(HapPartKeySet *set)
{
	Oid relid = set->scan_dimension_relid;
	List *desc_list, *value_set;
	RangeTblEntry *rte;
	EState *estate;
	PlanState *ps;
	Plan *plan;
	int len;

	/* Get the proper hidden attribute descriptors */
	desc_list = HapGetHiddenAttrDescListOfPartKeySet(set);
	len = list_length(desc_list);
	
	/* Prepare execution */
	estate = HapInitLightWeightExecutor(lappend_oid(NIL, relid),
										lappend_int(NIL, AccessShareLock));

	/* Make a plan */
	plan = HapCreateFindingPartKeySetValuesPlan(set, desc_list);
	ps = ExecInitNode(plan, estate, EXEC_FLAG_SKIP_TRIGGERS);

	/* Get the values */
	for (;;)
	{
		TupleTableSlot *tts = ExecProcNode(ps);

		if (TupIsNull(tts))
			break;

		value_set = NIL;

		for (int i = 0; i < len; i++)
			value_set = lappend_int(value_set, tts->tts_values[i]);

		set->value_set_list = lappend(set->value_set_list, value_set);
	}

	/* Done */
	HapDoneLightWeightExecutor(estate, ps);
}

/*
 * HapFindPartitionKeyValues
 *		Find possible partition key values.
 *
 * If we form partition keys by simply multiplying the cardinalities of each
 * partition key, even non-existent values can be used as partition keys.
 *
 * To avoid this, read the dimension table directly and filter only those values
 * that actually exists. 
 */
void
HapFindPartKeyValues(HapPartCreateStmt *stmt)
{
	ListCell *lc;

	/* Make combinations of keys that share a propagation path */
	HapMakePartKeySets(stmt);

	/*
	 * For each partition key set, read the dimension table and figure out the
	 * possible set of partition key values.
	 */
	foreach(lc, stmt->partkey_set_list)
	{
		HapPartKeySet *set = lfirst(lc);
		ListCell *lc2;

		HapFindPartKeySetValues(set);
		set->value_set_count = list_length(set->value_set_list);
		set->value_set_idx = 0;
		stmt->total_value_set_count *= set->value_set_count;
	}
}

/*
 * HapGetNextPartKeyValues
 *		Write next partition key values to the @arr.
 *
 * Each partition set has an iterator. Using this, find the values of the
 * partition key to be obtained next and write them to the @arr.
 */
void
HapGetNextPartKeyValues(HapPartCreateStmt *stmt, Datum *arr)
{
	int set_cnt = list_length(stmt->partkey_set_list);
	int key_cnt = list_length(stmt->partkey_list);
	bool cascade = true;

	/* Save partkey values to the arr */
	for (int i = 0; i < key_cnt; i++)
	{
		HapPartKey *key = list_nth(stmt->partkey_list, i);
		HapPartKeySet *set = list_nth(stmt->partkey_set_list, key->set_idx);
		List *value_set = list_nth(set->value_set_list, set->value_set_idx);
		arr[i] = Int32GetDatum(list_nth_int(value_set, key->key_idx));
	}

	/* Set value index */
	for (int i = set_cnt - 1; i >= 0; i--)
	{
		HapPartKeySet *set = list_nth(stmt->partkey_set_list, i);

		if (cascade && (++set->value_set_idx == set->value_set_count))
		{
			/* Turn back to the start index */
			set->value_set_idx = 0;
			continue;
		}

		cascade = false;
	}
}
