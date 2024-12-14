/*-------------------------------------------------------------------------
 *
 * hap_encoding_table.c
 *	  Encode table access functions for HAP
 *
 *
 * IDENTIFICATION
 *	  src/backend/hap/planner/hap_encoding_table.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/table.h"
#include "access/tableam.h"
#include "executor/executor.h"
#include "executor/nodeSeqscan.h"
#include "executor/tuptable.h"
#include "nodes/execnodes.h"
#include "nodes/makefuncs.h"
#include "nodes/primnodes.h"
#include "optimizer/optimizer.h"
#include "utils/snapmgr.h"
#ifdef HAP_REDUCTION
#include "nodes/print.h"
#include "access/tupmacs.h"
#endif  /* HAP_REDUCTION */

#include "hap/hap_executor.h"
#include "hap/hap_planner.h"
#ifdef HAP_REDUCTION
#include "hap/hap_reduction.h"

/* Does att's datatype allow packing into the 1-byte-header varlena format? */
#define ATT_IS_PACKABLE(att) \
	((att)->attlen == -1 && (att)->attstorage != TYPSTORAGE_PLAIN)
/* Use this if it's already known varlena */
#define VARLENA_ATT_IS_PACKABLE(att) \
	((att)->attstorage != TYPSTORAGE_PLAIN)
#endif  /* HAP_REDUCTION */

/*
 * hap_make_encoding_table_seqscan_plan
 *		Make SeqScan plan node.
 *
 * Modify the given expr according to the schema of the encode mapping table,
 * and use it to create a SeqScan plan node.
 *
 * Note that encode mapping table has two attribute. The first is encoded
 * attribute's original values, and the second is encoding values.
 */
static SeqScan *
hap_make_encoding_table_seqscan_plan(Expr *expr)
{
	Var *encoding_value_var, *encoded_attribute_var;
	Expr *encode_table_expr;
	SeqScan *scan_plan;
	List *tlist = NIL;
	TargetEntry *tle;
	List *varlist;

	/* Make Var to represent encoding value */
	encoding_value_var = makeNode(Var);
	encoding_value_var->varno = 1;
	encoding_value_var->varattno = 2;
	encoding_value_var->vartype = INT8OID;
	encoding_value_var->vartypmod = -1;
	encoding_value_var->varcollid = 0;
	encoding_value_var->varlevelsup = 0;
	encoding_value_var->varnosyn = 1;
	encoding_value_var->varattnosyn = 2;
	encoding_value_var->location = -1;

	tle = makeTargetEntry((Expr *) encoding_value_var, 1, NULL, false);
	tlist = lappend(tlist, tle);

	/* Change the original expression to the encode table's format */
	encode_table_expr = copyObject(expr);
	varlist = pull_vars_of_level((Node *)encode_table_expr, 0);
	encoded_attribute_var = linitial(varlist);
	encoded_attribute_var->varno = 1;
	encoded_attribute_var->varattno = 1;

	/* Make a plan */
	scan_plan = makeNode(SeqScan);
	scan_plan->scan.plan.targetlist = tlist;
	scan_plan->scan.plan.qual = lappend(NIL, encode_table_expr);
	scan_plan->scan.plan.lefttree = NULL;
	scan_plan->scan.plan.righttree = NULL;
	scan_plan->scan.scanrelid = 1;

	return scan_plan;
}

/*
 * hap_get_hidden_attribute_encoding_values
 *		Get encoding values from encode mapping table.
 *
 * Obtain encoding values that satisfy the given expr by sequentially scanning
 * the encode mapping table.
 */
List *
hap_get_hidden_attribute_encoding_values(Oid encode_table_id, Expr *expr)
{
	List *encoding_value_list = NIL;
	SeqScanState *ps;
	SeqScan *seqscan;
	EState *estate;

	/* Prepare execution */
	estate = HapInitLightWeightExecutor(lappend_oid(NIL, encode_table_id),
										lappend_int(NIL, AccessShareLock));

	/* Make a plan */
	seqscan = hap_make_encoding_table_seqscan_plan(expr);
	ps = ExecInitNode((Plan *) seqscan, estate, EXEC_FLAG_SKIP_TRIGGERS);

	/* Get the proper encoding values */
	for (;;)
	{
		TupleTableSlot *tts = ExecProcNode(ps);

		if (TupIsNull(tts))
			break;

		encoding_value_list = lappend_int(encoding_value_list,
										  *tts->tts_values);
	}
	
	/* Done */
	HapDoneLightWeightExecutor(estate, ps);
	
	return encoding_value_list;
}

#ifdef HAP_REDUCTION
static SeqScan *
hap_make_encoding_table_seqscan_plan_for_substitution(HapSubstitutionInfo *info)
{
	Var *encoding_value_var, *encoded_attribute_var;
	SeqScan *scan_plan;
	List *tlist = NIL;
	TargetEntry *tle;

	/* Make Var to represent encoding value */
	encoding_value_var = makeNode(Var);
	encoding_value_var->varno = 1;
	encoding_value_var->varattno = 2;
	encoding_value_var->vartype = INT8OID;
	encoding_value_var->vartypmod = -1;
	encoding_value_var->varcollid = 0;
	encoding_value_var->varlevelsup = 0;
	encoding_value_var->varnosyn = 1;
	encoding_value_var->varattnosyn = 2;
	encoding_value_var->location = -1;

	tle = makeTargetEntry((Expr *) encoding_value_var, 1, NULL, false);
	tlist = lappend(tlist, tle);

	/* Make Var to represent encoded attribute */
	encoded_attribute_var = makeNode(Var);
	encoded_attribute_var->varno = 1;
	encoded_attribute_var->varattno = 1;
	encoded_attribute_var->vartype = info->origin_vartype;
	encoded_attribute_var->vartypmod = -1;
	encoded_attribute_var->varcollid = info->origin_varcollid;
	encoded_attribute_var->varlevelsup = 0;
	encoded_attribute_var->varnosyn = 1;
	encoded_attribute_var->varattnosyn = 1;
	encoded_attribute_var->location = -1;

	tle = makeTargetEntry((Expr *) encoded_attribute_var, 2, NULL, false);
	tlist = lappend(tlist, tle);
	
	/* Make a plan */
	scan_plan = makeNode(SeqScan);
	scan_plan->scan.plan.targetlist = tlist;
	scan_plan->scan.plan.qual = NULL;
	scan_plan->scan.plan.lefttree = NULL;
	scan_plan->scan.plan.righttree = NULL;
	scan_plan->scan.scanrelid = 1;

	Assert(scan_plan);

	return scan_plan;
}

/*
 * GetAttributeSize
 */
static int 
GetAttributeSize(TupleDesc tupleDesc, Datum *values, bool *isnull, int idx)

{
	Datum val;
	Form_pg_attribute atti;
	int data_length = 0;

	val = values[idx];
	atti = TupleDescAttr(tupleDesc, idx);

	if (ATT_IS_PACKABLE(atti) &&
		VARATT_CAN_MAKE_SHORT(DatumGetPointer(val)))
	{
		/*
		 * we're anticipating converting to a short varlena header, so
		 * adjust length and don't count any alignment
		 */
		data_length += VARATT_CONVERTED_SHORT_SIZE(DatumGetPointer(val));
	}
	else if (atti->attlen == -1 &&
			 VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(val)))
	{
		/*
		 * we want to flatten the expanded value so that the constructed
		 * tuple doesn't depend on it
		 */
		data_length = att_align_nominal(data_length, atti->attalign);
		data_length += EOH_get_flat_size(DatumGetEOHP(val));
	}
	else
	{
		data_length = att_align_datum(data_length, atti->attalign,
									  atti->attlen, val);
		data_length = att_addlength_datum(data_length, atti->attlen,
									  val);
	}

	return data_length;
}


/*
 * hap_get_encoding_table_info
 *		Get max size of original value and tuple numbers.
 */
static void
hap_get_encoding_table_info(HapSubstitutionInfo *info, int *tuple_nums, 
														int *max_length)
{
	SeqScanState *ps;
	SeqScan *seqscan;
	EState *estate;
	
	*tuple_nums = 0;
	*max_length = 0;

	/* Prepare execution */
	estate = 
		HapInitLightWeightExecutor(lappend_oid(NIL, info->encoding_table_oid),
									lappend_int(NIL, AccessShareLock));

	/* Make a plan */
	seqscan = hap_make_encoding_table_seqscan_plan_for_substitution(info);
	ps = ExecInitNode((Plan *) seqscan, estate, EXEC_FLAG_SKIP_TRIGGERS);

	/* Get the proper encoding values */
	for (;;)
	{
		int original_value_length;
		TupleTableSlot *tts = ExecProcNode((PlanState *) ps);
		TupleDesc tupleDesc = tts->tts_tupleDescriptor;

		if (TupIsNull(tts))
			break;

		/* We get original value size. */
		original_value_length = 
			GetAttributeSize(tupleDesc, tts->tts_values, tts->tts_isnull, 1);

		*tuple_nums += 1;
		if (*max_length < original_value_length)
			*max_length = original_value_length;
	}
	
	/* Done */
	HapDoneLightWeightExecutor(estate, ps);
}

/*
 * hap_populate_for_substitution
 *		Populate data array using substitution info's encoding table oid.
 */
void
hap_populate_for_substitution(HapSubstitutionInfo *info, char **data_store, 
																int *data_size)
{
	SeqScanState *ps;
	SeqScan *seqscan;
	EState *estate;
	int tuple_nums;
	int max_length;
	MemoryContext oldcxt = MemoryContextSwitchTo(TopMemoryContext);

	/* Get tuple nums and max data length, scanning the table. */
	hap_get_encoding_table_info(info, &tuple_nums, &max_length);

	*data_store = palloc0(max_length * tuple_nums);
	*data_size = max_length;

	/* Prepare execution */
	estate = 
		HapInitLightWeightExecutor(lappend_oid(NIL, info->encoding_table_oid),
									lappend_int(NIL, AccessShareLock));

	/* Make a plan */
	seqscan = hap_make_encoding_table_seqscan_plan_for_substitution(info);
	ps = ExecInitNode((Plan *) seqscan, estate, EXEC_FLAG_SKIP_TRIGGERS);

	/* Get the proper encoding values */
	for (;;)
	{
		int64_t encoding_value;
		int original_value_length;
		TupleTableSlot *tts = ExecProcNode(ps);
		TupleDesc tupleDesc = tts->tts_tupleDescriptor;

		if (TupIsNull(tts))
			break;

		/* We get encoding value by val. */
		encoding_value = tts->tts_values[0];

		/* We get original value size for copy. */
		original_value_length = 
			GetAttributeSize(tupleDesc, tts->tts_values, tts->tts_isnull, 1);

		/* Memory copy original data to the data store. */
		memcpy((((char *) *data_store) + (encoding_value) * max_length), 
													(void *) tts->tts_values[1], 
													original_value_length);	
	}
	
	/* Done */
	HapDoneLightWeightExecutor(estate, ps);

	MemoryContextSwitchTo(oldcxt);
}
#endif /* HAP_REDUCTION */
