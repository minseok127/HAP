/*-------------------------------------------------------------------------
 *
 * hap_lw_executor.c
 *	  Light weight internal executor for HAP.
 *
 *
 * IDENTIFICATION
 *	  src/backend/hap/access/hap_lw_executor.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/table.h"
#include "executor/tuptable.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#include "hap/hap_executor.h"
#include "hap/hap_state.h"

#ifdef HAP
/*
 * HapInitLightWeightExecutor
 *		Create an EState to use on its own in HAP.
 *
 * HAP can create and execute its own plan during execution. For example, the
 * encoding table can be read in the planning, or the dimension table can be
 * read in the partitioning.
 *
 * Make an EState for these light weight executions.
 */
EState *
HapInitLightWeightExecutor(List *relid_list, List *lockmode_list)
{
	EState *estate = CreateExecutorState();
	List *relation_list = NIL;
	List *rangeTable = NIL;
	ListCell *lc, *lc2;
	MemoryContext oldcontext;
	int i = 0;

	oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

	/* Initialize estate */
	estate->es_hapstate = HapCreateState(CurrentMemoryContext);
	estate->es_snapshot = RegisterSnapshot(GetLatestSnapshot());
	estate->es_tupleTable = NIL;
	estate->es_epq_active = NULL;

	/* Build range table */
	forboth(lc, relid_list, lc2, lockmode_list)
	{
		Oid relid = lfirst_oid(lc);
		int lockmode = lfirst_int(lc2);
		Relation rel = table_open(relid, lockmode);
		RangeTblEntry *rte = palloc0(sizeof(RangeTblEntry));

		rte->relid = RelationGetRelid(rel);
		rte->relkind = rel->rd_rel->relkind;
		rte->rellockmode = lockmode;

		rangeTable = lappend(rangeTable, rte);
		relation_list = lappend(relation_list, rel);
	}

	ExecInitRangeTable(estate, rangeTable);

	foreach(lc, relation_list)
	{
		Relation rel = lfirst(lc);

		estate->es_relations[i++] = rel;
	}

	/* Remember newly opened relations to close them */
	estate->es_hapstate->relation_list = relation_list;
	estate->es_hapstate->lockmode_list = lockmode_list;

	MemoryContextSwitchTo(oldcontext);

	return estate;
}

/*
 * HapDoneLightWeightExecutor
 *		End the light weight plan execution of HAP.
 *
 * Close the tables and free the EState.
 */
void
HapDoneLightWeightExecutor(EState *estate, PlanState *ps)
{
	ListCell *lc, *lc2;
	HapState *hapstate = estate->es_hapstate;

	ExecEndNode(ps);
	ExecResetTupleTable(estate->es_tupleTable, false);

	forboth(lc, hapstate->relation_list, lc2, hapstate->lockmode_list)
	{
		Relation rel = lfirst(lc);
		int lockmode = lfirst_int(lc2);

		table_close(rel, lockmode);
	}

	UnregisterSnapshot(estate->es_snapshot);
	FreeExecutorState(estate);
}
#else
/* Dummy */
EState *
HapInitLightWeightExecutor(List *relid_list, List *lockmode_list)
{
	return NULL;
}

/* Dummy */
void
HapDoneLightWeightExecutor(EState *estate, PlanState *ps)
{
}
#endif /* HAP */
