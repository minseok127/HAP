/*-------------------------------------------------------------------------
 *
 * hap_selectivity.c
 *	  Selectivity of hidden attribute quals
 *
 *
 * IDENTIFICATION
 *	  src/backend/hap/planner/hap_selectivity.c
 *-------------------------------------------------------------------------
 */
#ifdef HAP
#include "postgres.h"

#include "hap/hap_hook.h"
#include "hap/hap_planner.h"

/*
 * Condition to hook the clause_selectivity_ext().
 */
HAP_DEFINE_HOOK_COND6(Selectivity, clause_selectivity_ext,
					  PlannerInfo *, root,
					  Node *, clause,
					  int, varRelid,
					  JoinType, jointype,
					  SpecialJoinInfo *, sjinfo,
					  bool, use_extended_stats)
{
	RestrictInfo *rinfo = NULL;

	if (IsA(clause, RestrictInfo))
	{
		rinfo = (RestrictInfo *) clause;
		return rinfo->hap_info_is_hidden_attribute;
	}

	return false;
}

/*
 * Hook on clause_selectivity_ext().
 */
HAP_DEFINE_HOOK_BODY6(Selectivity, clause_selectivity_ext,
					  PlannerInfo *, root,
					  Node *, clause,
					  int, varRelid,
					  JoinType, jointype,
					  SpecialJoinInfo *, sjinfo,
					  bool, use_extended_stats)
{
	RestrictInfo *rinfo = clause;

	/*
	 * Currently enable_hap_selectivity is false by default. This is because the
	 * selectivity of the hidden attribute can be seen as reflecting the join
	 * result in advance, but the optimizer thinks it independently and
	 * underestimates the cost of the nestd loop join.
	 *
	 * This may change if further consideration is given to more accureate
	 * measurements.
	 */
	if (!enable_hap_selectivity)
		return 1.0;

	return rinfo->hap_hidden_attribute_selectivity;
}

#endif /* HAP */
