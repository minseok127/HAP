/*-------------------------------------------------------------------------
 *
 * hap_planner.h
 *	  POSTGRES hidden attribute partitioning (HAP) planner.
 *
 *
 * src/include/hap/hap_planner.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef HAP_PLANNER_H
#define HAP_PLANNER_H

#include "nodes/nodes.h"
#include "nodes/pathnodes.h"
#include "nodes/pg_list.h"

#include "hap/hap_encoding.h"
#ifdef HAP_REDUCTION
#include "hap/hap_reduction.h"
#endif

/*
 * HapPropagatePath - The path of hidden attribute propagation.
 *
 * - rel_oid_list: Relation oids passed through in the propagation.
 * - target_rel_idx: Target RelOptInfo index to be propagated to.
 */
typedef struct HapPropagatePath
{
	List *rel_oid_list;
	Index target_rel_idx;
} HapPropagatePath;

/*
 * HapHiddenAttrOpExpr - "=" operation for hidden attribute.
 *
 * - expr: Expression node for an operator invocation.
 * - desc: Hidden attribute descriptor. 
 * - value: Encoding value.
 */
typedef struct HapHiddenAttrOpExpr
{
	OpExpr expr;
	HapHiddenAttrDesc *desc;
	int value;
	Selectivity selec;
} HapHiddenAttrOpExpr;

/*
 * HapHiddenAttrScalarArrayOpExpr - "IN (array)" operation for hidden attribute.
 *
 * - expr: Expression node for "scalar op ANY/ALL (array)".
 * - desc: Hidden attribute descriptor.
 * - value_list: Encoding value list.
 */
typedef struct HapHiddenAttrScalarArrayOpExpr
{
	ScalarArrayOpExpr expr;
	HapHiddenAttrDesc *desc;
	List *value_list;
	Selectivity selec;
} HapHiddenAttrScalarArrayOpExpr;

/*
 * HapHiddenAttrBoolExpr - "AND/OR" operation for hidden attribute.
 *
 * - expr: Expression node for the basic Boolean operators AND, OR, NOT.
 * - args: Arguments to this expression.
 */
typedef struct HapHiddenAttrBoolExpr
{
	BoolExpr expr;
	List *args;
	Selectivity selec;
} HapHiddenAttrBoolExpr;

extern bool hap_check_dimension_table_existence(PlannerInfo *root);

extern bool hap_propagate_hidden_attribute(PlannerInfo *root);

extern List *hap_get_hidden_attribute_encoding_values(Oid encode_table_id,
													  Expr *expr);

#ifdef HAP_REDUCTION
extern void hap_populate_for_substitution(HapSubstitutionInfo *info, 
											char **data_store, int *data_size);
#endif

extern PGDLLIMPORT bool enable_hap_planner;

extern PGDLLIMPORT bool enable_hap_selectivity;

#endif	/* HAP_PLANNER_H */
