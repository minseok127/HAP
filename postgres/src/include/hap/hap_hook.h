#ifndef HAP_HOOK_H
#define HAP_HOOK_H
#ifdef HAP
#include "postgres.h"

#include "catalog/objectaddress.h"
#include "executor/tuptable.h"
#include "nodes/execnodes.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pathnodes.h"
#include "nodes/pg_list.h"
#include "optimizer/planmain.h"

/*
 * Usage -
 *
 * If you want to hook:
 * 		int test(int a, int b)
 *		{
 *			return a - b;
 *		}
 *
 * Declare hook functions in this header file like this:
 *		HAP_DECLARE_HOOK2(int, test, int, a, int, b);
 * 		=> HOOK2 means the original function uses two arguments.
 *
 * Then define COND function and BODY function somewhere:
 *		HAP_DEFINE_HOOK_COND2(int, test, int, a, int, b)
 *		{
 *			return (a == b);
 *		}
 *		=> COND function must return boolean.
 *		=> In this case, we want to hook when (a == b).
 *
 * 		HAP_DEFINE_HOOK_BODY2(int, test, int, a, int, b)
 *		{
 *			return a + b;
 *		}
 *		=> Hook function to be executed instead of test().
 *
 * Replace the header of the original function like this:
 *		#ifdef HAP
 *		HAP_HOOK2(int, test, int, a, int, b)
 *		#else
 *		int test(int a, int b)
 *		#endif
 *		=> do not touch the implementation.
 *
 * According to this example, the hook is executed only when "a" and "b"
 * received as arguments are the same (COND function).
 *
 * That is, if the caller gives the same value for both arguments,
 * the hook will be executed. Otherwise the original test() will be executed.
 */

#define HAP_MAP0(m, ...)
#define HAP_MAP1(m, t, a, ...) m(t, a)
#define HAP_MAP2(m, t, a, ...) m(t, a), HAP_MAP1(m, __VA_ARGS__)
#define HAP_MAP3(m, t, a, ...) m(t, a), HAP_MAP2(m, __VA_ARGS__)
#define HAP_MAP4(m, t, a, ...) m(t, a), HAP_MAP3(m, __VA_ARGS__)
#define HAP_MAP5(m, t, a, ...) m(t, a), HAP_MAP4(m, __VA_ARGS__)
#define HAP_MAP6(m, t, a, ...) m(t, a), HAP_MAP5(m, __VA_ARGS__)
#define HAP_MAP7(m, t, a, ...) m(t, a), HAP_MAP6(m, __VA_ARGS__)
#define HAP_MAP8(m, t, a, ...) m(t, a), HAP_MAP7(m, __VA_ARGS__)
#define HAP_MAP9(m, t, a, ...) m(t, a), HAP_MAP8(m, __VA_ARGS__)
#define HAP_MAP10(m, t, a, ...) m(t, a), HAP_MAP9(m, __VA_ARGS__)
#define HAP_MAP(n, ...) HAP_MAP##n(__VA_ARGS__)

#define HAP_FUNC_ARG(argtype, argname) argtype argname
#define HAP_ARG_VAL(argtype, argname) argname

#define HAP_HOOK_CONDn(n, funcname, ...) \
	bool _HAP_HOOK_COND_##funcname(HAP_MAP(n, HAP_FUNC_ARG, __VA_ARGS__))
#define HAP_HOOK_BODYn(n, rettype, funcname, ...) \
	rettype _HAP_HOOK_BODY_##funcname(HAP_MAP(n, HAP_FUNC_ARG, __VA_ARGS__))

#define HAP_DECLARE_HOOKn(n, rettype, funcname, ...) \
	HAP_HOOK_CONDn(n, funcname, __VA_ARGS__); \
	HAP_HOOK_BODYn(n, rettype, funcname, __VA_ARGS__);

#define HAP_DECLARE_HOOK0(rettype, funcname, ...) \
	HAP_DECLARE_HOOKn(0, rettype, funcname, __VA_ARGS__)
#define HAP_DECLARE_HOOK1(rettype, funcname, ...) \
	HAP_DECLARE_HOOKn(1, rettype, funcname, __VA_ARGS__)
#define HAP_DECLARE_HOOK2(rettype, funcname, ...) \
	HAP_DECLARE_HOOKn(2, rettype, funcname, __VA_ARGS__)
#define HAP_DECLARE_HOOK3(rettype, funcname, ...) \
	HAP_DECLARE_HOOKn(3, rettype, funcname, __VA_ARGS__)
#define HAP_DECLARE_HOOK4(rettype, funcname, ...) \
	HAP_DECLARE_HOOKn(4, rettype, funcname, __VA_ARGS__)
#define HAP_DECLARE_HOOK5(rettype, funcname, ...) \
	HAP_DECLARE_HOOKn(5, rettype, funcname, __VA_ARGS__)
#define HAP_DECLARE_HOOK6(rettype, funcname, ...) \
	HAP_DECLARE_HOOKn(6, rettype, funcname, __VA_ARGS__)
#define HAP_DECLARE_HOOK7(rettype, funcname, ...) \
	HAP_DECLARE_HOOKn(7, rettype, funcname, __VA_ARGS__)
#define HAP_DECLARE_HOOK8(rettype, funcname, ...) \
	HAP_DECLARE_HOOKn(8, rettype, funcname, __VA_ARGS__)
#define HAP_DECLARE_HOOK9(rettype, funcname, ...) \
	HAP_DECLARE_HOOKn(9, rettype, funcname, __VA_ARGS__)
#define HAP_DECLARE_HOOK10(rettype, funcname, ...) \
	HAP_DECLARE_HOOKn(10, rettype, funcname, __VA_ARGS__)

#define HAP_DEFINE_HOOK_COND0(rettype, funcname, ...) \
	HAP_HOOK_CONDn(0, funcname, __VA_ARGS__)
#define HAP_DEFINE_HOOK_COND1(rettype, funcname, ...) \
	HAP_HOOK_CONDn(1, funcname, __VA_ARGS__)
#define HAP_DEFINE_HOOK_COND2(rettype, funcname, ...) \
	HAP_HOOK_CONDn(2, funcname, __VA_ARGS__)
#define HAP_DEFINE_HOOK_COND3(rettype, funcname, ...) \
	HAP_HOOK_CONDn(3, funcname, __VA_ARGS__)
#define HAP_DEFINE_HOOK_COND4(rettype, funcname, ...) \
	HAP_HOOK_CONDn(4, funcname, __VA_ARGS__)
#define HAP_DEFINE_HOOK_COND5(rettype, funcname, ...) \
	HAP_HOOK_CONDn(5, funcname, __VA_ARGS__)
#define HAP_DEFINE_HOOK_COND6(rettype, funcname, ...) \
	HAP_HOOK_CONDn(6, funcname, __VA_ARGS__)
#define HAP_DEFINE_HOOK_COND7(rettype, funcname, ...) \
	HAP_HOOK_CONDn(7, funcname, __VA_ARGS__)
#define HAP_DEFINE_HOOK_COND8(rettype, funcname, ...) \
	HAP_HOOK_CONDn(8, funcname, __VA_ARGS__)
#define HAP_DEFINE_HOOK_COND9(rettype, funcname, ...) \
	HAP_HOOK_CONDn(9, funcname, __VA_ARGS__)
#define HAP_DEFINE_HOOK_COND10(rettype, funcname, ...) \
	HAP_HOOK_CONDn(10, funcname, __VA_ARGS__)

#define HAP_DEFINE_HOOK_BODY0(rettype, funcname, ...) \
	HAP_HOOK_BODYn(0, rettype, funcname, __VA_ARGS__)
#define HAP_DEFINE_HOOK_BODY1(rettype, funcname, ...) \
	HAP_HOOK_BODYn(1, rettype, funcname, __VA_ARGS__)
#define HAP_DEFINE_HOOK_BODY2(rettype, funcname, ...) \
	HAP_HOOK_BODYn(2, rettype, funcname, __VA_ARGS__)
#define HAP_DEFINE_HOOK_BODY3(rettype, funcname, ...) \
	HAP_HOOK_BODYn(3, rettype, funcname, __VA_ARGS__)
#define HAP_DEFINE_HOOK_BODY4(rettype, funcname, ...) \
	HAP_HOOK_BODYn(4, rettype, funcname, __VA_ARGS__)
#define HAP_DEFINE_HOOK_BODY5(rettype, funcname, ...) \
	HAP_HOOK_BODYn(5, rettype, funcname, __VA_ARGS__)
#define HAP_DEFINE_HOOK_BODY6(rettype, funcname, ...) \
	HAP_HOOK_BODYn(6, rettype, funcname, __VA_ARGS__)
#define HAP_DEFINE_HOOK_BODY7(rettype, funcname, ...) \
	HAP_HOOK_BODYn(7, rettype, funcname, __VA_ARGS__)
#define HAP_DEFINE_HOOK_BODY8(rettype, funcname, ...) \
	HAP_HOOK_BODYn(8, rettype, funcname, __VA_ARGS__)
#define HAP_DEFINE_HOOK_BODY9(rettype, funcname, ...) \
	HAP_HOOK_BODYn(9, rettype, funcname, __VA_ARGS__)
#define HAP_DEFINE_HOOK_BODY10(rettype, funcname, ...) \
	HAP_HOOK_BODYn(10, rettype, funcname, __VA_ARGS__)

#define HAP_HOOKn(n, rettype, funcname, ...) \
	rettype \
	_standard_##funcname(HAP_MAP(n, HAP_FUNC_ARG, __VA_ARGS__)); \
		\
	rettype funcname(HAP_MAP(n, HAP_FUNC_ARG, __VA_ARGS__)) \
	{	\
		if (!_HAP_HOOK_COND_##funcname(HAP_MAP(n, HAP_ARG_VAL, __VA_ARGS__))) \
			return _standard_##funcname(HAP_MAP(n, HAP_ARG_VAL, __VA_ARGS__)); \
		return _HAP_HOOK_BODY_##funcname(HAP_MAP(n, HAP_ARG_VAL, __VA_ARGS__));\
	}	\
		\
	rettype	\
	_standard_##funcname(HAP_MAP(n, HAP_FUNC_ARG, __VA_ARGS__))

#define HAP_HOOK0(rettype, funcname, ...) \
	HAP_HOOKn(0, rettype, funcname, __VA_ARGS__)
#define HAP_HOOK1(rettype, funcname, ...) \
	HAP_HOOKn(1, rettype, funcname, __VA_ARGS__)
#define HAP_HOOK2(rettype, funcname, ...) \
	HAP_HOOKn(2, rettype, funcname, __VA_ARGS__)
#define HAP_HOOK3(rettype, funcname, ...) \
	HAP_HOOKn(3, rettype, funcname, __VA_ARGS__)
#define HAP_HOOK4(rettype, funcname, ...) \
	HAP_HOOKn(4, rettype, funcname, __VA_ARGS__)
#define HAP_HOOK5(rettype, funcname, ...) \
	HAP_HOOKn(5, rettype, funcname, __VA_ARGS__)
#define HAP_HOOK6(rettype, funcname, ...) \
	HAP_HOOKn(6, rettype, funcname, __VA_ARGS__)
#define HAP_HOOK7(rettype, funcname, ...) \
	HAP_HOOKn(7, rettype, funcname, __VA_ARGS__)
#define HAP_HOOK8(rettype, funcname, ...) \
	HAP_HOOKn(8, rettype, funcname, __VA_ARGS__)
#define HAP_HOOK9(rettype, funcname, ...) \
	HAP_HOOKn(9, rettype, funcname, __VA_ARGS__)
#define HAP_HOOK10(rettype, funcname, ...) \
	HAP_HOOKn(10, rettype, funcname, __VA_ARGS__)

/* Hook on DefineRelation() */
HAP_DECLARE_HOOK5(ObjectAddress, DefineRelation,
				  CreateStmt *, stmt, char, relkind, Oid, ownerId,
				  ObjectAddress *, typaddress, const char *, queryString);

/* Hook on createForeignKeyCheckTriggers() */
HAP_DECLARE_HOOK9(void, createForeignKeyCheckTriggers,
				  Oid, myRelOid, Oid, refRelOid,
				  Constraint *, fkconstraint, Oid, constraintOid,
				  Oid, indexOid,
				  Oid, parentInsTrigger, Oid, parentUpdTrigger,
				  Oid *, insertTrigOid, Oid *, updateTrigOid);

/* Hook on ExecModifyTable() */
HAP_DECLARE_HOOK1(TupleTableSlot *, ExecModifyTable,
				  PlanState *, pstate);

/* Hook on query_planner() */
HAP_DECLARE_HOOK3(RelOptInfo *, query_planner,
				  PlannerInfo *, root,
				  query_pathkeys_callback, qp_callback, void *, qp_extra);

/* Hook on clause_selectivity_ext() */
HAP_DECLARE_HOOK6(Selectivity, clause_selectivity_ext,
				  PlannerInfo *, root,
				  Node *, clause,
				  int, varRelid,
				  JoinType, jointype,
				  SpecialJoinInfo *, sjinfo,
				  bool, use_extended_stats);

/* Hook on ATExecSetRelOptions() */
HAP_DECLARE_HOOK4(void, ATExecSetRelOptions,
				  Relation, rel,
				  List *, defList,
				  AlterTableType, operation,
				  LOCKMODE, lockmode);

/* Hook on create_seqscan_plan() */
HAP_DECLARE_HOOK4(SeqScan *, create_seqscan_plan,
				  PlannerInfo *, root,
				  Path *, best_path,
				  List *, tlist,
				  List *, scan_clauses);

#endif /* HAP */
#endif /* HAP_HOOK_H */
