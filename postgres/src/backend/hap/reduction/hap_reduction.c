/*-------------------------------------------------------------------------
 *
 * hap_reduction.c
 *	  Query Reduction with Hidden Attribute Partitioning (HAP)
 *
 *
 * IDENTIFICATION
 *	  src/backend/hap/planner/hap_reduction.c
 *-------------------------------------------------------------------------
 */
#ifdef HAP_REDUCTION
#include "postgres.h"
#include "nodes/nodes.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pathnodes.h"
#include "nodes/primnodes.h"
#include "optimizer/pathnode.h"
#include "optimizer/joininfo.h"
#include "optimizer/paths.h"
#include "optimizer/optimizer.h"
#include "optimizer/planmain.h"
#include "access/sysattr.h"
#include "access/table.h"
#include "catalog/pg_type.h"
#include "utils/relcache.h"
#include "utils/rel.h"
#include "utils/fmgroids.h"
#include "utils/fmgrprotos.h"
#include "portability/instr_time.h"

#include "hap/hap_am.h"
#include "hap/hap_reduction.h"
#include "hap/hap_planner.h"
#include "hap/hap_encoding.h"
#include "hap/hap_catalog.h"

/* GUC parameter */
bool enable_hap_reduction; /* default: true */

/* Meta data for substitution cache */
HapSubstitutionCacheMeta SubstitutionCacheMeta;

/* Private functions */

/* To find redundant relations */
static bool CheckJoinRelationship(PlannerInfo* root, RelOptInfo *baserel);
static bool CheckPrimaryKeyRelationship(RelOptInfo *baserel, AttrNumber attno);
static bool CheckForeignKeyRelationship(PlannerInfo* root, Var *otherrelVar, 
												Var *baserelVar);
static bool CheckForeignKeyRelationshipRecurse(PlannerInfo *root, List *fkInfos,  
												Var *baserelVar);
static List *GetNextHapForeignKeyInfos(PlannerInfo *root, HapForeignKeyInfo *fkInfo,
												Var *baserelVar, bool *match);
static bool CheckNonEqualPrediate(PlannerInfo* root, RelOptInfo *baserel,
												bool *hasAnyRelationship);
static bool CheckFilterPredicate(PlannerInfo* root, RelOptInfo *baserel);
static bool CheckAttributesNeeded(PlannerInfo* root, RelOptInfo *baserel);

/* To reduce redundant relations */
static void RemoveRelFromQuery(PlannerInfo *root, RelOptInfo *baserel, int relid);
static void RemoveEclassMembersFromEclass(EquivalenceClass *eclass, 
												RelOptInfo *baserel);
static void RemoveEclassSourcesFromEclass(PlannerInfo *root, 
								EquivalenceClass *eclass, RelOptInfo *baserel);
static List *RemoveRelFromJoinlist(List *joinlist, int relid, int *nremoved);


/* To subsitute existing attrbutes with new function */
static bool CheckSubstitutableAttribute(PlannerInfo *root, RelOptInfo *baserel, 
															AttrNumber attno);
static Relids GetOtherrelsNeeding(PlannerInfo *root, RelOptInfo *baserel, 
															AttrNumber attno);
static bool ContainAttribute(RelOptInfo *baserel, AttrNumber attno, Node *clause);
static void SetSubstitutionInfo(PlannerInfo *root, RelOptInfo *baserel, 
															AttrNumber attno);
static Var *GetVarFromClause(RelOptInfo *baserel, AttrNumber attno, Node *clause);
static void PropagateSubstitutionInfo(PlannerInfo *root, RelOptInfo *baserel);
static RelOptInfo *GetPropagatedRelation(PlannerInfo *root, RelOptInfo *baserel);
static void ApplySubstitutionInfos(PlannerInfo *root);
static bool SubstituteVarWithFunc(Node *node, HapSubstitutionInfo *info, 
													HapSubstitutionCache *cache);
static HapSubstitutionCache *PoplulateSubstitutionCache(HapSubstitutionInfo* info);
static AttrNumber GetHiddenAttributeNumber(Oid reloid);

static Var *MakeVar(Index relid, AttrNumber attno);
static bool CheckAllPropagated(PlannerInfo *root, RestrictInfo *rinfo);


/*
 * HapReducePlan
 *		Main entry for query reduction.
 */
List *
HapReducePlan(PlannerInfo* root, List *joinlist)
{
	instr_time reductionStartTime;
	instr_time reductionEndTime;

	/* HAP guc flags must be activated */
	if (!enable_hap_planner || !enable_hap_reduction)
	{
		elog(WARNING, 
			"enable_hap_planner or enable_hap_reduction is not activated");
		return joinlist;
	}

	if (!hap_check_dimension_table_existence(root))
		return joinlist;	

	/* Query reduction is not possible with single relation */
	if (root->simple_rel_array_size < 3)
		return joinlist;

	/* Reduction start time */
	INSTR_TIME_SET_CURRENT(reductionStartTime);
	
	/* Find and remove redundant relations. */
	while (FindRedundantRelations(root))
		joinlist = RemoveRedundantRelations(root, joinlist);

	/* Apply substitution. */
	ApplySubstitutionInfos(root);

	/* Reduction end time */
	INSTR_TIME_SET_CURRENT(reductionEndTime);

	/* Add reduction time */
	INSTR_TIME_SUBTRACT(reductionEndTime, reductionStartTime);
	root->glob->hapReductionTime += INSTR_TIME_GET_MILLISEC(reductionEndTime);

	return joinlist;
}

/*
 * FindRedundantRelations
 *		We are finding redundant relations. If some relations within PlannerInfo 
 * 		is removable, return value is true. Otherwise, return value is false.
 *
 * There are three steps involved in identifying redundant relations, involving 
 * the iteration through all relations in the PlannerInfo structure.  
 *
 * Step 1: Checking join relationship
 * 		The RestrictInfo list of eclass is used for checking join relationship.
 *		The EquivalenceClass retains 'ec_sources' member variable dedicated 
 *		to managing join relationship. Here, we examine whether the relations are 
 *		being referenced or are referencing others. If a relation is referencing 
 *		other relations, it cannot be removed. Conversely, if a relation is not 
 *		referencing others but is referenced by them, we consider this relation 
 *		as a potential candidate for removal.
 *
 * Step 2: Checking filter predicates
 * 		We examine filter predicates that are solely utilized within a single 
 *		relation. The PlannerInfo structure maintains 'baserestrictinfo' for 
 *		this purpose. As we iterate through the filter predicates in 
 *		'baserestrictinfo', we assess whether the filter predicate is used for 
 *		Hidden Attribute Partitioning (HAP) or not. Additionally, we evaluate 
 *		whether the filter predicate has been propagated. If there is an 
 *		unpropagated filter predicate that does not apply to HAP, the target 
 *		relation cannot be removed.
 *
 * Step 3: Checking attributes needed within other relations
 *		We evaluate whether attributes within a relation are necessary in other 
 *		relations (i.e. RelOptInfo). If the attributes are required not solely 
 *		for a foreign key relationship, the coressponding relation cannot be 
 *		eliminated.
 */
bool 
FindRedundantRelations(PlannerInfo* root)
{
	Index		rti;
	bool		found = false;

	/* Iterate all relations */
	for (rti = 1; rti < root->simple_rel_array_size; rti++)
	{
		RelOptInfo *baserel = root->simple_rel_array[rti];
		RangeTblEntry *rte = root->simple_rte_array[rti];

		/* Ignore the relation that is not be used for the join path */
		if ((baserel == NULL) || (baserel->reloptkind != RELOPT_BASEREL))
			continue;

		/* Ignore non-HAP relations */
		if (!HapCheckAmUsingRelId(rte->relid))
			continue;

		/* If this relation has been previously deleted, we skip it. */
		if (baserel->is_redundant == true)
			continue;

		/* 
		 * Step 1: Checking join relationship 
		 */
		if (!CheckJoinRelationship(root, baserel))
			continue;
		
		/* 
		 * Step 2: Checking filter predicates 
		 */
		if (!CheckFilterPredicate(root, baserel))
			continue;

		/* 
		 * Step 3: Checking attributes needed within other relations 
		 */
		if (!CheckAttributesNeeded(root, baserel))
			continue;

		/* If the relation passes all steps, it is a removable relation */
		found = true;
		baserel->is_redundant = true;
	}

	return found;
}

/*
 * CheckJoinRelationship
 *		We check wether the relation has proper a join relationship with other 
 *		relations.
 */
static bool 
CheckJoinRelationship(PlannerInfo* root, RelOptInfo *baserel)
{
	int			i;
	ListCell	*lc;
	Bitmapset	*matched_ecs;
	bool		hasAnyRelationship = false;
	bool 		hasNonPrimaryKey = false;
	bool		hasNonForeignKey = false;
	bool 		hasMultiColumn = false;
	bool		hasCannotJoin = false;
	bool		hasNonEqualPredicate = false;

	/* Examine only eclasses mentioning target relation */
	matched_ecs = 
		get_eclass_indexes_for_relids(root, baserel->relids);

	i = -1;
	while ((i = bms_next_member(matched_ecs, i)) >= 0)
	{
		EquivalenceClass *cur_ec = (EquivalenceClass *) 
										list_nth(root->eq_classes, i);

		/* Iterate all restrict info within eclass */
		foreach(lc, cur_ec->ec_sources)
		{
			List		*vars;
			ListCell	*vl;
			Var			*otherrelVar = NULL;
			Var			*baserelVar = NULL;
			Offset		attoff;
			RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);
	
			/* If it is not mentioned, skip it. */
			if (!bms_is_member(baserel->relid, rinfo->clause_relids))
				continue;

			/* This predicate has been propagated, skip it. */
			if (CheckAllPropagated(root, rinfo))
				continue;

			hasAnyRelationship = true;

			/* If it is not joinable restrict info, we cannot reduce it. */
			if (!rinfo->can_join)
			{
				hasCannotJoin = true;
				break;
			}

			/* Get Var nodes from RestrictInfo */
			vars = pull_var_clause((Node *) rinfo->clause,
										PVC_RECURSE_AGGREGATES |
										PVC_RECURSE_WINDOWFUNCS |
										PVC_INCLUDE_PLACEHOLDERS);

			/* Check all Var nodes within RestrictInfo */
			foreach(vl, vars)
			{
				Var	*var = (Var *) lfirst(vl);

				if (var->varno == baserel->relid)
					/* Current Var node is mentioned for baserel */
					baserelVar = var;
				else
					/* Current Var node is mentioned for other relation */
					otherrelVar = var;
			}

			/* If this relation uses multi column, skip it */
			if (list_length(vars) != 2)
			{
				hasMultiColumn = true;
				list_free(vars);
				break;
			}

			/* Resource cleanup */
			list_free(vars);

			Assert(baserelVar != NULL && otherrelVar != NULL);

			/* Init values */
			hasNonPrimaryKey = false;
			hasNonForeignKey = false;

			/* 
			 * If primary key of current relation is not referenced by other 
			 * relations, it cannot be reduced. 
			 */
			if (!CheckPrimaryKeyRelationship(baserel, baserelVar->varattno))
				hasNonPrimaryKey = true;

			if (hasNonPrimaryKey)
				break;

			/* 
			 * If foreign key of other relation is not referencing current 
			 * relation, it cannot be reduced. 
			 */
			if (!CheckForeignKeyRelationship(root, otherrelVar, baserelVar))
				hasNonForeignKey = true;

			if (hasNonForeignKey)
				break;

			/* Calculate offset of attribute number */
			attoff = baserelVar->varattno - baserel->min_attr;
			
			/* Add relation index of referencing relation to attr_referenced. */
			baserel->attr_referenced[attoff] = 
				bms_add_member(baserel->attr_referenced[attoff], otherrelVar->varno); 
			baserel->attr_referenced[attoff] = 
				bms_add_member(baserel->attr_referenced[attoff], baserelVar->varno); 
		}
	
		/* 
	 	 * If this relation satisfies the following conditions, it is considered 
	 	 * inappropriate. 
	 	 */
		if (hasNonPrimaryKey || hasNonForeignKey || hasMultiColumn || hasCannotJoin)
			break;
	}

	/*
	 * Furthermore, we assess whether the relation includes non-equality join 
	 * predicates. If it does, reduction is not feasible.
	 */
	if (!CheckNonEqualPrediate(root, baserel, &hasAnyRelationship))
		hasNonEqualPredicate = true;		

	/* 
	 * If this relation satisfies the following conditions, it is considered 
	 * inappropriate. 
	 *
	 * (1) The relation doesn't have any relationship with other relations.
	 * (2) A join predicate don't mention primary key.
	 * (3) A join predicate don't mention foreign key.
	 * (4) A join predicate uses multicolumn. (e.g. A = B + C)
	 * (5) A join predicate is not marked as 'can_join'.
	 * (6) A join predicate involves a non-equality operation. (e.g. A < B)
	 */
	if (!hasAnyRelationship || hasNonPrimaryKey || hasNonForeignKey || 
					hasMultiColumn || hasCannotJoin || hasNonEqualPredicate)
		return false;

	return true;
}

/*
 * CheckPrimaryKeyRelationship
 *		If attribute number is primary key of 'baserel', return value is true.
 *		Return value is false, if not.
 */
static bool 
CheckPrimaryKeyRelationship(RelOptInfo *baserel, AttrNumber attno)
{
	if (bms_is_member((attno - FirstLowInvalidHeapAttributeNumber), 
												baserel->primarykeyattrs))
	{
		return true;
	}

	return false;
}

/*
 * CheckForeignKeyRelationship
 */
static bool 
CheckForeignKeyRelationship(PlannerInfo* root, Var *otherrelVar, Var *baserelVar)
{
	HapForeignKeyInfo *fkInfo;
	bool result = false;
	List *fkInfos = NIL;
	RangeTblEntry *otherrte = root->simple_rte_array[otherrelVar->varno];

	/* Make HapForeignKeyInfo. */
	fkInfo = (HapForeignKeyInfo *)
  				palloc(sizeof(struct HapForeignKeyInfo));

	fkInfo->attno = otherrelVar->varattno;
	fkInfo->reloid = otherrte->relid;

	/* Init first HapForeignKeyInfo list. */
	fkInfos = lappend(fkInfos, fkInfo);

	/* 
	 * We check whether the foreign key relationship is suitable or 
	 * not recursively.
	 */
	if (CheckForeignKeyRelationshipRecurse(root, fkInfos, baserelVar))
		result = true;

	/* Resource cleanup. */
	list_free_deep(fkInfos);

	return result;
}

/*
 * CheckForeignKeyRelationshipRecurse
 */
static bool
CheckForeignKeyRelationshipRecurse(PlannerInfo *root, List *fkInfos,  
															Var *baserelVar)
{
	ListCell *lc;
	bool result = false;
	bool match = false;
	List *nextFkInfos = NIL;

	/* Iterate all HapForeignKeyInfo structure in the list. */
	foreach(lc, fkInfos)
	{
		List *newFkInfos;
		HapForeignKeyInfo *fkInfo = (HapForeignKeyInfo *) lfirst(lc);

		/* Get next HapForeignKeyInfo list using current list. */
		newFkInfos = GetNextHapForeignKeyInfos(root, fkInfo, baserelVar, &match);

		/* Concatenate newFkInfos with global list for next recursive step. */
		if (newFkInfos)
			nextFkInfos = list_concat(nextFkInfos, newFkInfos);

		/* Resource cleanup. */
		list_free(newFkInfos);

		/* We found matched foreign key relationship, it is suitable. */
		if (match)
		{
    		result = true;
    		break;
		}
	}

	/* 
	 * We check whether the foreign key relationship is suitable or 
	 * not recursively.
	 */
	if (!match && nextFkInfos != NIL && 
			CheckForeignKeyRelationshipRecurse(root, nextFkInfos, baserelVar))
		result = true;

	/* Resource cleanup. */
	list_free(nextFkInfos);

	return result;
}

/*
 * GetNextHapForeignKeyInfos
 */
static List *
GetNextHapForeignKeyInfos(PlannerInfo *root, HapForeignKeyInfo *fkInfo,
												Var *baserelVar, bool *match)
{
	Relation relation;
	List *cachedfkeys;
	ListCell *lc;
	List *newFkInfos = NIL;
	RangeTblEntry *baserte = root->simple_rte_array[baserelVar->varno];
	Oid baserelOid = baserte->relid;

	/* Get Relation with table_open. */
	relation = table_open(fkInfo->reloid, AccessShareLock);

	/* Get foreign key information. */
	cachedfkeys = RelationGetFKeyList(relation);
	foreach(lc, cachedfkeys)    
	{
		int i;
		ForeignKeyCacheInfo *cachedfk = (ForeignKeyCacheInfo *) lfirst(lc);

		/* Iterate all foreign keys. */
		for (i = 0; i < cachedfk->nkeys; ++i)
		{
    		/* 
			 * Inspect the foreign key columns in the referencing table. 
			 * If a matched foreign key column is found, we proceed to the next 
			 * HapForeignKeyInfo. 
			 */
    		if (cachedfk->conkey[i] == fkInfo->attno)
    		{
        		HapForeignKeyInfo *newFkInfo;

				/* Make new HapForeignKeyInfo */
				newFkInfo = (HapForeignKeyInfo *) 
								palloc(sizeof(struct HapForeignKeyInfo));

				newFkInfo->reloid = cachedfk->confrelid;
				newFkInfo->attno = cachedfk->confkey[i];

				newFkInfos = lappend(newFkInfos, newFkInfo);

				/*
			 	 * If there is a correspondence with baserelVar, it is deemed 
				 * suitable.
			 	 */
				if (baserelOid == cachedfk->confrelid && 
						baserelVar->varattno == cachedfk->confkey[i])
					*match = true;
    		}
		}
	}

	table_close(relation, AccessShareLock);

	return newFkInfos;
}

/*
 * SetAllPropagated
 */
static void
SetAllPropagated(PlannerInfo *root, RestrictInfo *rinfo)
{
	ListCell *lc;
	List *vars;
	Relids relsNeeding = NULL;
	RelOptInfo *rel;

	/* Operations with multiple attribute are not considered yet */
 	vars = pull_var_clause((Node *) rinfo->clause,
									PVC_RECURSE_AGGREGATES |
									PVC_RECURSE_WINDOWFUNCS |
									PVC_INCLUDE_PLACEHOLDERS);

	/* Collect relations needed from the restrict info. */
	foreach (lc, vars)
	{
		Var *var = (Var *) lfirst(lc);
		
		relsNeeding = bms_add_member(relsNeeding, var->varno); 
	}

	/* Remove relation ids from each 'attr_needed'. */
	foreach (lc, vars)
	{
		Offset attoff;
		Var *var = (Var *) lfirst(lc);

		rel = find_base_rel(root, var->varno);
		attoff = var->varattno - rel->min_attr;	

		rel->attr_needed[attoff] = 
				bms_del_members(rel->attr_needed[attoff], relsNeeding);
	}
}

/*
 * ClausePropagated
 */
static bool
ClausePropagated(PlannerInfo *root, Expr *clause)
{
	List *vars;
	Var *var;
	AttrNumber attrno;
	Oid encode_table_id;
	List *values = NIL;
	RangeTblEntry *rte;

	/* Don't use 'and' clause or 'or' clause as parameter. */
	Assert(!is_orclause(clause) && !is_andclause(clause));

	/* Operations with multiple attribute are not considered yet */
	vars = pull_var_clause((Node *) clause,
									PVC_RECURSE_AGGREGATES |
									PVC_RECURSE_WINDOWFUNCS |
									PVC_INCLUDE_PLACEHOLDERS);
	if (list_length(vars) != 1)
		return false;
	else
		var = (Var *) linitial(vars);
	
	/* Ignore system attributes */
	attrno = var->varattno;
	if (attrno < 1)
		return false;

	/* Check whether this attribute is encoded or not */
	rte = find_base_rte(root, var->varno);
	encode_table_id = HapGetEncodeTableId(rte->relid, attrno);
	if (encode_table_id == InvalidOid)
		return false;

	/* Get encoding values */
	values = hap_get_hidden_attribute_encoding_values(encode_table_id, clause);
	if (values == NIL)
		return false;
	
	return true;
}

/*
 * CheckAllPropagatedRecurse
 */
static bool
CheckAllPropagatedRecurse(PlannerInfo *root, Expr *clause)
{
	if (is_orclause(clause) || is_andclause(clause))
	{
		ListCell   *temp;

		foreach(temp, ((BoolExpr *) clause)->args)
		{
			Expr *expr = (Expr *) lfirst(temp);
	
			if (!CheckAllPropagatedRecurse(root, expr))
				return false;
		}
	}
	else 
	{
		/* This clause is not 'and' or 'or'. */
		if (!ClausePropagated(root, clause))
			return false;
	}

	/* All elements of clause is well propagated. */
	return true;
}

/*
 * CheckAllPropagated
 */
static bool
CheckAllPropagated(PlannerInfo *root, RestrictInfo *rinfo)
{
	return CheckAllPropagatedRecurse(root, rinfo->clause);
}

/*
 * CheckNonEqualPredicate
 * 		If relation has non equal predicate, return value is false. If not, 
 *		return value is true.
 */
static bool
CheckNonEqualPrediate(PlannerInfo* root, RelOptInfo *baserel, 
													bool *hasAnyRelationship)
{
	ListCell *lc;
	bool hasNonEqualPredicate = false;

	foreach(lc, baserel->joininfo)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		/* The relation has some relationship with other relations. */
		*hasAnyRelationship = true;

		/* Check whether restrict info is all propagated or not. */
		if (CheckAllPropagated(root, rinfo))
		{
			/* 
			 * If this restrict info is propagated, we remove relation ids 
			 * within 'attr_needed'. 
			 */
			SetAllPropagated(root, rinfo);
			continue;
		}
		
		/*
		 * The given clause has a mergejoinable operator and can be applied without
		 * any delay by an outer join, so its two sides can be considered equal
		 * anywhere they are both computable.
		 */
		if (!rinfo->mergeopfamilies || !check_equivalence_delay(root, rinfo))
		{
			hasNonEqualPredicate = true;
			break;
		}
	}

	return (hasNonEqualPredicate == false);
}

/*
 * CheckFilterPredicate
 */
static bool 
CheckFilterPredicate(PlannerInfo* root, RelOptInfo *baserel)
{
	bool		allFilterPropagated;
	ListCell	*lc;

	allFilterPropagated = true;

	/* Iterate all filter predicates in baserestricinfo */
	foreach(lc, baserel->baserestrictinfo)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		/* Skip filters that are propagated as hidden attributes. */
		if (rinfo->hap_info_is_hidden_attribute)
			continue;

		/* Verify that all filters have been propagated. */
		if (!CheckAllPropagated(root, rinfo))
		{
			allFilterPropagated = false;
			break;
		}
	}

	/* If unpropagated filters exist, we cannot eliminate the relation. */
	if (allFilterPropagated == false)
		return false;

	return true;
}

/*
 * CheckAttributesNeeded
 */
static bool 
CheckAttributesNeeded(PlannerInfo* root, RelOptInfo *baserel)
{
	int		i;
	Relids	relsNeeding;
	Offset	attoff;
	Relids	referencingRelids = NULL;
	bool	attNeeds = false;
	bool	partialConnected = false;
	bool	multiConnected = false;
	int		numberOfAttributes = (baserel->max_attr - baserel->min_attr + 1);

	/* Iterate all attribtes in relation */
	for (attoff = 0; attoff < numberOfAttributes; ++attoff)
	{
		/* Obtain the relations (i.e. bitmapset) that require this attribute. */
		relsNeeding = baserel->attr_needed[attoff];

		/* There is no needs. */
		if (bms_is_empty(relsNeeding))
			continue;

		/* 
		 * This attribute is a system defined attribute or a whole-row var. 
		 * Skip it. 
		 */
		if (attoff <= (-baserel->min_attr))
		{
			attNeeds = true;
			break;
		}

		/*
		 * We check if this attribute can be substituted with a hidden attribute 
		 * of another relation in the function. If it is possible, we set the 
		 * substitution information to be used later.
		 */
		if (CheckSubstitutableAttribute(root, baserel, 
												attoff + baserel->min_attr))
		{
			SetSubstitutionInfo(root, baserel, attoff + baserel->min_attr);
			continue;
		}

		/* 
		 * If the attribute is utilized for a foreign key relationship and is 
		 * not used in other relations, it is suitable for reduction.
		 */
		if (!bms_equal(relsNeeding, baserel->attr_referenced[attoff]))
		{
			/* 
			 * This attribute is not used for foreign key. We cannot remove the
			 * relation.
			 */
			attNeeds = true;
			break;
		}
	}


	if (!attNeeds)
	{
		/*
		 * We should have fully connected primary key and foreign key 
		 * relationship. We do this, checking referencing relations of primary 
		 * key is all same key by key.
		 */
		i = -1;
		while ((i = bms_next_member(baserel->primarykeyattrs, i)) > 0)
		{
			Offset attoff = i - 1;

			Assert(i > -baserel->min_attr);

			/* Save referencing relations. */
			if (referencingRelids == NULL)
			{
				referencingRelids = bms_copy(baserel->attr_referenced[attoff]);

				/* 
				 * 'attr_referenced' includes relation index itself. So, we 
				 * consider it to check multi relationship.
				 */
				if (bms_num_members(referencingRelids) > 2)
				{
					multiConnected = true;
					break;
				}

				continue;
			} 
			
			/* Equality check. */
			if (!bms_equal(referencingRelids, baserel->attr_referenced[attoff]))
			{
				partialConnected = true;
				break;
			}
		}

		/* Resource cleanup */
		bms_free(referencingRelids);
	}

	/* 
	 * If this relation possesses attributes utilized in other
	 * relations, we bypass it. 
	 */
	if (attNeeds || partialConnected || multiConnected)
		return false;

	return true;
}

/*
 * RemoveRedundantRelations
 *		We undertake the process of discarding redundant relations by iterating 
 *		through all the relations within a query. Initially, all resources 
 *		associated with the eliminated 'RelOptInfo' are removed. Following this, 
 * 		the relation is omitted from the 'joinlist'. Consequently, the relation 
 *		is excised from the query since the query optimizer discovers join paths 
 *		using the 'joinlist' and 'RelOptInfo' list.		 
 */
List *
RemoveRedundantRelations(PlannerInfo *root, List *joinlist)
{
	int rti;
	RelOptInfo *baserel;
	RangeTblEntry *baserte;

	/* Iterate all relations */
	for (rti = 1; rti < root->simple_rel_array_size; rti++)
	{
		baserel = root->simple_rel_array[rti];
		baserte = root->simple_rte_array[rti];
 
		/* Ignore the relation that is not be used for the join path */
		if ((baserel == NULL) || (baserel->reloptkind != RELOPT_BASEREL))
			continue;

		/* Ignore the relation that has already been removed. */
		if (baserel->reloptkind == RELOPT_DEADREL)
			continue;
		
		if (baserel->is_redundant)
		{
			int	nremoved;

			/* Remove resources of PlannerInfo */
			RemoveRelFromQuery(root, baserel, baserel->relid);
			
			/* Remove resources of joinlist */
			joinlist = 
				RemoveRelFromJoinlist(joinlist, baserel->relid, &nremoved);

			/* Propagate substitution info. */
			PropagateSubstitutionInfo(root, baserel);
		
			/* This table is eliminated */
			baserte->eliminated = true;
		}
	}
	
	return joinlist;
}

/*
 * RemoveRelFromQuery
 *
 * Remove the target relid from the planner's data structures, having
 * determined that there is no need to include it in the query.
 *
 * We are not terribly thorough here.  We must make sure that the rel is
 * no longer treated as a baserel, and that attributes of other baserels
 * are no longer marked as being needed at joins involving this rel.
 * Also, join quals involving the rel have to be removed from the joininfo
 * lists, but only if they belong to the outer join identified by joinrelids.
 */
static void
RemoveRelFromQuery(PlannerInfo *root, RelOptInfo* baserel, int relid)
{
	RelOptInfo *rel = find_base_rel(root, relid);
	Bitmapset  *matched_ecs;
	List	   *joininfos;
	Index		rti;
	ListCell   *l;
	int			i;

	/*
	 * Mark the rel as "dead" to show it is no longer part of the join tree.
	 * (Removing it from the baserel array altogether seems too risky.)
	 */
	rel->reloptkind = RELOPT_DEADREL;

	/*
	 * Remove references to the rel from other baserels' attr_needed arrays.
	 */
	for (rti = 1; rti < root->simple_rel_array_size; rti++)
	{
		RelOptInfo *otherrel = root->simple_rel_array[rti];
		int			attroff;

		/* there may be empty slots corresponding to non-baserel RTEs */
		if (otherrel == NULL)
			continue;

		Assert(otherrel->relid == rti); /* sanity check on array */

		/* no point in processing target rel itself */
		if (otherrel == rel)
			continue;

		for (attroff = otherrel->max_attr - otherrel->min_attr;
			 attroff >= 0;
			 attroff--)
		{
			otherrel->attr_needed[attroff] =
				bms_del_member(otherrel->attr_needed[attroff], relid);
		}
	}

	/*
	 * Remove any joinquals referencing the rel from the joininfo lists.
	 *
	 * In some cases, a joinqual has to be put back after deleting its
	 * reference to the target rel.  This can occur for pseudoconstant and
	 * outerjoin-delayed quals, which can get marked as requiring the rel in
	 * order to force them to be evaluated at or above the join.  We can't
	 * just discard them, though.  Only quals that logically belonged to the
	 * outer join being discarded should be removed from the query.
	 *
	 * We must make a copy of the rel's old joininfo list before starting the
	 * loop, because otherwise remove_join_clause_from_rels would destroy the
	 * list while we're scanning it.
	 */
	joininfos = list_copy(rel->joininfo);
	foreach(l, joininfos)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(l);

		remove_join_clause_from_rels(root, rinfo, rinfo->required_relids);
	}

	/* Equivalence class clean up */

	/* Examine only eclasses mentioning target relation */
	matched_ecs = 
		get_eclass_indexes_for_relids(root, baserel->relids);

	i = -1;
	while ((i = bms_next_member(matched_ecs, i)) >= 0)
	{
		EquivalenceClass *eclass = (EquivalenceClass *) 
											list_nth(root->eq_classes, i);

		/* Sanity check eclass_indexes only contain ECs for rel */
		Assert(bms_is_member(relid, eclass->ec_relids));

		/* Delete target relid from eclass's relid */
		eclass->ec_relids = bms_del_member(eclass->ec_relids, relid);

		/* Resource cleanup of ec_members */
		RemoveEclassMembersFromEclass(eclass, baserel);

		/* Resource cleanup of ec_sources */
		RemoveEclassSourcesFromEclass(root, eclass, baserel);
	}
}


/*
 * RemoveEclassMembersFromEclass
 *		We eliminate eclass members that originated from the relation. 		
 */
static void
RemoveEclassMembersFromEclass(EquivalenceClass *eclass, RelOptInfo *baserel)
{
	ListCell *lc;
	EquivalenceMember *eclassMember;

restart_ec_members:
	foreach(lc, eclass->ec_members)
	{
		/* Iterate all eclass member within eclass */
		eclassMember = (EquivalenceMember *) lfirst(lc);

		if (bms_is_member(baserel->relid, eclassMember->em_relids))
		{
			/* Current eclass member is relevant to target relation */
			eclassMember->em_relids = 
				bms_del_member(eclassMember->em_relids, baserel->relid);

			if (bms_is_empty(eclassMember->em_relids))
			{
				/* Current eclass member is empty. Proceed with cleanup. */
				eclass->ec_members = 
					list_delete_cell(eclass->ec_members, lc);

				/*
				 * Restart the scan.  This is necessary to ensure we find all
				 * removable eclass member independently of ordering of the 
				 * join_info_list.
				 */
				goto restart_ec_members;
			}
		}
		else
		{
			/* This eclass member is irrelevant to target relation. */
		}
	}
}

/*
 * RemoveEclassSourcesFromEclass
 */
static void
RemoveEclassSourcesFromEclass(PlannerInfo *root, EquivalenceClass *eclass, 
															RelOptInfo *baserel)
{
	ListCell *lc;

restart_ec_sources:
	foreach(lc, eclass->ec_sources)
	{
		List		*vars;
		ListCell	*vl;
		Offset		attoff;
		RelOptInfo	*otherrel;
		Var			*otherrelVar = NULL;
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		if (bms_is_member(baserel->relid, rinfo->clause_relids))
		{
			/* Get Var nodes from RestrictInfo */
			vars = pull_var_clause((Node *) rinfo->clause,
										PVC_RECURSE_AGGREGATES |
										PVC_RECURSE_WINDOWFUNCS |
										PVC_INCLUDE_PLACEHOLDERS);

			/* Check all Var nodes within RestrictInfo */
			foreach(vl, vars)
			{
				Var	*var = (Var *) lfirst(vl);

				if (var->varno != baserel->relid)
				{
					/* Current Var node is mentioned for other relation */
					otherrelVar = var;

					/* 
					 * If this is join predicate, we have to free other 
					 * relations's 'attr_needed' member variable.
					 */
					otherrel = find_base_rel(root, otherrelVar->varno);
					attoff = otherrelVar->varattno - otherrel->min_attr;
	
					/* Remove required relid from attr_needed. */
					otherrel->attr_needed[attoff] =
						bms_del_member(otherrel->attr_needed[attoff], otherrel->relid);
				}
			}

			list_free(vars);

			/* Remove restirc info from eclass sources. */
			eclass->ec_sources = 
				list_delete_cell(eclass->ec_sources, lc);

			/*
			 * Restart the scan.  This is necessary to ensure we find all
			 * removable eclass sources independently of ordering of the 
			 * join_info_list.
			 */
			goto restart_ec_sources;
		}
	}
}


/*
 * RemoveRelFromJoinlist
 *
 * Remove any occurrences of the target relid from a joinlist structure.
 *
 * It's easiest to build a whole new list structure, so we handle it that
 * way.  Efficiency is not a big deal here.
 *
 * *nremoved is incremented by the number of occurrences removed (there
 * should be exactly one, but the caller checks that).
 */
static List *
RemoveRelFromJoinlist(List *joinlist, int relid, int *nremoved)
{
	List	   *result = NIL;
	ListCell   *jl;

	foreach(jl, joinlist)
	{
		Node	   *jlnode = (Node *) lfirst(jl);

		if (IsA(jlnode, RangeTblRef))
		{
			int			varno = ((RangeTblRef *) jlnode)->rtindex;

			if (varno == relid)
				(*nremoved)++;
			else
				result = lappend(result, jlnode);
		}
		else if (IsA(jlnode, List))
		{
			/* Recurse to handle subproblem */
			List	   *sublist;

			sublist = RemoveRelFromJoinlist((List *) jlnode,
											   relid, nremoved);
			/* Avoid including empty sub-lists in the result */
			if (sublist)
				result = lappend(result, sublist);
		}
		else
		{
			elog(ERROR, "unrecognized joinlist node type: %d",
				 (int) nodeTag(jlnode));
		}
	}

	/* Shallow free */
	list_free(joinlist);

	return result;
}

/*
 ******************************************************************************
 *
 *	Substitution Functions
 *
 ******************************************************************************
 */

/*
 * CheckSubstitutableAttribute
 *		We verify whether the attribute number of the relation is eligible for 
 *		substitution or not. To be eligible, the attribute should be encoded as 
 *		a hidden attribute, propagated, and not used in grouping sets or window 
 *		functions. While we can perform substitution for such cases, supporting 
 *		it is considered as future work.
 */
static bool
CheckSubstitutableAttribute(PlannerInfo *root, RelOptInfo *baserel, 
															AttrNumber attno)
{
	Relids relsNeeding;
	int descId;
	Relids otherrelsNeeding = GetOtherrelsNeeding(root, baserel, attno);
	Offset attoff = attno - baserel->min_attr;
	bool ret = true;
	Query *parse = root->parse;
	RangeTblEntry *rte = find_base_rte(root, baserel->relid);

	relsNeeding = baserel->attr_needed[attoff];

	if (!bms_is_empty(otherrelsNeeding))
		ret = false;

	if (ret && !bms_is_member(0, relsNeeding))
		ret = false; 
	
	/* Is this attribute encoded as a hidden attribute? */
	if (ret)
		descId = HapGetDimensionHiddenAttrDescid(rte->relid, attno);
	if (ret && descId == -1)
		ret = false;

	/* 
	 * If the substitution target is used in grouping sets, 
	 * we cannot proceed. 
	 */
	if (ret && list_length(parse->groupingSets) > 0 && 
			ContainAttribute(baserel, attno, (Node *) parse->groupClause))
		ret = false;

	/* 
	 * If the substitution target is used in window function, 
	 * we cannot proceed. 
	 */
	if (ret && ContainAttribute(baserel, attno, (Node *) parse->windowClause))
		ret = false;

	bms_free(otherrelsNeeding);

	return ret;
}

/*
 * GetOtherrelsNeeding
 */
static Relids
GetOtherrelsNeeding(PlannerInfo *root, RelOptInfo *baserel, AttrNumber attno)
{
	Relids otherrelsNeeding;	
	Offset attoff = attno - baserel->min_attr;
	Relids relsNeeding = baserel->attr_needed[attoff];

	otherrelsNeeding = bms_copy(relsNeeding);
	otherrelsNeeding = bms_del_member(otherrelsNeeding, 0);
	otherrelsNeeding = bms_difference(otherrelsNeeding, baserel->attr_referenced[attoff]);

	return otherrelsNeeding;
}

/*
 * ContainAttribute
 */
static bool
ContainAttribute(RelOptInfo *baserel, AttrNumber attno, Node *clause)
{
	ListCell *lc;
	List *varList = pull_var_clause(clause,
									PVC_RECURSE_AGGREGATES |
									PVC_RECURSE_WINDOWFUNCS |
									PVC_INCLUDE_PLACEHOLDERS);

	/* 
	 * We verify whether 'clause' contains a specific attribute number in the 
	 * relation.
	 */
	foreach (lc, varList)
	{
		Var *var = (Var *) lfirst(lc);

		/* We get match node. */
		if (var->varno == baserel->relid && var->varattno == attno)
			return true;
	}

	/* The clause has not target. */
	return false;
}

/*
 * SetSubstitutionInfo
 */
static void
SetSubstitutionInfo(PlannerInfo *root, RelOptInfo *baserel, AttrNumber attno)
{
	Var *var;
	HapSubstitutionInfo *info;
	RangeTblEntry *rte = find_base_rte(root, baserel->relid);
		
	/* Get 'Var' from target list. */	
	var = GetVarFromClause(baserel, attno, (Node *) root->processed_tlist);
	Assert(var != NULL);
	Assert(var->varno == baserel->relid && var->varattno == attno);

	/* Make info */
	info = (HapSubstitutionInfo *) palloc0(sizeof(struct HapSubstitutionInfo));

	info->origin_relid = baserel->relid;
	info->origin_attno = attno;
	info->origin_vartype = var->vartype;
	info->origin_varcollid = var->varcollid;
	info->origin_descid = HapGetDimensionHiddenAttrDescid(rte->relid, attno);
	info->encoding_table_oid = HapGetEncodeTableId(rte->relid, attno);
	info->target_desc = HapMakeHiddenAttrDesc(rte->relid, info->origin_descid);
	info->target_relid = baserel->relid;

	Assert(info->origin_descid != -1);
	Assert(info->encoding_table_oid != InvalidOid);
	Assert(info->target_desc != NULL);

	/* Add new substitution info into the relation. */
	baserel->substitution_info = lappend(baserel->substitution_info, info);
}

/*
 * GetVarFromClause
 */
static Var *
GetVarFromClause(RelOptInfo *baserel, AttrNumber attno, Node *clause)
{
	ListCell *lc;
	List *varList = pull_var_clause(clause,
								PVC_RECURSE_AGGREGATES |
								PVC_RECURSE_WINDOWFUNCS |
								PVC_INCLUDE_PLACEHOLDERS);

	foreach (lc, varList)
	{
		Var *var = (Var *) lfirst(lc);

		if (var->varno == baserel->relid && var->varattno == attno)
			return var;
	}

	return NULL;
}

/*
 * GetDescendantHiddenAttrDescid
 */
static int
GetDescendantHiddenAttrDescid(PlannerInfo *root, Index conidx, Index confidx, 
																int confDescId)
{
	ListCell *lc;
	RelOptInfo *confRel = find_base_rel(root, confidx);

	foreach (lc, confRel->hap_propagate_paths)
	{
		HapPropagatePath *path = (HapPropagatePath *) lfirst(lc);
	
		/* When we find target relation, get descendant desc id, using that. */
		if (path->target_rel_idx == conidx)
			return HapGetDescendantHiddenAttrDescid(path->rel_oid_list, 
															confDescId, true);
	}
	
	Assert(false);
	return -1; /* queit compiler */
}

/*
 * PropagateSubstitutionInfo
 *		During the reduction table finding process, we propagate the substitution 
 *		information that was generated. We create new substitution information 
 * 		and send it to the connected relation. The propagated relation is 
 *		connected through a foreign key relationship.
 */
static void
PropagateSubstitutionInfo(PlannerInfo *root, RelOptInfo *baserel)
{
	ListCell *lc;
	RelOptInfo *propagatedRel = GetPropagatedRelation(root, baserel);
	RangeTblEntry *propagatedRte = find_base_rte(root, propagatedRel->relid);

	/* Iterate all substitution information. */
	foreach (lc, baserel->substitution_info)
	{
		int16 childDescid;
		HapSubstitutionInfo *newInfo;
		HapSubstitutionInfo *info = (HapSubstitutionInfo *) lfirst(lc);

		/* Make new substitution info */
		newInfo = (HapSubstitutionInfo *) 
						palloc0(sizeof(struct HapSubstitutionInfo));

		newInfo->origin_relid = info->origin_relid;
		newInfo->origin_attno = info->origin_attno;
		newInfo->origin_vartype = info->origin_vartype;
		newInfo->origin_varcollid = info->origin_varcollid;
		newInfo->origin_descid = info->origin_descid;
		newInfo->encoding_table_oid = info->encoding_table_oid;

		/* Get child HAP desc id from parent desc id. */
		childDescid = 
			GetDescendantHiddenAttrDescid(root, propagatedRel->relid, 
									baserel->relid, info->target_desc->descid);

		/* We make new target description of HAP. */
		newInfo->target_desc = 
			HapMakeHiddenAttrDesc(propagatedRte->relid, childDescid);
		newInfo->target_relid = propagatedRel->relid;

		/* We add new target description of HAP within prapagated relation. */
		propagatedRel->substitution_info = 
			lappend(propagatedRel->substitution_info, newInfo);
	}
}

/*
 * GetPropagatedRelation
 */
static RelOptInfo *
GetPropagatedRelation(PlannerInfo *root, RelOptInfo *baserel)
{
	int i;
	RelOptInfo *rel;

	Assert(!bms_is_empty(baserel->primarykeyattrs));

	i = -1;
	while ((i = bms_next_member(baserel->primarykeyattrs, i)) > 0)
	{
		Index relid;
		Offset attoff = i - 1;
		Relids referencingRelids;

		Assert(i > -baserel->min_attr);

		/* Retrieve referencing relations excluding the base relation itself. */
		referencingRelids = bms_copy(baserel->attr_referenced[attoff]);
		referencingRelids = bms_del_member(referencingRelids, baserel->relid);

		relid = bms_singleton_member(referencingRelids);
		rel = find_base_rel(root, relid);

		return rel;
	}

	/* We cannnot access this line. */
	Assert(false);
	
	return NULL;	/* quiet complier */
}

/*
 * ApplySubstitutionInfos
 *		After the query reduction process is completed, we apply the substitution 
 *		information. To apply it, we modify the expression tree of the clauses.
 *	
 * We modify the following clauses based on the substitution information.
 * (1) Target list
 * (2) Having qual
 * (3) Relation's target list
 * (3) Relation's attributes needed
 */
static void 
ApplySubstitutionInfos(PlannerInfo *root)
{
	int rti;
	ListCell *lc;
	Query *query = root->parse;

	/* Iterate relations */
	for (rti = 1; rti < root->simple_rel_array_size; ++rti)
	{
		RelOptInfo *baserel = root->simple_rel_array[rti];
		RangeTblEntry *rte = root->simple_rte_array[rti];

		/* Ignore the relation that is not be used for the join path */
		if ((baserel == NULL) || (baserel->reloptkind != RELOPT_BASEREL))
			continue;

		/* Ignore non-HAP relations */
		if (!HapCheckAmUsingRelId(rte->relid))
			continue;

		/* If this relation has been previously deleted, we skip it. */
		if (baserel->is_redundant == true)
			continue;
		
		foreach (lc, baserel->substitution_info)
		{
			HapSubstitutionCache *cache;
			AttrNumber hidden_attribute_attno;
			Offset attoff;
			struct PathTarget *reltarget = baserel->reltarget;
			HapSubstitutionInfo *info = (HapSubstitutionInfo *) lfirst(lc);
	
			/* Make encoding table data. */
			cache = PoplulateSubstitutionCache(info);

			/* Substitute existing Var node with FuncExpr node in targetlist. */
			SubstituteVarWithFunc((Node *) query->targetList, info, cache); 

			/* Substitute existing Var node with FuncExpr node in havingQual. */
			SubstituteVarWithFunc((Node *) query->havingQual, info, cache); 

			/* Append Var node in this relation's reltarget. */
			reltarget->exprs = 
				lappend(reltarget->exprs, 
					MakeVar(info->target_relid, 
						GetHiddenAttributeNumber(info->target_desc->relid)));

			/* Get hidden attribute number. */
			hidden_attribute_attno = 
					GetHiddenAttributeNumber(info->target_desc->relid);

			/* Calculate attribute offset from attribute number. */
			attoff = hidden_attribute_attno - baserel->min_attr;

			/* Modify 'attr_needed' in 'RelOptInfo'. */
			baserel->attr_needed[attoff] = 
				bms_add_member(baserel->attr_needed[attoff], 0);
		}
	}
}

/*
 * SubstituteVarWithFunc
 * 		We substitute Var node with FuncExpr using expression tree walker.
 */
static bool
SubstituteVarWithFunc(Node *node, HapSubstitutionInfo *info, 
													HapSubstitutionCache *cache)
{
	HapSubstitutionContext context;

	context.cache = cache;
	context.info = info;
	context.parent_node = NULL;

	return hap_expression_tree_walker(node, &context);
}

/*
 * GetHiddenAttributeNumber
 */
static AttrNumber
GetHiddenAttributeNumber(Oid reloid)
{
	AttrNumber attno = InvalidAttrNumber;
	Relation relation = table_open(reloid, AccessShareLock);
	TupleDesc tupdesc = RelationGetDescr(relation);

	for (int i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
		if (attr->attisdropped)
			continue;

		if (strcmp(NameStr(attr->attname), "_hap_hidden_attribute") == 0)
		{
			attno = i + 1;
			break;
		}
	}

	table_close(relation, AccessShareLock);

	return attno;
}

/*
 * PoplulateSubstitutionCache
 */
static HapSubstitutionCache *
PoplulateSubstitutionCache(HapSubstitutionInfo* info)
{
	HapSubstitutionCache *cache;
	int shift;
	HapHiddenAttrDesc *desc = info->target_desc;
	char *data_store = NULL;
	int data_size = 0;

	/* Allocate substitution cache from array. */
 	cache = AllocateSubstitutionCache();

	Assert(desc != NULL);

	cache->start_bit = desc->startbit;
	cache->end_bit = desc->endbit;
	cache->bit_size = desc->bitsize;
	cache->start_byte = BitOffsetToByteOffset(desc->startbit);
	cache->end_byte = BitOffsetToByteOffset(desc->endbit);

	hap_populate_for_substitution(info, &data_store, &data_size);
	Assert(data_store);

	/* Set data store */
	cache->encoding_data_store = data_store;
	cache->data_size = data_size;

	/* Set info for fast decoding */
	if (cache->start_byte == cache->end_byte)
	{
		int lshift = BitsInByte - 1 - (cache->end_bit % BitsInByte);

		cache->is_fast_decoding = true;
		cache->mask = ((1ULL << (cache->bit_size)) - 1) << lshift;
		cache->rshift = lshift;
	}
	else
	{
		cache->is_fast_decoding = false;
	}

	return cache;
}

/* 
 * MakeVar
 */
static Var *
MakeVar(Index relid, AttrNumber attno)
{
	Var *var = makeNode(Var);

	var->varno = relid;
	var->varattno = attno;
	var->vartype = BYTEAOID;
	var->vartypmod = -1;
	var->varlevelsup = 0;
	var->varnosyn = relid;
	var->varattnosyn = attno;
	var->location = -1;

	return var;
}

/*
 * MakeConst
 */
static Const *
MakeConst(Index cacheIdx)
{
	Const *con = makeNode(Const);

	con->consttype = INT4OID;
	con->consttypmod = -1;
	con->constcollid = 0;
	con->constlen = 4;
	con->constvalue = cacheIdx;
	con->constisnull = false;
	con->constbyval = true;
	con->location = -1;

	return con;
}

/*
 * MakeFuncExprForSubstitution
 *		We construct the function expression tree using substitution information
 *		and a substitution cache.
 */
FuncExpr *
MakeFuncExprForSubstitution(HapSubstitutionInfo *info, HapSubstitutionCache *cache)
{
	FuncExpr *expr = makeNode(FuncExpr);
	Var *arg1;
	Const *arg2;
	Index hidden_attribute_attno;

	expr->funcid = 8000; /* OID of 'hap_decode_to_original_value' */
	expr->funcresulttype = info->origin_vartype; /* original var's type */
	expr->funcretset = false;
	expr->funcvariadic = false;
	expr->funcformat = COERCE_EXPLICIT_CALL;
	expr->funccollid = info->origin_varcollid; /* original var's collid */
	expr->inputcollid = 0;
	expr->location = 0;
	expr->args = NIL;

	/* We get hidden attribute number in the relation. */
	hidden_attribute_attno = GetHiddenAttributeNumber(info->target_desc->relid);

	/* 
	 * We utilize the hidden attribute of the target relation as the first 
	 * parameter.
	 */
	arg1 = MakeVar(info->target_relid, hidden_attribute_attno);

	/*
	 * We utilize the cache key as the second parameter.
	 */
	arg2 = MakeConst(cache->cache_idx);

	/* Append items. */
	expr->args = lappend(expr->args, arg1);
	expr->args = lappend(expr->args, arg2);

	return expr;
}

/*
 * hap_decode_to_original_value
 *		The first parameter is the hidden attribute of the relation, and the 
 *		second parameter is a cache key. We decode the propagated hidden 
 *		attribute to its original value.
 */
Datum
hap_decode_to_original_value(PG_FUNCTION_ARGS)
{
	uint64_t dataStoreIdx;
	HapSubstitutionCache* cache;
	bytea *hiddenAttr = PG_GETARG_BYTEA_PP(0);
	int cache_idx = PG_GETARG_INT64(1);
	uint8_t *data = (uint8 *) VARDATA_ANY(hiddenAttr);

	/* Get substitution cache using a cache key. */
	cache = GetSubstitutionCache(cache_idx);

	if (cache->is_fast_decoding)
	{
		uint64_t index = 
			(*(data + cache->start_byte) & cache->mask) >> cache->rshift;

		/* Fast path for decoding. */

		dataStoreIdx = index;
	}
	else
	{
		int encoded = 0;
		uint16 encoding_value = 0;

		/* Slow path for decoding. */

		for (int byteidx = cache->end_byte; byteidx >= cache->start_byte; 
																	byteidx--)
		{
			unsigned int tmp_value;
			uint8 *byte = data + byteidx;
			uint8 bitmask = 0xff, rshift = 0, lshift = 0;

			if (byteidx == cache->start_byte)	
			{
				rshift = cache->start_bit & 7;
				bitmask &= 0xff >> rshift;
			}

			if (byteidx == cache->end_byte)
			{
				lshift = 7 - (cache->end_bit & 7);
				bitmask &= 0xff << lshift;
			}	

			tmp_value = (*byte & bitmask) >> lshift;
			tmp_value <<= encoded;
			encoding_value |= tmp_value;
			encoded += (8 - lshift);
		}

		dataStoreIdx = encoding_value;
	}

	/* Return target element within data store. */
	PG_RETURN_DATUM((Datum) CacheGetDataPointer(cache, dataStoreIdx));
}

#endif /* HAP_REDUCTION */
