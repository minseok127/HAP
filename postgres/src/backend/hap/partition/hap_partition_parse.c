/*-------------------------------------------------------------------------
 *
 * hap_partition_parse.c
 *	  Parse the partition key reloption.
 *
 *
 * IDENTIFICATION
 *	  src/backend/hap/planner/hap_partition_parse.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/namespace.h"
#include "utils/lsyscache.h"

#include "hap/hap_catalog.h"
#include "hap/hap_partition.h"

#define MAX_QUOTED_NAME_LEN (64*2+3)

#define PARSE_START				1
#define PARSE_ATTRIBUTE_NAME	2
#define PARSE_DELIMITERS		3
#define PARSE_PROPAGATE_PATH	4
#define PARSE_END				5

/*
 * Skip ' ' or '\n'.
 */
static void
HapSkipWhiteSpace(char **str)
{
	while (**str == ' ' || **str == '\n')
		(*str)++;
}

/*
 * Extract namespace.relname.attrname
 */
static void
HapExtractNames(char **name, char *namespace, char *relname, char *attrname)
{
	char *src = *name;

	/* Extract namespace */
	while (*src && *src != '.')
		*namespace++ = *src++;
	*namespace = '\0';
	src++;

	/* Extract relname */
	while (*src && *src != '.')
		*relname++ = *src++;
	*relname ='\0';
	src++;

	/* Extract attrname */
	while (*src && *src != ' ' && *src != '\n' && *src != '(')
		*attrname++ = *src++;
	*attrname = '\0';

	/* Move the offset to the current */
	*name = src;
}

/*
 * HapParsePropagatePath
 *		Parse propagate path string and return oid list of the relations.
 *
 * The given string has propagation path like this:
 *		table1->table2->table3 ...
 *
 * Parse it and get the oid of each relation. Add them to the returning list.
 */
static List *
HapParsePropagatePath(char **str, Oid relnamespace)
{
	char relname[MAX_QUOTED_NAME_LEN];
	char *n = relname, *src = *str;
	List *oid_list = NIL;
	Oid relid;

parse_relname:

	while (*src && *src != '-' && *src != ' ' && *src != '\n' && *src != ')')
		*n++ = *src++;
	*n = '\0';

	relid = get_relname_relid(relname, relnamespace);
	if (relid == InvalidOid)
		elog(ERROR,
			 "invalid hidden partition key relation");

	oid_list = lappend_oid(oid_list, relid);

	/* Skip white space */
	HapSkipWhiteSpace(&src);

	/* Check propagate path delimiter */
	if (*src && *src == '-')
	{
		src++;

		/* Move to the next relation */
		if (*src && *src == '>')
		{
			src++;
			n = relname;
			HapSkipWhiteSpace(&src);
			goto parse_relname;
		}

		/* Not a "->" */ 
		elog(ERROR, "invalid hidden partition key delimiter");
	}

	/* Move the offset to the current */
	*str = src;

	return oid_list;
}

/*
 * HapAddPartKey
 *		Allocate HapPartitionKey and add it to the statement.
 */
static void
HapAddPartKey(HapPartCreateStmt *stmt,
			  Oid relid, AttrNumber attrnum, List *propagate_oid_list)
{
	HapPartKey *partkey = palloc0(sizeof(HapPartKey));
	int16 ancestor_descid, descendant_descid;
	List *tmp = NIL;

	partkey->propagate_oid_list = propagate_oid_list;
	partkey->dimension_relid = relid;
	partkey->dimension_attrnum = attrnum;

	/* Get the hidden attribute descriptor ids */
	ancestor_descid = HapGetDimensionHiddenAttrDescid(relid, attrnum);
	descendant_descid = HapGetDescendantHiddenAttrDescid(propagate_oid_list,
														 ancestor_descid,
														 false);
	partkey->dimension_descid = ancestor_descid;
	partkey->target_descid = descendant_descid;

	/* Add it to the statement */
	stmt->partkey_list = lappend(stmt->partkey_list, partkey);
}

/*
 * HapParsePartKeyString
 *		Parse hidden partition key string.
 *
 * The hidden partition key string has a format like this:
 * 		'{[encoded attribute name] ([propagation path]), ...}'
 *
 * For example, let's assume that there are two encoded attributes named
 * col1 in table tbl1 and col2 in table tbl2, both table's namespace is public.
 *
 * h1 is propagated along the following path:
 *		tbl1 -> tbl3 -> tbl4 (partitioning table)
 *
 * h2 is propagated along the following path:
 *		tbl2 -> tbl5 -> tbl4 (partitioning table)
 *
 * Then the string should be written as:
 *		'{
 *			public.tbl1.col1 (tbl1->tbl3->tbl4),
 *			public.tbl2.col2 (tbl2->tbl5->tbl4)
 *		 }'
 *
 * Note that white spaces are ignored.
 */
void
HapParsePartKeyStr(HapPartCreateStmt *stmt, char *keystr)
{
	char namespace[MAX_QUOTED_NAME_LEN];
	char relname[MAX_QUOTED_NAME_LEN];
	char attrname[MAX_QUOTED_NAME_LEN];
	int parsestate = PARSE_START;
	List *propagate_oid_list;
	Oid relnamespace, relid;
	AttrNumber attrnum;

	for (;;)
	{
		switch (parsestate)
		{
			case PARSE_START:
				/* Skip white space */
				HapSkipWhiteSpace(&keystr);

				/* First character must be '{' */
				if (*keystr != '{')
					elog(ERROR,
						 "invalid hidden partition key start");

				/* Move to the next stage */
				keystr++;				
				parsestate = PARSE_ATTRIBUTE_NAME;
				break;
			case PARSE_ATTRIBUTE_NAME:
				/* Skip white space */
				HapSkipWhiteSpace(&keystr);

				/* Extract names */
				HapExtractNames(&keystr, namespace, relname, attrname);

				/* Get the oid of relation */
				relnamespace = get_namespace_oid(namespace, false);
				relid = get_relname_relid(relname, relnamespace);
				if (relid == InvalidOid)
					elog(ERROR,
						 "invalid hidden partition key relation");

				/* Get the attribute number */
				attrnum = get_attnum(relid, attrname);
				if (attrnum == InvalidAttrNumber)
					elog(ERROR,
						 "invalid hidden partition key attribute");

				/* Move to the next stage */
				parsestate = PARSE_DELIMITERS;
				break;
			case PARSE_DELIMITERS:
				/* Skip white space */
				HapSkipWhiteSpace(&keystr);

				/* If the delimiter is '(', start parsing propagate path */
				if (*keystr == '(')
				{
					parsestate = PARSE_PROPAGATE_PATH;
					keystr++;
				}
				/* If the delimiter is ')', check the next character */
				else if (*keystr == ')')
				{
					/* Move to the next character */
					keystr++;

					/* Skip white space (if there is no space, do not move)  */
					HapSkipWhiteSpace(&keystr);

					/* We meet comma, move to the next partition key */
					if (*keystr == ',')
					{
						parsestate = PARSE_ATTRIBUTE_NAME;
						keystr++;
					}
					/* There is no more partition key */
					else
						parsestate = PARSE_END;
				}
				else
					elog(ERROR,
						 "invalid hidden partition key delimiter");

				break;
			case PARSE_PROPAGATE_PATH:
				/* Skip white space */
				HapSkipWhiteSpace(&keystr);

				/* Get the propagate path */
				propagate_oid_list = HapParsePropagatePath(&keystr,
														   relnamespace);

				if (list_length(propagate_oid_list) < 2)
					elog(ERROR,
						 "invalid hidden partition key propagate list");
				if (llast_oid(propagate_oid_list) != stmt->target_relid)
					elog(ERROR,
						 "invalid hidden partition key propagate list");

				/* Build Partition key and save it to the statement */
				HapAddPartKey(stmt, relid, attrnum, propagate_oid_list);

				/* Move to the next stage */
				parsestate = PARSE_DELIMITERS;
				break;
			case PARSE_END:
				/* Skip white space */
				HapSkipWhiteSpace(&keystr);

				/* Last character must be '}' */
				if (*keystr != '}')
					elog(ERROR,
						 "invalid hidden partition key end");

				/* Exit */
				return;
			default:
				elog(ERROR,
					 "invalid hidden partition key parse state: %d\n",
					 parsestate);
		}
	}
}

