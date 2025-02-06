# Hidden attribute

Hidden attribute contains encoded values of ancestor tables' attributes in a foreign key relationship. When a new tuple is inserted into the child table it receives the encoded values from its parent tuples through foreign key checks and appends them at the end of the child tuple. Parent tuples themselves may have obtained encoded values from their own parent tuples. So the child don't have to perform joins across all ancestor tables to retrieve these values during the insert operation. Hidden attribute allows predicates on ancestor tables to be converted into predicates on descendant tables. This reduces the total number of tuples processed by the query because the tuples are filtered before the join, not during the join.

Attributes are encoded using dictionary encoding and bit-packing. The dictionary is created as a PostgreSQL-style table during the encoding process for the dimension table, with the attribute values of the dimension table becoming entries in the dictionary. These entries are stored in the hidden attribute, a variable-length byte array where the dictionary's entry IDs are bit-packed for efficient storage.

This repository archives the HAP module, extracted from the [LOCATOR](https://github.com/snu-dbxlab/LOCATOR) project. The module is located in src/backend/hap, and modifications to existing PostgreSQL functions are marked with the HAP_HOOK keyword and #ifdef HAP. The name HAP is an abbreviation of Hidden Attribute Partitioning, but partitioning is not enforced. In fact, partitioning is handled by LOCATOR's logic, not HAP.

This README explains implementation details of HAP. It is divided into five main categories: (1) creating HAP tables, (2) encoding hidden attribute, (3) retrieving encoded values during the insert process, (4) converting predicates on ancestor tables into predicates on hidden attributes in descendant tables, (5) the techniques used in the LOCATOR project to find partitions matching the predicates.

# CREATE TABLE

```
> CREATE ACCESS METHOD hap TYPE TABLE HANDLER haphandler;
> CREATE TABLE test ( ... ) USING hap;
```

```
HAP_HOOK(DefineRelation) 
|
-- HAP_HOOK_COND(DefineRelation)
    |
    -- if access method is hap
    |    |
    |    -- HAP_HOOK_BODY(DefineRelation)
    |        |
    |        -- Append hidden attribute as the last column (_hap_hidden_attribute)
    |        |
    |        -- Add the new table into the pg_hap entry
    |        |
    |        -- Original DefineRelation()
    |
    -- else
        |
        -- Original DefineRelation()
```
```
/* include/catalog/pg_hap.h */
CATALOG(pg_hap,9999,HapRelationId)
{
	Oid		haprelid		BKI_LOOKUP(pg_class);
	Oid		happartmapid		BKI_DEFAULT(0);
	Oid		haprelnamespace;
	int16		hapbitsize;
	int16		hapdesccount;
	bool		hapencoded		BKI_DEFAULT(f);
	NameData	haprelname;
} FormData_pg_hap;

DECLARE_UNIQUE_INDEX_PKEY(pg_hap_haprelid_index,9998,HapRelIdIndexId,on pg_hap using btree(haprelid oid_ops));

DECLARE_UNIQUE_INDEX(pg_hap_haprelname_nsp_index,9997,HapNameNspIndexId,on pg_hap using btree(haprelname name_ops, haprelnamespace oid_ops));
```

The above pseudocode represents the creation of an HAP table. First the HAP access method must be registered. This access method will triggers the DefineRelation function to hook into HAP's logic. At this hook function, a hidden attribute is added as the last attribute of the table, and the table is registered in the pg_hap catalog. This catalog records the total bit size of the hidden attribute (hapbitsize) and how many attributes are encoded in the hidden attribute (hapdesccount). This information is aggregated by checking the pg_hap entries of the parent tables referenced by the new table.

# Encoding

Encoding performs updates on all existing tuples. Therefore, it is recommended to encode when only the tuples in the dimension tables exist, before generating data for the fact tables and running the OLTP workload.

### 1. hap_encode()

```
/* src/include/catalog/pg_proc.dat */
{ oid => '4549', descr => 'encode attribute to hidden attribute and propagate it',
  proname => 'hap_encode', provolatile => 's',
  prorettype => 'text', proargtypes => 'text',
  prosrc => 'hap_encode' }
```
```
> SELECT hap_encode('public.region.r_name');
```

The example above represents encoding the *r_name* attribute of the *region* table in the *public* namespace. Each piece of information is separated by a dot (.). This built-in function internally executes the following query.

```
/* -------------
 * The encoding query
 *	DO $$
 *	DECLARE tmparray text[] = '{}'; filterarray text[] = '{}';
 *			valtype text; filter text;
 *			cardinality int2; descid int2; encode_table_oid oid;
 *	BEGIN
 *		CREATE MATERIALIZED VIEW
 *		__hap_<relname>_<attrname>_encode_table AS
 *		SELECT	<attrname>,
 *				row_number() over (order by <attrname>) - 1 AS value
 *		FROM 	(SELECT distinct(<attrname>) as <attrname>
 *				 FROM <namespace>.<relname>) AS t;
 *
 *		SELECT '__hap_<relname>_<attrname>_encode_table'::regclass::oid
 *		INTO encode_table_oid;
 *
 *		SELECT array_cat(tmparray, array_agg((<attrname>))::text[])
 *		INTO tmparray
 *		FROM __hap_<relname>_<attrname>_encode_table;
 *		cardinality := cardinality(tmparray);
 *
 *		IF cardinality = 0 THEN
 *			RAISE EXCEPTION 'cardinality is 0';
 *		ELSEIF cardinality > 256 THEN
 *			RAISE EXCEPTION 'overflow';
 *		END IF;
 *
 *		SELECT pg_typeof(<attrname>)
 *		INTO valtype
 *		FROM <namespace>.<relname>;
 *
 *		IF valtype = 'character' THEN
 *			valtype := 'text';
 *		END IF;
 *
 *		SELECT hap_build_hidden_attribute_desc(
 *					<namepsace>.<relname>.<attrname>, cardinality,
 *					encode_table_oid)
 *		INTO descid;
 *
 *		FOREACH filter IN ARRAY tmparray LOOP
 *			filterarray := array_append(filterarray,
 *							concat('''', filter, '''', ':'', valtype));
 *		END LOOP;
 *
 *		PERFORM hap_encode_to_hidden_attribute(
 *					<namespace>.<relname>.<attrname>,
 *					descid, filterarray);
 *	END;
 *	$$;
 * -------------
 */
```

This query performs three operations. First, it identifies the distinct values of the attribute being encoded and generates a materialized view that assigns IDs to those values. Second, it calculates the cardinality of the encoded values and calls the built-in function hap_build_hidden_attribute_desc() to update catalogs. Finally, it calls the built-in function hap_encode_to_hidden_attribute() to add the encoded values into the hidden attribute.

### 2. hap_build_hidden_attribute_desc()

```
/* src/include/catalog/pg_proc.dat */
{ oid => '4550', descr => 'build hidden attribute descriptor',
  proname => 'hap_build_hidden_attribute_desc', provolatile => 's',
  prorettype => 'int2', proargtypes => 'text int2 oid',
  prosrc => 'hap_build_hidden_attribute_desc' }
```

The built-in function hap_build_hidden_attribute_desc() updates the pg_hap_hidden_attribute_desc, pg_hap_encoded_attribute, and pg_hap catalogs. The pg_hap catalog was explained earlier. Here, the hapencoded field in pg_hap is set to indicate that this table is an encoded dimension table.

```
/* include/catalog/pg_hap_hidden_attribute_desc.h */
CATALOG(pg_hap_hidden_attribute_desc,9991,HapHiddenAttributeDescRelationId)
{
	Oid		haprelid		BKI_LOOKUP(pg_class);
	Oid		hapconfrelid		BKI_LOOKUP(pg_class);
	int16		hapstartbit;
	int16		hapbitsize;
	int16		hapdescid;
	int16		hapconfdescid;
	int16		happartkeyidx;
} FormData_pg_hap_hidden_attribute_desc;

DECLARE_UNIQUE_INDEX_PKEY(pg_hap_hidden_attribute_desc_relid_descid,9989,HapHiddenAttributeDescRelidDescidIndexId,on pg_hap_hidden_attribute_desc using btree(haprelid oid_ops, hapdescid int2_ops));

DECLARE_UNIQUE_INDEX(pg_hap_hidden_attribute_desc_relid_confrelid_confdescid,9990,HapHiddenAttributeDescRelidConfrelidConfdescidIndexId,on pg_hap_hidden_attribute_desc using btree(haprelid oid_ops, hapconfrelid oid_ops, hapconfdescid int2_ops));
```
```
HapInsertHiddenAttrDesc
|
-- HapPrepareNewHiddenAttrDesc
|
-- __HapInsertHiddenAttrDesc
|
-- HapPropagateHiddenAttrDesc
	|
	-- conrelOids = HapGetReferencingRelIds
	|
	-- foreach conrelOids
		|
		-- HapPrepareNewHiddenAttrDesc
		|
		-- __HapInsertHiddenAttrDesc
		|
		-- HapPropagateHiddenAttrDesc /* recursive */

```

The pg_hap_hidden_attribute_desc catalog stores information about the encoded attributes for all tables. This includes not only the dimension tables that are the source of the encoding but also the lower-level tables that inherit the encoded attributes through foreign keys. For example, if *r_name* is encoded in the *region* table, the hidden attribute of *region* must know which bit position and how many bits are used for *r_name*. Similarly, the hidden attribute of *nation*, a child table of *region*, must also know the position and size of the bits where *r_name* is encoded within the *nation*'s hidden attribute. The pseudocode above illustrates this recursive process.

```
/* include/catalog/pg_hap_encoded_attribute.h */
CATALOG(pg_hap_encoded_attribute,9988,HapEncodedAttributeRelationId)
{
	Oid	haprelid		BKI_LOOKUP(pg_class);
	Oid	hapencodetable;
	int16	hapattrnum;
	int16	hapdescid;
	int32	hapcardinality;
} FormData_pg_hap_encoded_attribute;

DECLARE_UNIQUE_INDEX_PKEY(pg_hap_encoded_attribute_relid_attrnum,9987,HapEncodedAttributeRelidAttrnumIndexId,on pg_hap_encoded_attribute using btree(haprelid oid_ops, hapattrnum int2_ops));
```

The pg_hap_encoded_attribute catalog, unlike pg_hap_hidden_attribute_desc, contains only one entry per attribute targeted by hap_encode(). In other words, it represents information about the table and attribute being encoded, not the descendant tables. It provides the necessary information to access the dictionary that maps the encoding values for the attribute.

### 3. hap_encode_to_hidden_attribute()

```
/* src/include/catalog/pg_proc.dat */
{ oid => '4551', descr => 'encode specific value to hidden attribute and propagate it',
  proname => 'hap_encode_to_hidden_attribute', provolatile => 's',
  prorettype => 'void', proargtypes => 'text anyarray int2',
  prosrc => 'hap_encode_to_hidden_attribute' }
```
```
SELECT array_cat(tmparray, array_agg((<attrname>))::text[])
INTO tmparray
FROM __hap_<relname>_<attrname>_encode_table;
cardinality := cardinality(tmparray);

SELECT pg_typeof(<attrname>)
INTO valtype
FROM <namespace>.<relname>;

FOREACH filter IN ARRAY tmparray LOOP
	filterarray := array_append(filterarray,
				concat('''', filter, '''', ':'', valtype));
END LOOP;
```

Now we know where the encoded values should go within the hidden attribute, but we also need to know the values of the attributes being encoded and their data types before starting. The above queries handle this task. These queries are executed by hap_encode().

```
__hap_encode_to_hidden_attribute
|
-- HapUpdateRootHiddenAttr
|	|
|	-- HapMakeRootAttrFilterList
|	|
|	-- HapMakeCaseWhenSet
|	|
|	-- HapUpdateHiddenAttr
|
-- HapUpdateChildHiddenAttrRecurse
	|
	-- conrelOids = HapGetReferencingReldis
	|
	-- foreach conrelOids
		|
		-- HapUpdateChildHiddenAttr
		|	|
		|	-- HapMakeParentHiddenAttrFilterList
		|	|
		|	-- HapMakeCaseWhenSet
		|	|
		|	-- HapMakeFKeyCmpWhere
		|	|
		|	-- HapUpdateHiddenAttr
		|
		-- HapUpdateChildHiddenAttrRecures /* recursive */
```

The pseudocode above illustrates the encoding process. It is divided into functions with the root keyword and those with the child keyword. Here, root refers to the table targeted by hap_encode(), while child refers to the descendant tables.

The updates to the hidden attribute of the root table are based on the values and types of the encoded attributes identified earlier, generating an UPDATE query using a CASE WHEN statement. The updates for child tables are performed using an UPDATE query that joins with the parent table, applying CASE WHEN conditions based on the parent's hidden attribute and using foreign key match conditions to update the child's hidden attribute. Such updates proceed recursively to descendant tables along the foreign key relationships.

# Insert

After completing the encoding process, new tuples inherit encoded values through foreign key checks instead of performing joins with ancestor tables. To enable this, the foreign key check function must be replaced with HAP's function. The pseudocode below defines a function that creates triggers related to foreign key constraints during the table creation. This function uses HAP_HOOK and, if the table uses the HAP access method, it is hooked into HAP's logic.

```
/* src/include/catalog/pg_proc.dat */
{ oid => '1568', descr => 'referential integrity FOREIGN KEY ... REFERENCES',
  proname => 'HAP_RI_FKey_check_before_ins', provolatile => 'v',
  prorettype => 'trigger', proargtypes => '',
  prosrc => 'HAP_RI_FKey_check_before_ins' },
```
```
HAP_HOOK(createForeignKeyCheckTriggers)
|
-- HAP_HOOK_COND(createForeignKeyCheckTriggers)
    |
    -- if access method is hap
    |	 |
    |	 -- HAP_HOOK_BODY(createForeignKeyCheckTriggers)
    |    	|
    |	 	-- insertTrigger = HapCreateFKCheckTrigger(on_insert=true)
    |	 	|	|
    |	 	|	-- fk_trigger->funcname = SystemFuncName("HAP_RI_FKey_check_before_ins")
    |	 	|	|
    |	 	|	-- fk_trigger->timing = TRIGGER_TYPE_BEFORE /* before trigger */
    |	 	|
    |	 	-- updateTrigger = HapCreateFKCheckTrigger(on_insert=false)
    |		|	|
    |	 	|	-- fk_trigger->funcname = SystemFuncName("RI_FKey_check_upd")
    |	 	|	|
    |	 	|	-- fk_trigger->timing = TRIGGER_TYPE_AFTER
    |	 	|
    |	 	-- HapInheritHiddenAttrDesc /* make new entries on pg_hap_hidden_attribute_desc */
    |
    -- else
         |
	 -- Original function
		|
         	-- insertTrigger = CreateFKCheckTrigger(on_insert=true)
	 	|	|
	 	|	-- fk_trigger->funcname = SystemFuncName("RI_FKey_check_ins")
	 	|	|
	 	|	-- fk_trigger->timing = TRIGGER_TYPE_AFTER
	 	|
	 	-- updateTrigger = CreateFKCheckTrigger(on_insert=false)
			|
			-- fk_trigger->funcname = SystemFuncName("RI_FKey_check_upd")
			|
			-- fk_trigger->timing = TRIGGER_TYPE_AFTER
```

There are two main changes:

First, the function of the insert trigger is changed to HAP_RI_FKey_check_before_ins, and it is set as a BEFORE trigger instead of an AFTER trigger. This ensures that the encoded values for the hidden attribute are determined before the heap_insert. Note that the foreign key check explicitly locks the parent row using SELECT ... FOR KEY SHARE, preventing other transactions from deleting or modifying the primary key after the BEFORE trigger has been successed.

Second, HapInheritHiddenAttrDesc is called to add new entries to pg_hap_hidden_attribute_desc. This allows tables that did not exist during the encoding process to have the metadata that determines where the encoded values should be placed within the hidden attribute and how many bits they occupy.

```
HAP_HOOK(ExecModifyTable)
|
-- HAP_HOOK_COND(ExecModifyTable)
	|
    	-- if access method is hap
	|    |
	|    -- if the operation is INSERT
	|    	|
	|    	-- HapGetInsertNewTuple /* reallocate hidden attribute */
	|    	|
	|    	-- HapExecInsert
	|		|
	|		-- HapExecForeignKeyCheckTriggers
	|		|	|
	|		|	-- foreach FKey check
	|		|		|
	|		|		-- parent_hidden_attribute = ExecCallTriggerFunc
	|		|		|	|
	|		|		|	-- HAP_RI_FKey_check_before_ins
	|		|		|
	|		|		-- HapDeconstructParentHiddenAttr
	|		|
	|		-- HapBuildHiddenAttr
	|		|
	|		-- table_tuple_insert
	|
	-- else
	    |
	    -- Original ExecModifyTable
		|
		-- if the operation is INSERT
			|
			-- ExecGetInsertNewTuple
			|
			-- ExecInsert
```

The pseudocode above represents the actual insert process for an HAP table. HAP_HOOK is used into ExecModifyTable, and in the insert case, ExecGetInsertNewTuple and ExecInsert are replaced with HapGetInsertNewTuple and HapExecInsert.

Since the user does not explicitly specify hidden attribute values in the insert query, the hidden attribute only has an array header (0byte for values). HapGetInsertNewTuple retrieves the actual size of the hidden attribute from pg_hap and reallocates the hidden attribute.

HapExecInsert calls HapExecForeignKeyCheckTriggers and HapBuildHiddenAttr before heap_insert. HapExecForeignKeyCheckTriggers performs foreign key checks on parent tables, calling the trigger function HAP_RI_FKey_check_before_ins, which returns the hidden attribute of the parent tuple. It then calls HapDeconstructParentHiddenAttr to create a mapping table that links the parent's hidden attribute to the child's hidden attribute. After all parent tuples are checked, HapBuildHiddenAttr populates the child tuple's hidden attribute using the mapping table.

# Predicate transformation and propagation

To transform predicates on ancestor tables into predicates on hidden attributes of descendant tables, a HAP_HOOK is placed in the query_planner function. If the query is a SELECT and enable_hap_planner is ON, HAP's logic is applied. The enable_hap_planner setting can be toggled using SET and defaults to true.

```
> SET enable_hap_planner = on;
> SET enable_hap_planner = off;
```
```
HAP_HOOK(query_planner)
|
-- HAP_HOOK_COND(query_planner)
	|
	-- if query is SELECT and enable_hap_planner is true
	|   |
	|   -- ... /* original query_planner's functions */
	|   |
	|   -- hap_planner
	|   |	|
	|   |	-- hap_check_dimension_table_existence
	|   |	|
	|   |	-- hap_propagate_hidden_attribute
	|   |		|
	|   |		-- hap_find_propagation_paths
	|   |		|
	|   |		-- hap_propagate_filter_predicates
	|   |			|
	|   |			-- hap_find_propagation_paths
	|   |			|
	|   |			-- hap_propagate_filter_predicates
	|   |
	|   -- ... /* original query_planner's functions */
	|
	-- else
	    |
	    -- Original query_planner
```

At the point when hap_planner is called, all possible subqueries have been merged into the main query, and the basic predicates have been distributed across the all tables. The first task of the hap_planner is checking whether any dimension tables targeted for encoding exist and whether there are predicates on attributes encoded with hap_encode. If no dimension table is present or there are no predicates that can be transformed into hidden attributes, it skips hap_propagate_hidden_attribute.

# Partition map
