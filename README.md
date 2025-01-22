This repository archives the HAP module, extracted from the [LOCATOR](https://github.com/snu-dbxlab/LOCATOR) project. The module is located in src/backend/hap, and modifications to existing PostgreSQL functions are marked with the HAP_HOOK keyword and #ifdef HAP. The name HAP is an abbreviation of Hidden Attribute Partitioning, but partitioning is not enforced. In fact, partitioning is handled by [LOCATOR](https://github.com/snu-dbxlab/LOCATOR)'s logic, not HAP.

This README explains implementation details of HAP. It is divided into four main categories: (1) the creation and encoding of HAP tables, (2) retrieving encoded values during the insert process via foreign key checks, (3) the process of pushing down predicates on ancestor tables to hidden attributes in child tables, (4) the techniques used in the [LOCATOR](https://github.com/snu-dbxlab/LOCATOR) project to find partitions matching the predicates.

# Hidden attribute

Hidden attributes are encoded attributes of ancestor tables (dimension tables) in a foreign key relationship. When a new tuple is inserted into the child table it receives the encoded values from its parent tuples through a foreign key check and appends them at the end of the child tuple. Parent tuples themselves may have obtained encoded values from their own parent tuples. So the child don't have to perform joins across all ancestor tables to retrieve these values.

Attributes are encoded using dictionary encoding and bit-packing. The dictionary is created as a PostgreSQL-style table during the encoding process for the dimension table, with the attribute values of the dimension table becoming entries in the dictionary. These entries are then stored as hidden attributes in a variable-length byte array, where the dictionary's entry IDs are bit-packed for efficient storage.

### CREATE TABLE

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
The above pseudocode represents the creation of an HAP table. First the HAP access method must be registered. This access method will triggers the DefineRelation function to hook into HAP's logic. At this hook function, a hidden attribute is added as the last attribute of the table, and the table is registered in the pg_hap catalog. The pg_hap catalog records the total bit size of the hidden attribute (hapbitsize) and how many attributes are encoded in the hidden attribute (hapdesccount). This information is aggregated by checking the pg_hap entries of the tables referenced by the new table through foreign keys.

### Encoding

Encoding is performed by calling the built-in function hap_encode(). The example below represents encoding the *r_name* attribute of the *region* table in the *public* namespace. Each piece of information is separated by a dot (.).
```
> SELECT hap_encode('public.region.r_name');
```
```
/* src/include/catalog/pg_proc.dat */
{ oid => '4549', descr => 'encode attribute to hidden attribute and propagate it',
  proname => 'hap_encode', provolatile => 's',
  prorettype => 'text', proargtypes => 'text',
  prosrc => 'hap_encode' }
```

This built-in function internally executes the following query.
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
This query performs three operations. First, it identifies the distinct values of the attribute being encoded and generates a materialized view that assigns IDs to those values. Second, it calculates the cardinality of the encoded values and calls the built-in function hap_build_hidden_attribute_desc() to update the catalog. Finally, it calls the built-in function hap_encode_to_hidden_attribute() to add the encoded values into the hidden attribute.

The built-in function hap_build_hidden_attribute_desc() updates the pg_hap_hidden_attribute_desc, pg_hap_encoded_attribute, and pg_hap catalogs. This function set the hapencoded flag in the pg_hap entry to indicate that the table is a target for encoding.

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
pg_hap_hidden_attribute_desc stores information about the encoded attributes for all tables. This includes not only the dimension tables that are the source of the encoding but also the lower-level tables that inherit the encoded attributes through foreign keys. For example, if *r_name* is encoded in the *region* table, the hidden attribute of *region* must know which bit position and how many bits it occupies. Similarly, the hidden attribute of *nation*, a child table of *region*, must also know the position and size of the bits where *r_name* is encoded within the *nation*'s hidden attribute. This catalog contains such information.

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

# Foriegn key check

# Predicate pushdown

# Partition map
