This README does not provide an overview of the LOCATOR project but focuses on the implementation details. The first part is hidden attribute, implemented in the src/backend/locator/hap. The second part is processing columnar slot, implemented in the src/backend/locator/executor and existing PostgreSQL functions with #ifdef LOCATOR. The third part is catalog management for the LOCATOR project (PostgreSQL-style catalogs, not the ex-catalog mentioned in the paper). This README describes hidden attribute and columnar slot and explains the required catalogs when necessary.

# Hidden attribute

Hidden attributes are encoded attributes of ancestor tables (dimension tables) in a foreign key relationship. When a new tuple is inserted into the child table it receives the encoded values from its parent tuples through a foreign key check and appends them at the end of the child tuple. Parent tuples themselves may have obtained encoded values from their own parent tuples. So the child don't have to perform joins across all ancestor tables to retrieve these values.

Attributes are encoded using dictionary encoding and bit-packing. The dictionary is created as a PostgreSQL-style table during the encoding process for the dimension table, with the attribute values of the dimension table becoming entries in the dictionary. These entries are then stored as hidden attributes in a variable-length byte array, where the dictionary's entry IDs are bit-packed for efficient storage.

Encoding is performed by calling the built-in function *locator_hap_encode*. The example below represents encoding the r_name attribute of the region table in the public namespace. Each piece of information is separated by a dot (.).
```
> SELECT locator_hap_encode('public.region.r_name');
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
This query performs three operations. First, it identifies the distinct values of the attribute being encoded and generates a materialized view that assigns IDs to those values. Second, it calculates the cardinality of the encoded values and calls the built-in function *hap_build_hidden_attribute_desc* to update the catalog. Finally, it calls the built-in function *hap_encode_to_hidden_attribute* to add the encoded values into the hidden attribute.
