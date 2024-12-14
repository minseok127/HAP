/*-------------------------------------------------------------------------
 *
 * hapencoding.c
 *	  routines to support hidden attribute encoding.
 *
 *
 * IDENTIFICATION
 *	  src/backend/hap/encoding/hapencoding.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/relation.h"
#include "access/table.h"
#include "access/tableam.h"
#include "catalog/dependency.h"
#include "catalog/namespace.h"
#include "catalog/indexing.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_class.h"
#include "catalog/pg_constraint.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "parser/parse_relation.h"
#include "utils/array.h"
#include "utils/arrayaccess.h"
#include "utils/builtins.h"
#include "utils/expandeddatum.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "utils/fmgroids.h"
#include "utils/fmgrprotos.h"

#include "hap/hap_am.h"
#include "hap/hap_catalog.h"
#include "hap/hap_encoding.h"

#define MAX_QUOTED_NAME_LEN (64*2+3)
#define BitOffsetToByteOffset(bit) (bit >> 3)

/*
 * Safely quote a given SQL name.
 */
static inline void
HapQuoteName(char *buffer, char *name)
{
	*buffer++ = '"';
	while(*name)
	{
		if (*name == '"')
			*buffer++ = '"';
		*buffer++ = *name++;
	}
	*buffer++ = '"';
	*buffer = '\0';
}

/*
 * The given name is separated by dot ('.').
 * Separate it.
 */
static inline void
HapSplitNameByDot(char **name, char *desc)
{
	char *src = *name;

	while (*src && *src != '.')
		*desc++ = *src++;
	*desc = '\0';
	*name = ++src;
}

/*
 * Extract namespace/relname/attribute from @name.
 */
static inline void
HapExtractNames(char *name, char *namespace, char *relname, char *attrname)
{
	/* Extract namespace */
	if (namespace)
		HapSplitNameByDot(&name, namespace);

	/* Extract relname */
	if (relname)
		HapSplitNameByDot(&name, relname);

	/* Extract attrname */
	if (attrname)
		HapSplitNameByDot(&name, attrname);
}

/*
 * HapGetHiddenAttrByteInfo
 *		Using hidden attribute descriptor and byte index, get bitmask and shift
 *		information to descript the byte.
 *
 * Hidden attribute descriptor tells which bits are valid within the hidden
 * attribute. Through this, bitmask and bitshift information for interpreting a
 * specific byte can be known.
 *
 * Write the relevant information in the @mask and @shift.
 */
static void
HapGetHiddenAttrByteInfo(HapHiddenAttrDesc *desc, int byteidx,
						 uint8 *mask, uint8 *shift)
{
	uint8 bitmask = 0xff, rshift = 0, lshift = 0;

	if (byteidx == desc->startbyte)
	{
		rshift = desc->startbit & 7;
		bitmask &= 0xff >> rshift;
	}
	if (byteidx == desc->endbyte)
	{
		lshift = 7 - (desc->endbit & 7);
		bitmask &= 0xff << lshift;
	}

	*mask = bitmask; 
	*shift = lshift;
}

/*
 * HapMakeHiddenAttrDesc
 *		Make hidden attribute descriptor.
 *
 * Using the given relid and descriptor id, get the syscache and form hidden
 * attribute descriptor structure.
 *
 * This will be used to translate the hidden attribute.
 */
HapHiddenAttrDesc *
HapMakeHiddenAttrDesc(Oid relid, int16 descid)
{
	HapHiddenAttrDesc *desc = palloc0(sizeof(HapHiddenAttrDesc));
	Form_pg_hap_hidden_attribute_desc hapdesctup;
	int16 bitsize, startbit, partkeyidx;
	HeapTuple tuple;

	tuple = SearchSysCache2(HAPDESCRELID,
							ObjectIdGetDatum(relid), Int16GetDatum(descid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for hidden attribute descriptor");

	hapdesctup = (Form_pg_hap_hidden_attribute_desc) GETSTRUCT(tuple);
	bitsize = hapdesctup->hapbitsize;
	startbit = hapdesctup->hapstartbit;
	partkeyidx = hapdesctup->happartkeyidx;
	ReleaseSysCache(tuple);

	desc->relid = relid;
	desc->descid = descid;
	desc->bitsize = bitsize;
	desc->startbit = startbit;
	desc->endbit = startbit + bitsize - 1;
	desc->startbyte = BitOffsetToByteOffset(startbit);
	desc->endbyte = BitOffsetToByteOffset(desc->endbit);
	desc->partkeyidx = partkeyidx;

	return desc;
}

/*
 * HapExtractEncodingValueFromBytea
 *		Get the encoding value corresponding to the hidden attribute descriptor.
 *
 * Using the given relid and descriptor id, get the startbit and bitsize.
 * Then extract the encoding value from the bytea.
 *
 * Note that byte descriptors start from end byte.
 *
 * Return the extracted encodnig value.
 */
uint16
HapExtractEncodingValueFromBytea(HapHiddenAttrDesc *desc, bytea *hiddenattr)
{
	int encoded = 0, nbytes = VARSIZE_ANY_EXHDR(hiddenattr);
	uint8 *data = (uint8 *) VARDATA_ANY(hiddenattr);
	uint16 encoding_value = 0;

	Assert(desc->endbyte < nbytes && desc->startbyte >= 0);

	for (int byteidx = desc->endbyte; byteidx >= desc->startbyte; byteidx--)
	{
		uint8 mask, shift, *byte = data + byteidx;
		unsigned int tmp_value;		

		HapGetHiddenAttrByteInfo(desc, byteidx, &mask, &shift);
		tmp_value = (*byte & mask) >> shift;
		tmp_value <<= encoded;
		encoding_value |= tmp_value;
		encoded += (8 - shift);
	}

	return encoding_value;
}

/*
 * HapApplyEncodingValueToBytea
 *		Apply the encoding value of the given hidden attributge descriptor to
 *		the hidden attribute byte array.
 *
 * HapHiddenAttrDesc knows what value to apply to which bytes in bytea. Use this
 * to update the given hidden attribute (bytea).
 *
 * Note that byte descriptors start from end byte.
 */
void
HapApplyEncodingValueToBytea(HapHiddenAttrDesc *desc,
							 uint16 value, bytea *hiddenattr)
{
	int nbytes = VARSIZE_ANY_EXHDR(hiddenattr);
	uint8 *data = (uint8 *) VARDATA_ANY(hiddenattr);

	Assert(desc->endbyte < nbytes && desc->startbyte >= 0);

	for (int byteidx = desc->endbyte; byteidx >= desc->startbyte; byteidx--)
	{
		uint8 mask, shift, *byte = data + byteidx;
		uint16 encoding_value_mask;

		HapGetHiddenAttrByteInfo(desc, byteidx, &mask, &shift);
		encoding_value_mask = mask >> shift;
		*byte |= (value & encoding_value_mask) << shift;
		value >>= (8 - shift);
	}
}

/*
 * HapPrepareHintEncodingSPIPlan
 *		Prepare SPI plan to encode hidden attribute using "hint".
 *
 * We use plsql to encode hidden attribute. Prepare it.
 *
 * Note that this query will call the built-in functions such as
 * hap_build_hidden_attribute_desc() and hap_encode_to_hidden_attribute().
 */
static SPIPlanPtr
HapPrepareHintEncodingSPIPlan(Relation rel, char *namespace,
						  	  char *relname, char *attrname, char *hint)
{
	char namespacequoted[MAX_QUOTED_NAME_LEN];
	char relnamequoted[MAX_QUOTED_NAME_LEN];
	char attrnamequoted[MAX_QUOTED_NAME_LEN];
	StringInfoData querybuf;
	int save_sec_context;
	SPIPlanPtr qplan;
	Oid save_userid;

	Assert(hint != NULL);
	
	/* -------------
	 * The encoding query
	 *	DO $$
	 *	DECLARE tmparray text[] = '{}'; valtype text; filter text;
	 *			cardinality int2; descid int2; encoding int2;
	 *	BEGIN
	 *		SELECT array_cat(tmparray, array_agg(distinct(<attrname>))::text[])
	 *		INTO tmparray
	 *		FROM <namespace>.<relname>;
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
	 *					<namepsace>.<relname>.<attrname>, cardinality)
	 *		INTO descid;
	 *
	 *		encoding := 0;
	 *		FOREACH filter IN ARRAY tmparray LOOP
	 *			PERFORM hap_encode_to_hidden_attribute(
	 *					<namepsace>.<relname>.<attrname>, 
	 *					concat('''', filter, '''', '::', valtype),
	 *					encoding, descid);
	 *			encoding := encoding + 1;
	 *		END LOOP;
	 *	END;
	 *	$$;
	 * -------------
	 */
	initStringInfo(&querybuf);
	HapQuoteName(namespacequoted, namespace);
	HapQuoteName(relnamequoted, relname);
	HapQuoteName(attrnamequoted, attrname);
	appendStringInfo(&querybuf,
		"DO $$ "
			"DECLARE "
				"tmparray text[] = '{}'; valtype text; filter text; "
				"cardinality int2; descid int2; encoding int2; "
			"BEGIN "
				"SELECT array_cat(tmparray, array_agg(distinct(%s))::text[]) "
				"INTO tmparray "
				"FROM %s.%s; "
				"cardinality := cardinality(tmparray); "

				"IF cardinality = 0 THEN "
					"RAISE EXCEPTION 'hap_encode(): cardinality is 0'; "
				"ELSEIF cardinality > 256 THEN "
					"RAISE EXCEPTION 'hap_encode(): overflow'; "
				"END IF; "

				"SELECT pg_typeof(%s) "
				"INTO valtype "
				"FROM %s.%s; "

				"IF valtype = 'character' THEN "
					"valtype := 'text'; "
				"END IF; "

				"SELECT hap_build_hidden_attribute_desc("
						"'%s.%s.%s', cardinality) "
				"INTO descid; "

				"encoding := 0; "
				"FOREACH filter IN ARRAY tmparray LOOP "
					"PERFORM hap_encode_to_hidden_attribute("
						"'%s.%s.%s', concat('''', filter, '''', '::', valtype), "
						"encoding, descid); "
					"encoding := encoding + 1; "
				"END LOOP; "
			"END; "
		"$$;",
		attrnamequoted,
		namespacequoted, relnamequoted,
		attrnamequoted,
		namespacequoted, relnamequoted,
		namespace, relname, attrname,
		namespace, relname, attrname);

	/* Switch to proper UID to perform check as */
	GetUserIdAndSecContext(&save_userid, &save_sec_context);
	SetUserIdAndSecContext(RelationGetForm(rel)->relowner,
						   save_sec_context | SECURITY_LOCAL_USERID_CHANGE |
						   SECURITY_NOFORCE_RLS);
	
	/* Create a plan */
	qplan = SPI_prepare(querybuf.data, 0, NULL);

	if (qplan == NULL)
		elog(ERROR, "SPI_prepare returned %s for %s",
			 SPI_result_code_string(SPI_result), querybuf.data);

	/* Restore UID and security context */
	SetUserIdAndSecContext(save_userid, save_sec_context);

	return qplan;		
}

/*
 * HapPrepareEncodingSPIPlan
 *		Prepare SPI plan to encode hidden attribute.
 *
 * We use plsql to encode hidden attribute. Prepare it.
 *
 * Note that this query will call the built-in functions such as
 * hap_build_hidden_attribute_desc() and hap_encode_to_hidden_attribute().
 */
static SPIPlanPtr
HapPrepareEncodingSPIPlan(Relation rel, char *namespace,
						  char *relname, char *attrname)
{
	char namespacequoted[MAX_QUOTED_NAME_LEN];
	char relnamequoted[MAX_QUOTED_NAME_LEN];
	char attrnamequoted[MAX_QUOTED_NAME_LEN];
	StringInfoData querybuf;
	int save_sec_context;
	SPIPlanPtr qplan;
	Oid save_userid;
	
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
	initStringInfo(&querybuf);
	HapQuoteName(namespacequoted, namespace);
	HapQuoteName(relnamequoted, relname);
	HapQuoteName(attrnamequoted, attrname);
	appendStringInfo(&querybuf,
		"DO $$ "
			"DECLARE "
				"tmparray text[] = '{}'; filterarray text[] = '{}'; "
				"valtype text; filter text; "
				"cardinality int2; descid int2; encode_table_oid oid; "
			"BEGIN "
				"CREATE MATERIALIZED VIEW "
				"__hap_%s_%s_encode_table AS "
				"SELECT %s, row_number() over (order by %s) - 1 AS value "
				"FROM (SELECT distinct(%s) as %s FROM %s.%s) AS t; "

				"SELECT '__hap_%s_%s_encode_table'::regclass::oid "
				"INTO encode_table_oid; "

				"SELECT array_cat(tmparray, array_agg(%s)::text[]) "
				"INTO tmparray "
				"FROM __hap_%s_%s_encode_table;"
				"cardinality := cardinality(tmparray); "

				"IF cardinality = 0 THEN "
					"RAISE EXCEPTION 'hap_encode(): cardinality is 0'; "
				"ELSEIF cardinality > 256 THEN "
					"RAISE EXCEPTION 'hap_encode(): overflow'; "
				"END IF; "

				"SELECT pg_typeof(%s) "
				"INTO valtype "
				"FROM %s.%s; "

				"IF valtype = 'character' THEN "
					"valtype := 'text'; "
				"END IF; "

				"SELECT hap_build_hidden_attribute_desc("
						"'%s.%s.%s', cardinality, encode_table_oid) "
				"INTO descid; "

				"FOREACH filter IN ARRAY tmparray LOOP "
					"filterarray := array_append(filterarray, "
	 					"concat('''', filter, '''', '::', valtype)); "
				"END LOOP; "

				"PERFORM hap_encode_to_hidden_attribute("
							"'%s.%s.%s', filterarray, descid); "
			"END; "
		"$$;",
		relname, attrname,
		attrnamequoted, attrnamequoted,
		attrnamequoted, attrnamequoted, namespacequoted, relnamequoted,
		relname, attrname,
		attrnamequoted,
		relname, attrname,
		attrnamequoted,
		namespacequoted, relnamequoted,
		namespace, relname, attrname,
		namespace, relname, attrname);

	/* Switch to proper UID to perform check as */
	GetUserIdAndSecContext(&save_userid, &save_sec_context);
	SetUserIdAndSecContext(RelationGetForm(rel)->relowner,
						   save_sec_context | SECURITY_LOCAL_USERID_CHANGE |
						   SECURITY_NOFORCE_RLS);
	
	/* Create a plan */
	qplan = SPI_prepare(querybuf.data, 0, NULL);

	if (qplan == NULL)
		elog(ERROR, "SPI_prepare returned %s for %s",
			 SPI_result_code_string(SPI_result), querybuf.data);

	/* Restore UID and security context */
	SetUserIdAndSecContext(save_userid, save_sec_context);

	return qplan;		
}

/*
 * __hap_encode
 *		Help function for hap_encode().
 *
 * To encode hidden attribute, we use SPI to use plsql.
 */
static void
__hap_encode(char *namespace, char *relname, char *attrname, char *hint)
{
	Oid relnamespace = get_namespace_oid(namespace, false);
	Oid relid = get_relname_relid(relname, relnamespace);
	Snapshot crosscheck_snapshot;
	Snapshot test_snapshot;
	int save_sec_context;
	SPIPlanPtr qplan;
	Oid save_userid;
	int spi_result;
	Relation rel;

	/* Open the dimension table to get the relowner information */
	rel = table_open(relid, AccessShareLock);

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed at __hap_encode()");

	/* Make SPI plan */
	if (hint)
		qplan = HapPrepareHintEncodingSPIPlan(rel, namespace,
											  relname, attrname, hint);
	else
		qplan = HapPrepareEncodingSPIPlan(rel, namespace, relname, attrname);

	/* Set up proper snapshots */
	if (IsolationUsesXactSnapshot())
	{
		CommandCounterIncrement();
		test_snapshot = GetLatestSnapshot();
		crosscheck_snapshot = GetTransactionSnapshot();
	}
	else
	{
		test_snapshot = InvalidSnapshot;
		crosscheck_snapshot = InvalidSnapshot;
	}

	/* Switch to proper UID to perform check as */
	GetUserIdAndSecContext(&save_userid, &save_sec_context);
	SetUserIdAndSecContext(RelationGetForm(rel)->relowner,
						   save_sec_context | SECURITY_LOCAL_USERID_CHANGE |
						   SECURITY_NOFORCE_RLS);

	/* Close the relation before executing SPI */
	table_close(rel, AccessShareLock);

	/* Run the query */
	spi_result = SPI_execute_snapshot(qplan, NULL, NULL,
									  test_snapshot, crosscheck_snapshot,
									  false, true, 0);

	/* Restore UID and security context */
	SetUserIdAndSecContext(save_userid, save_sec_context);

	/* Check result */
	if (spi_result < 0)
		elog(ERROR, "SPI_execute_snapshot retuned %s at __hap_encode()",
			 SPI_result_code_string(spi_result));

	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed at __hap_encode()");
}

/*
 * hap_encode
 *		Encode the given attribute to the hidden attribute and propagate it.
 *
 * Translate the given argument to the namespace, relname, attribte name.
 * Using them, start hidden attribute encoding.
 *
 * Note that the format of the name received as a parameter must be
 * 'namespace.relname.attributename'.
 */
Datum
hap_encode(PG_FUNCTION_ARGS)
{
	char *name = text_to_cstring(PG_GETARG_TEXT_P(0));
	char namespace[MAX_QUOTED_NAME_LEN];
	char relname[MAX_QUOTED_NAME_LEN];
	char attrname[MAX_QUOTED_NAME_LEN];
	char result[MAX_QUOTED_NAME_LEN];

	/* Extract namespace, relname, and attribute name */
	HapExtractNames(name, namespace, relname, attrname);

	/* Start encoding */
	__hap_encode(namespace, relname, attrname, NULL);

	sprintf(result, "%s.%s.%s is successfully encoded",
			namespace, relname, attrname);
	PG_RETURN_TEXT_P(cstring_to_text(result));
}

/*
 * __hap_build_hidden_attribute_desc
 *		Workhorse for hap_build_hidden_attribute_desc().
 *
 * Check whether the given relation uses HAP or not. If it uses HAP, create a
 * new hidden attribute descriptor for that relation.
 *
 * To do this, we calculate how many bits the given cardinality requires.
 */
static int16
__hap_build_hidden_attribute_desc(Oid relid, AttrNumber attrnum,
								  int16 cardinality, Oid encode_table)
{
	ObjectAddress rel_addr, encode_table_addr;
	int bitsize = 0, cur_cardinality = 1;
	int16 descid;

	/* Check whether the given relation uses HAP or not */
	if (!HapCheckAmUsingRelId(relid))
		elog(ERROR, "hap_build_hidden_attribute_desc() can be used only for HAP tables");

	/* Calculate the bitsize */
	do
	{
		cur_cardinality *= 2;
		bitsize++;
	} while (cur_cardinality < cardinality);

	/* pg_hap_hidden_attribute_desc */
	descid = HapInsertHiddenAttrDesc(relid, bitsize);

	/* pg_hap_encoded_attribute */
	HapInsertEncodedAttribute(relid, encode_table,
							  attrnum, descid, cardinality);

	/* pg_hap */
	HapRelidMarkAsEncoded(relid);

	/* Set dependency to DROP encode table automatically */
	ObjectAddressSet(rel_addr, RelationRelationId, relid);
	ObjectAddressSet(encode_table_addr, RelationRelationId, encode_table);
	recordDependencyOn(&encode_table_addr, &rel_addr, DEPENDENCY_INTERNAL);

	return descid;
}

/*
 * hap_build_hidden_attribute_desc
 *		Add a new hidden attribute descriptor.
 *
 * Build a new hidden attribute descriptor which is the entry of
 * pg_hap_hidden_attribute_desc catalog. The entry has a startbit and bitsize
 * for the new hidden attribute.
 *
 * Note that this function propagates the new hidden attribute descriptor
 * through the foreign key relationship.
 *
 * Return the created hidden attribute descriptor id.
 */
Datum
hap_build_hidden_attribute_desc(PG_FUNCTION_ARGS)
{
	Oid relid, relnamespace, encode_table;
	char namespace[MAX_QUOTED_NAME_LEN];
	char relname[MAX_QUOTED_NAME_LEN];
	char attrname[MAX_QUOTED_NAME_LEN];
	int16 descid, cardinality;
	AttrNumber attrnum;
	char *name;

	/* Get arguments */
	name = text_to_cstring(PG_GETARG_TEXT_P(0));
	cardinality = PG_GETARG_INT16(1);
	encode_table = PG_GETARG_OID(2);

	/* Extract namespace, relname, and attribute name */
	HapExtractNames(name, namespace, relname, attrname);

	/* Represents namespace, relname and attribute name as a number */
	relnamespace = get_namespace_oid(namespace, false);
	relid = get_relname_relid(relname, relnamespace);
	attrnum = get_attnum(relid, attrname);

	/* Start building new hidden attribute descriptor */
	descid = __hap_build_hidden_attribute_desc(relid, attrnum,
											   cardinality, encode_table);

	PG_RETURN_INT16(descid);
}

/*
 * HapMakeHiddenAttrSET
 *		Make SET clause for updating hidden attribute.
 *
 * Since the hidden attribute SET clause concatenates new encoding values, we
 * should start with a low address.
 *
 * Note that the byte descriptors of the hidden attribute descriptor start with
 * a high address. So loop backward.
 */
static StringInfo
HapMakeHiddenAttrSET(char *relname, HapHiddenAttrDesc *desc, uint16 encoding)
{
	StringInfo setbuf = palloc0(sizeof(StringInfoData));
	int length = desc->endbyte - desc->startbyte + 1;
	char attrnamequoted[MAX_QUOTED_NAME_LEN];
	char relnamequoted[MAX_QUOTED_NAME_LEN];
	List *byte_stack = NIL;
	ListCell *lc;

	/* Quote hidden attribute */
	HapQuoteName(attrnamequoted, "_hap_hidden_attribute");
	HapQuoteName(relnamequoted, relname);
	initStringInfo(setbuf);

	/* Extract byte values from the encoding value */
	for (int byteidx = desc->endbyte; byteidx >= desc->startbyte; byteidx--)
	{
		uint8 mask, shift, byteval;
		uint16 encoding_value_mask;

		HapGetHiddenAttrByteInfo(desc, byteidx, &mask, &shift);
		encoding_value_mask = mask >> shift;
		byteval = (encoding & encoding_value_mask) << shift;

		/* Remember it */
		byte_stack = lappend_int(byte_stack, byteval);

		/* Remove encoded bits */
		encoding >>= (8 - shift);
	}

	/*
	 * If the startbit belongs to an existing byte, we have to use OR.
	 *
	 * case 1 => OR)
	 *   set_byte(<table>._hap_hidden_attribute, idx,
	 *				get_byte(<table>._hap_hidden_attribute, idx) | value)
	 *
	 * case 2 => append)
	 *   <table>._hap_hidden_attribute || set_byte('\000', 0, value)
	 *
	 */
	if (desc->startbit % 8 != 0)
	{
		appendStringInfo(setbuf,
						 "set_byte(%s.%s, %d, get_byte(%s.%s, %d) | %d) ",
						 relnamequoted, attrnamequoted,
						 BitOffsetToByteOffset(desc->startbit),
						 relnamequoted, attrnamequoted,
						 BitOffsetToByteOffset(desc->startbit),
						 llast_int(byte_stack));
	}
	else
	{
		appendStringInfo(setbuf,
						 "%s.%s || set_byte('\\000', 0, %d) ",
						 relnamequoted, attrnamequoted,
						 llast_int(byte_stack));
	}

	/*
	 * The lowest byte is already processed. So start from the second byte.
	 * Note that from this point, it is only an append.
	 */
	Assert(list_length(byte_stack) == length);
	for (int i = length - 2; i >= 0; i--)
	{
		appendStringInfo(setbuf,
						 "|| set_byte('\\000', 0, %d) ",
						 list_nth_int(byte_stack, i));
	}

	return setbuf;
}

/*
 * Prepare SPI plan for UPDATE.
 */
static SPIPlanPtr
HapPrepareUpdateSPIPlan(Relation rel, char *target,
						char *set, char *from, char *where)
{
	StringInfoData querybuf;
	int save_sec_context;
	SPIPlanPtr qplan;
	Oid save_userid;

	/* ------------
	 * The update query
	 *  UPDATE <target>
	 *  SET <set>
	 *  (FROM <from>)
	 *  (WHERE <where>);
	 * ------------
	 */
	initStringInfo(&querybuf);

	/* UPDATE */
	appendStringInfo(&querybuf, "UPDATE %s ", target);

	/* SET */
	appendStringInfo(&querybuf, "SET %s ", set);

	/* FROM is optional */
	if (from)
		appendStringInfo(&querybuf, "FROM %s ", from);

	/* WHERE is optional */
	if (where)
		appendStringInfo(&querybuf, "WHERE %s", where);

	/* Add ; */
	appendStringInfo(&querybuf, ";");

	/* Switch to proper UID to perform check as */
	GetUserIdAndSecContext(&save_userid, &save_sec_context);
	SetUserIdAndSecContext(RelationGetForm(rel)->relowner,
						   save_sec_context | SECURITY_LOCAL_USERID_CHANGE |
						   SECURITY_NOFORCE_RLS);
	
	/* Create a plan */
	qplan = SPI_prepare(querybuf.data, 0, NULL);

	if (qplan == NULL)
		elog(ERROR, "SPI_prepare returned %s for %s",
			 SPI_result_code_string(SPI_result), querybuf.data);

	/* Restore UID and security context */
	SetUserIdAndSecContext(save_userid, save_sec_context);

	return qplan;
}

/*
 * Common function for updating hidden attribute.
 */
static void
HapUpdateHiddenAttr(HapHiddenAttrDesc *desc,
					char *target, char *set, char *from, char *where)
{
	Snapshot crosscheck_snapshot;
	Snapshot test_snapshot;
	int save_sec_context;
	SPIPlanPtr qplan;
	Oid save_userid;
	int spi_result;
	Relation rel;

	/* Open the updating table to get the relowner information */
	rel = table_open(desc->relid, AccessShareLock);

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed at __hap_encode()");

	/* Make SPI plan */
	qplan = HapPrepareUpdateSPIPlan(rel, target, set, from, where);

	/* Set up proper snapshots */
	if (IsolationUsesXactSnapshot())
	{
		CommandCounterIncrement();
		test_snapshot = GetLatestSnapshot();
		crosscheck_snapshot = GetTransactionSnapshot();
	}
	else
	{
		test_snapshot = InvalidSnapshot;
		crosscheck_snapshot = InvalidSnapshot;
	}

	/* Switch to proper UID to perform check as */
	GetUserIdAndSecContext(&save_userid, &save_sec_context);
	SetUserIdAndSecContext(RelationGetForm(rel)->relowner,
						   save_sec_context | SECURITY_LOCAL_USERID_CHANGE |
						   SECURITY_NOFORCE_RLS);

	/* Close the relation before executing SPI */
	table_close(rel, AccessShareLock);

	/* Run the query */
	spi_result = SPI_execute_snapshot(qplan, NULL, NULL,
									  test_snapshot, crosscheck_snapshot,
									  false, true, 0);

	/* Restore UID and security context */
	SetUserIdAndSecContext(save_userid, save_sec_context);

	/* Check result */
	if (spi_result < 0)
		elog(ERROR, "SPI_execute_snapshot retuned %s at encoding",
			 SPI_result_code_string(spi_result));

	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed at encoding");
}

/*
 * HapMakeCaseWhenSET
 *		Write SQL to set hidden attribute using CASE WHEN clause.
 *
 * The given filter list is used to set conditions for the SET clause.
 * In each case, the encoding values are incremented sequentially from 0.
 */
static StringInfo
HapMakeCaseWhenSET(char *relname, HapHiddenAttrDesc *desc, List *filter_list)
{
	int encoding = 0;
	StringInfo set;
	ListCell *lc;

	Assert(list_length(filter_list) != 0);

	/* Initialize */
	set = palloc0(sizeof(StringInfoData));
	initStringInfo(set);
	appendStringInfo(set, "\"_hap_hidden_attribute\" = CASE ");

	/* Make SET clause */
	foreach(lc, filter_list)
	{
		char *filter_str = lfirst(lc);
		StringInfo encoding_clause;

		/* Make hidden attribute setting clause using the encoding value */
		encoding_clause = HapMakeHiddenAttrSET(relname, desc, encoding++);

		appendStringInfo(set,
						 "WHEN %s THEN %s ",
						 filter_str, encoding_clause->data);
	}

	/*
	 * Set corner cases to invaliad hidden attirbute.
	 * It can be used for error checking.
	 */
	appendStringInfo(set, "ELSE '\\000' END");

	return set;
}

/*
 * HapMakeRootAttrFilterList
 *		Make a list which has elements representing lvalue of CASE WHEN UPDATE.
 *
 * The given filter_list has the elements like this format:
 *		'abcd'::text ...
 *
 * To use this text in CASE WHEN UPDATE SQL, we have to process it like this:
 *		<table>.<attribute> = 'abcd'::text
 *
 * Process it and add it to the returing list.
 */
static List *
HapMakeRootAttrFilterList(char *relname, char *attrname,
						  List *filter_list)
{
	List *ret = NIL;
	ListCell *lc;

	foreach(lc, filter_list)
	{
		char *filter_str = lfirst(lc);
		StringInfo cmp;
	
		/* Initialize */
		cmp = palloc0(sizeof(StringInfoData));
		initStringInfo(cmp);

		/* Make clause */
		appendStringInfo(cmp,
						 "\"%s\".\"%s\" = %s ",
						 relname, attrname, filter_str);

		ret = lappend(ret, cmp->data);
	}

	return ret;
}

/*
 * HapUpdateRootHiddenAttr
 *		Update the hidden attribute of the table to be encoded.
 *
 * Update the hidden attribute of the dimension table. What makes this table
 * differnet from child tables is that the WHERE clause is an actual filter,
 * not a foreign key condition.
 */
static void
HapUpdateRootHiddenAttr(char *namespace, char *relname, char *attrname,
						HapHiddenAttrDesc *desc, List *filter_list)
{
	char namespacequoted[MAX_QUOTED_NAME_LEN];
	char relnamequoted[MAX_QUOTED_NAME_LEN];
	char attrnamequoted[MAX_QUOTED_NAME_LEN];
	char target[MAX_QUOTED_NAME_LEN];
	char where[MAX_QUOTED_NAME_LEN];
	StringInfo set;

	/* Quote names */
	HapQuoteName(namespacequoted, namespace);
	HapQuoteName(relnamequoted, relname);
	HapQuoteName(attrnamequoted, attrname);

	/* Make update target clause */
	sprintf(target, "%s.%s", namespacequoted, relnamequoted);

	/* Make SET string */
	filter_list = HapMakeRootAttrFilterList(relname, attrname,
											filter_list);
	set = HapMakeCaseWhenSET(relname, desc, filter_list);

	/* FROM cluase is not required */
	HapUpdateHiddenAttr(desc, target, set->data, NULL, NULL);
}

/*
 * HapTranslateHiddenAttrDescToCmpSQL
 *		Write SQL that compares the hidden attribute with the given value based
 *		on the hidden attribute descriptor.
 *
 * Write and return the hidden attribute comparison SQL to be used in the
 * WHERE clause.
 *
 * -------------
 * Comparing Format
 *   length(<table>._hap_hidden_attribute) > byteid AND
 *   get_byte(<table>._hap_hidden_attribute, byteid) & mask = value AND ...
 * -------------
 *
 * We need length() to avoid error on get_byte()
 * when the given byte id does not exists.
 */
static StringInfo
HapTranslateHiddenAttrDescToCmpSQL(char *relname, HapHiddenAttrDesc *desc,
								   uint16 value)
{
	StringInfo cmpbuf = palloc0(sizeof(StringInfoData));
	char relnamequoted[MAX_QUOTED_NAME_LEN];
	char attrnamequoted[MAX_QUOTED_NAME_LEN];
	ListCell *lc;

	initStringInfo(cmpbuf);
	HapQuoteName(relnamequoted, relname);
	HapQuoteName(attrnamequoted, "_hap_hidden_attribute");

	for (int byteidx = desc->endbyte; byteidx >= desc->startbyte; byteidx--)
	{
		uint16 encoding_value_mask;
		uint8 mask, shift, byteval;

		HapGetHiddenAttrByteInfo(desc, byteidx, &mask, &shift);
		encoding_value_mask = mask >> shift;
		byteval = (value & encoding_value_mask) << shift;

		appendStringInfo(cmpbuf,
						 "length(%s.%s) > %d AND "
						 "get_byte(%s.%s, %d) & %d = %d AND ",
						 relnamequoted, attrnamequoted, byteidx,
						 relnamequoted, attrnamequoted, byteidx,
						 mask, byteval);
		value >>= (8 - shift);
	}
	appendStringInfo(cmpbuf, "TRUE");

	return cmpbuf;
}

/*
 * HapMakeFKeyCmpWHERE
 *		Make WHERE clause that compares foreign key condition and hidden
 *		attribute value.
 *
 * This function returns a string like this:
 *
 *   pkatt1 = fkatt1 [AND ...] pkatt2 = fkatt2 [AND ...]
 */
static StringInfo
HapMakeFKeyCmpWHERE(Relation pkrel, Relation fkrel,
					HapHiddenAttrDesc *parent_desc)
{
	int16 confkeys[INDEX_MAX_KEYS];
	int16 conkeys[INDEX_MAX_KEYS];
	StringInfo where;
	int16 numfkeys;

	/* Initialize */
	where = palloc0(sizeof(StringInfoData));
	initStringInfo(where);
	numfkeys = HapDeconstructFKeyConst(RelationGetRelid(pkrel),
									  RelationGetRelid(fkrel),
									  confkeys, conkeys);

	/* Append foreign key conditions */
	for (int i = 0; i < numfkeys; i++)
	{
		int16 pkattnum = confkeys[i];
		int16 fkattnum = conkeys[i];

		appendStringInfo(where,
						 "\"%s\".\"%s\" = \"%s\".\"%s\" AND ",
						 RelationGetRelationName(pkrel),
						 NameStr(*attnumAttName(pkrel, pkattnum)),
						 RelationGetRelationName(fkrel),
						 NameStr(*attnumAttName(fkrel, fkattnum)));
						 
	}
	appendStringInfo(where, "TRUE");

	return where;
}

/*
 * HapMakeParentHiddenAttrFilterList
 *		Make hidden attribute comparing SQL for the parent table.
 *
 * To propagate encoding values to the child tables, we have to know what
 * the parent hidden attribute is matched to the propagating encoding value.
 *
 * This function makes comparing SQLs for the all possible encoding values and
 * add them to the returing list.
 */
static List *
HapMakeParentHiddenAttrFilterList(char *relname, HapHiddenAttrDesc *desc,
								  int filter_list_len)
{
	List *filter_list = NIL;

	for (int i = 0; i < filter_list_len; i++)
	{
		StringInfo cmp = HapTranslateHiddenAttrDescToCmpSQL(relname, desc, i);

		filter_list = lappend(filter_list, cmp->data);
	}

	return filter_list;
}

/*
 * HapUpdateChildHiddenAttr
 *		Update child table's hidden attribute using foreign key condition.
 *
 * Unlike HapUpdateChildHiddenAttrRecurse(), this function only cares about
 * updating the given child table.
 */
static void
HapUpdateChildHiddenAttr(HapHiddenAttrDesc *parent_desc,
						 HapHiddenAttrDesc *child_desc,
						 int filter_list_len)
{
	char target[MAX_QUOTED_NAME_LEN];
	char from[MAX_QUOTED_NAME_LEN];
	List *parent_filter_list;
	Relation pkrel, fkrel;
	StringInfo set, where;

	/* Open the relations that are used for update qurey */
	pkrel = table_open(parent_desc->relid, AccessShareLock);
	fkrel = table_open(child_desc->relid, AccessShareLock);

	/* Make TARGET string */
	sprintf(target, "\"%s\".\"%s\"",
			get_namespace_name(RelationGetNamespace(fkrel)),
			RelationGetRelationName(fkrel));

	/* Make SET string */
	parent_filter_list = HapMakeParentHiddenAttrFilterList(
												 RelationGetRelationName(pkrel),
											 	 parent_desc,
												 filter_list_len);
	set = HapMakeCaseWhenSET(RelationGetRelationName(fkrel),
							 child_desc, parent_filter_list);

	/* Make FROM string */
	sprintf(from, "\"%s\".\"%s\"",
			get_namespace_name(RelationGetNamespace(pkrel)),
			RelationGetRelationName(pkrel));

	/* Make WHERE string */
	where = HapMakeFKeyCmpWHERE(pkrel, fkrel, parent_desc);

	/* We got enough informations, close the relations */
	table_close(pkrel, AccessShareLock);
	table_close(fkrel, AccessShareLock);

	/* Execute query */
	HapUpdateHiddenAttr(child_desc, target, set->data, from, where->data);
}

/*
 * HapUpdateChildHiddenAttrRecurse
 *		Update the hidden attribute of the child tables.
 *
 * Update the hidden attribute using the foreign key condition. This function is
 * called recursively through foreign key condition.
 */
static void
HapUpdateChildHiddenAttrRecurse(HapHiddenAttrDesc *desc, int filter_list_len)
{
	int16 descid = desc->descid;
	Oid relid = desc->relid;
	List *conrelOids = NIL;
	ListCell *lc = NULL;

	/* Since this function recurses, it could be driven to stack overflow */
	check_stack_depth();

	/* Get the relation oids that are referencing the given relation */
	conrelOids = HapGetReferencingRelids(relid);

	/* Update the hidden attribute of the child tables */
	foreach(lc, conrelOids)
	{
		Oid conrelid = lfirst_oid(lc);
		HapHiddenAttrDesc *child_desc;
		int16 child_descid;

		/* Ignore non-hap table */
		if (!HapCheckAmUsingRelId(conrelid))
			continue;

		child_descid = HapGetChildHiddenAttrDescid(conrelid, relid, descid);
		child_desc = HapMakeHiddenAttrDesc(conrelid, child_descid);
		HapUpdateChildHiddenAttr(desc, child_desc, filter_list_len);

		/* Propagate it recursively */
		HapUpdateChildHiddenAttrRecurse(child_desc, filter_list_len);
	}
}

/*
 * __hap_encode_to_hidden_attribute
 *		Workhorse for hap_encode_to_hidden_attribute().
 *
 * To encode hidden attribute, we use SPI to use plsql.
 */
static void
__hap_encode_to_hidden_attribute(char *name, List *filter_list, int16 descid)
{
	char namespace[MAX_QUOTED_NAME_LEN];
	char relname[MAX_QUOTED_NAME_LEN];
	char attrname[MAX_QUOTED_NAME_LEN];
	Oid relnamespace, relid;

	/* Extract namespace, relname, and attribute name */
	HapExtractNames(name, namespace, relname, attrname);
	
	/* Get relation oid */
	relnamespace = get_namespace_oid(namespace, false);
	relid = get_relname_relid(relname, relnamespace);
	HapHiddenAttrDesc *desc = HapMakeHiddenAttrDesc(relid, descid);

	/* Update root hidden attribute */
	HapUpdateRootHiddenAttr(namespace, relname, attrname, desc, filter_list);

	/* Update hidden attribute of child tables */
	HapUpdateChildHiddenAttrRecurse(desc, list_length(filter_list));
}

/*
 * HapTransformArrayToCStringList
 *		Transform the elements of the given array to the cstring.
 *
 * The given array has elements as a text type which is variable length array.
 * Change it to the cstring and make a List.
 */
static List *
HapTransformArrayToCStringList(AnyArrayType *array, TypeCacheEntry *typentry)
{
	int nitems = ArrayGetNItems(AARR_NDIM(array), AARR_DIMS(array));
	List *filter_list = NIL;
	array_iter iter;

	array_iter_setup(&iter, array);

	/* Loop over source data and add them to the list */
	for (int i = 0; i < nitems; i++)
	{
		Datum elt;
		bool isnull;

		elt = array_iter_next(&iter,
							  &isnull,
							  i,
							  typentry->typlen,
							  typentry->typbyval,
							  typentry->typalign);

		Assert(!isnull);

		/* Change the element to the palloc'd cstring */
		filter_list = lappend(filter_list, text_to_cstring((text *)elt));
	}

	return filter_list;
}

/*
 * hap_encode_to_hidden_attribute
 *		Encode the given value to the hidden attribute.
 *
 * Using the given relation and the given attribute, encode the given value to
 * the hidden attribute. Then propagate it to the child tables recursively.
 */
Datum
hap_encode_to_hidden_attribute(PG_FUNCTION_ARGS)
{
	char *name = text_to_cstring(PG_GETARG_TEXT_P(0));
	AnyArrayType *array = PG_GETARG_ANY_ARRAY_P(1);
	int16 descid = PG_GETARG_INT16(2);
	TypeCacheEntry *typentry;
	Oid element_type;
	List *filter_list;

	/* Each element must text */
	element_type = AARR_ELEMTYPE(array);
	Assert(element_type == TEXTOID);

	/* To get array elements, we have to get type information */
	typentry = (TypeCacheEntry *) fcinfo->flinfo->fn_extra;
	if (typentry == NULL ||
		typentry->type_id != element_type)
	{
		typentry = lookup_type_cache(element_type, 0);
		fcinfo->flinfo->fn_extra = (void *) typentry;
	}

	/* Transform the given array to the cstring list */
	filter_list = HapTransformArrayToCStringList(array, typentry);

	/* Start encoding */
	__hap_encode_to_hidden_attribute(name, filter_list, descid);

	/* Avoid memory leak if supplied array is toasted */
	if (!VARATT_IS_EXPANDED_HEADER(array))
		PG_FREE_IF_COPY(array, 0);

	PG_RETURN_VOID();
}

/*
 * hap_extract_encoding_value
 *		Extract encoding value from hidden attribute.
 *
 * This function is used for filter propagation.
 */
Datum
hap_extract_encoding_value(PG_FUNCTION_ARGS)
{
	bytea *v = PG_GETARG_BYTEA_PP(0);
	HapHiddenAttrDesc desc;

	desc.startbit = PG_GETARG_INT16(1);
	desc.bitsize = PG_GETARG_INT16(2);
	desc.endbit = desc.startbit + desc.bitsize - 1;
	desc.startbyte = BitOffsetToByteOffset(desc.startbit);
	desc.endbyte = BitOffsetToByteOffset(desc.endbit);

	PG_RETURN_INT32(HapExtractEncodingValueFromBytea(&desc, v));
}
