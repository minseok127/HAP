/*-------------------------------------------------------------------------
 *
 * hap_encoding.h
 *	  POSTGRES hidden attribute partitioning (HAP) encoding.
 *
 *
 * src/include/hap/hap_encoding.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef HAP_ENCODING_H
#define HAP_ENCODING_H

#include "access/attnum.h"
#include "nodes/pg_list.h"
#include "utils/memutils.h"
#include "utils/palloc.h"

/*
 * HapHiddenAttrDesc - Descriptor for specific encoded attribute.
 *
 * - relid: Relation oid.
 * - descid: Descriptor id for the encoded attribute.
 * - startbit: Start bit's index in hidden attribute.
 * - bitsize: Size of bits for the encoded attribute.
 * - startbyte: Start byte's index in hidden attribute.
 * - endbyte: End byte's index in hidden attribute.
 * - partkeyidx: Index in partition key.
 */
typedef struct HapHiddenAttrDesc
{
	Oid			relid;
	int16		descid;
	int16		bitsize;
	int16		startbit;
	int16		endbit;
	int16		startbyte;
	int16		endbyte;
	int16		partkeyidx;
} HapHiddenAttrDesc;

extern HapHiddenAttrDesc *HapMakeHiddenAttrDesc(Oid relid, int16 descid);

extern uint16 HapExtractEncodingValueFromBytea(HapHiddenAttrDesc *desc,
											   bytea *hiddenattr);

extern void HapApplyEncodingValueToBytea(HapHiddenAttrDesc *desc,
										 uint16 value, bytea *hiddenattr);

#endif	/* HAP_ENCODING_H */
