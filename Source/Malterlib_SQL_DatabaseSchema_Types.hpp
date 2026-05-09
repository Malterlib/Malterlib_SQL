// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#pragma once

namespace NMib::NSQL
{
	template <> struct TCSqlTypeTraits<int8> : public TCSqlPrimitiveTypeTraits<int8, ESqlColumnType::mc_Integer8, ESqlValueType::mc_Integer8>
	{
	};

	template <> struct TCSqlTypeTraits<int16> : public TCSqlPrimitiveTypeTraits<int16, ESqlColumnType::mc_Integer16, ESqlValueType::mc_Integer16>
	{
	};

	template <> struct TCSqlTypeTraits<int32> : public TCSqlPrimitiveTypeTraits<int32, ESqlColumnType::mc_Integer32, ESqlValueType::mc_Integer32>
	{
	};

	template <> struct TCSqlTypeTraits<int64> : public TCSqlPrimitiveTypeTraits<int64, ESqlColumnType::mc_Integer64, ESqlValueType::mc_Integer64>
	{
	};

	template <> struct TCSqlTypeTraits<uint8> : public TCSqlPrimitiveTypeTraits<uint8, ESqlColumnType::mc_UnsignedInteger8, ESqlValueType::mc_UnsignedInteger8>
	{
	};

	template <> struct TCSqlTypeTraits<uint16> : public TCSqlPrimitiveTypeTraits<uint16, ESqlColumnType::mc_UnsignedInteger16, ESqlValueType::mc_UnsignedInteger16>
	{
	};

	template <> struct TCSqlTypeTraits<uint32> : public TCSqlPrimitiveTypeTraits<uint32, ESqlColumnType::mc_UnsignedInteger32, ESqlValueType::mc_UnsignedInteger32>
	{
	};

	template <> struct TCSqlTypeTraits<uint64> : public TCSqlPrimitiveTypeTraits<uint64, ESqlColumnType::mc_UnsignedInteger64, ESqlValueType::mc_UnsignedInteger64>
	{
	};

	template <> struct TCSqlTypeTraits<bool> : public TCSqlPrimitiveTypeTraits<bool, ESqlColumnType::mc_Boolean, ESqlValueType::mc_Boolean>
	{
	};

	template <> struct TCSqlTypeTraits<fp32> : public TCSqlPrimitiveTypeTraits<fp32, ESqlColumnType::mc_Float32, ESqlValueType::mc_Float32>
	{
	};

	template <> struct TCSqlTypeTraits<fp64> : public TCSqlPrimitiveTypeTraits<fp64, ESqlColumnType::mc_Float64, ESqlValueType::mc_Float64>
	{
	};

	template <> struct TCSqlTypeTraits<NStr::CStr> : public TCSqlPrimitiveTypeTraits<NStr::CStr, ESqlColumnType::mc_Text, ESqlValueType::mc_Text>
	{
	};

	template <> struct TCSqlTypeTraits<NContainer::CIOByteVector> : public TCSqlPrimitiveTypeTraits<NContainer::CIOByteVector, ESqlColumnType::mc_Blob, ESqlValueType::mc_Blob>
	{
	};

	template <> struct TCSqlTypeTraits<NTime::CTime> : public TCSqlPrimitiveTypeTraits<NTime::CTime, ESqlColumnType::mc_Time, ESqlValueType::mc_Time>
	{
	};

	template <>
	struct TCSqlTypeTraits<NCryptography::CUniversallyUniqueIdentifier>
		: public TCSqlPrimitiveTypeTraits<NCryptography::CUniversallyUniqueIdentifier, ESqlColumnType::mc_UUID, ESqlValueType::mc_UUID>
	{
	};

	template <> struct TCSqlTypeTraits<CSqlDate> : public TCSqlPrimitiveTypeTraits<CSqlDate, ESqlColumnType::mc_Date, ESqlValueType::mc_Date>
	{
	};

	template <> struct TCSqlTypeTraits<CSqlTimeOfDay> : public TCSqlPrimitiveTypeTraits<CSqlTimeOfDay, ESqlColumnType::mc_TimeOfDay, ESqlValueType::mc_TimeOfDay>
	{
	};

	template <> struct TCSqlTypeTraits<CSqlTimestamp> : public TCSqlPrimitiveTypeTraits<CSqlTimestamp, ESqlColumnType::mc_Timestamp, ESqlValueType::mc_Timestamp>
	{
	};

	template <> struct TCSqlTypeTraits<CSqlTimestampTz> : public TCSqlPrimitiveTypeTraits<CSqlTimestampTz, ESqlColumnType::mc_TimestampTz, ESqlValueType::mc_TimestampTz>
	{
	};

	template <> struct TCSqlTypeTraits<CSqlInterval> : public TCSqlPrimitiveTypeTraits<CSqlInterval, ESqlColumnType::mc_Interval, ESqlValueType::mc_Interval>
	{
	};

	template <> struct TCSqlTypeTraits<NEncoding::CJsonOrdered> : public TCSqlPrimitiveTypeTraits<NEncoding::CJsonOrdered, ESqlColumnType::mc_Json, ESqlValueType::mc_Json>
	{
	};

	template <> struct TCSqlTypeTraits<NEncoding::CJsonSorted> : public TCSqlPrimitiveTypeTraits<NEncoding::CJsonSorted, ESqlColumnType::mc_Jsonb, ESqlValueType::mc_Jsonb>
	{
	};

	template <> struct TCSqlTypeTraits<TCSqlArray<int16>> : public TCSqlPrimitiveTypeTraits<TCSqlArray<int16>, ESqlColumnType::mc_Array_Integer16, ESqlValueType::mc_Array_Integer16>
	{
	};

	template <> struct TCSqlTypeTraits<TCSqlArray<int32>> : public TCSqlPrimitiveTypeTraits<TCSqlArray<int32>, ESqlColumnType::mc_Array_Integer32, ESqlValueType::mc_Array_Integer32>
	{
	};

	template <> struct TCSqlTypeTraits<TCSqlArray<int64>> : public TCSqlPrimitiveTypeTraits<TCSqlArray<int64>, ESqlColumnType::mc_Array_Integer64, ESqlValueType::mc_Array_Integer64>
	{
	};

	template <> struct TCSqlTypeTraits<TCSqlArray<fp32>> : public TCSqlPrimitiveTypeTraits<TCSqlArray<fp32>, ESqlColumnType::mc_Array_Float32, ESqlValueType::mc_Array_Float32>
	{
	};

	template <> struct TCSqlTypeTraits<TCSqlArray<fp64>> : public TCSqlPrimitiveTypeTraits<TCSqlArray<fp64>, ESqlColumnType::mc_Array_Float64, ESqlValueType::mc_Array_Float64>
	{
	};

	template <> struct TCSqlTypeTraits<TCSqlArray<NStr::CStr>> : public TCSqlPrimitiveTypeTraits<TCSqlArray<NStr::CStr>, ESqlColumnType::mc_Array_Text, ESqlValueType::mc_Array_Text>
	{
	};

	template <> struct TCSqlTypeTraits<TCSqlArray<bool>> : public TCSqlPrimitiveTypeTraits<TCSqlArray<bool>, ESqlColumnType::mc_Array_Boolean, ESqlValueType::mc_Array_Boolean>
	{
	};

	template <>
	struct TCSqlTypeTraits<TCSqlArray<NContainer::CIOByteVector>>
		: public TCSqlPrimitiveTypeTraits<TCSqlArray<NContainer::CIOByteVector>, ESqlColumnType::mc_Array_Bytes, ESqlValueType::mc_Array_Bytes>
	{
	};

	template <> struct TCSqlTypeTraits<TCSqlArray<CSqlDate>> : public TCSqlPrimitiveTypeTraits<TCSqlArray<CSqlDate>, ESqlColumnType::mc_Array_Date, ESqlValueType::mc_Array_Date>
	{
	};

	template <>
	struct TCSqlTypeTraits<TCSqlArray<CSqlTimeOfDay>>
		: public TCSqlPrimitiveTypeTraits<TCSqlArray<CSqlTimeOfDay>, ESqlColumnType::mc_Array_TimeOfDay, ESqlValueType::mc_Array_TimeOfDay>
	{
	};

	template <>
	struct TCSqlTypeTraits<TCSqlArray<CSqlTimestamp>>
		: public TCSqlPrimitiveTypeTraits<TCSqlArray<CSqlTimestamp>, ESqlColumnType::mc_Array_Timestamp, ESqlValueType::mc_Array_Timestamp>
	{
	};

	template <>
	struct TCSqlTypeTraits<TCSqlArray<CSqlTimestampTz>>
		: public TCSqlPrimitiveTypeTraits<TCSqlArray<CSqlTimestampTz>, ESqlColumnType::mc_Array_TimestampTz, ESqlValueType::mc_Array_TimestampTz>
	{
	};

	template <>
	struct TCSqlTypeTraits<TCSqlArray<NCryptography::CUniversallyUniqueIdentifier>>
		: public TCSqlPrimitiveTypeTraits<TCSqlArray<NCryptography::CUniversallyUniqueIdentifier>, ESqlColumnType::mc_Array_UUID, ESqlValueType::mc_Array_UUID>
	{
	};

	template <>
	struct TCSqlTypeTraits<TCSqlArray<NEncoding::CJsonOrdered>>
		: public TCSqlPrimitiveTypeTraits<TCSqlArray<NEncoding::CJsonOrdered>, ESqlColumnType::mc_Array_Json, ESqlValueType::mc_Array_Json>
	{
	};

	template <>
	struct TCSqlTypeTraits<TCSqlArray<NEncoding::CJsonSorted>>
		: public TCSqlPrimitiveTypeTraits<TCSqlArray<NEncoding::CJsonSorted>, ESqlColumnType::mc_Array_Jsonb, ESqlValueType::mc_Array_Jsonb>
	{
	};

	template <>
	struct TCSqlTypeTraits<TCSqlArray<CSqlInterval>>
		: public TCSqlPrimitiveTypeTraits<TCSqlArray<CSqlInterval>, ESqlColumnType::mc_Array_Interval, ESqlValueType::mc_Array_Interval>
	{
	};
}
