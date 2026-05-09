// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#include "Malterlib_SQL_PostgresClient_Internal.h"

#include <Mib/Cryptography/RandomID>
#include <Mib/Cryptography/Scram>
#include <Mib/Encoding/Base64>
#include <Mib/Stream/ByteVector>

namespace NMib::NSQL
{
	CSqlErrorData fg_PostgresSqlErrorData(CPostgresErrorResponse const &_Error)
	{
		ESqlErrorCategory Category = ESqlErrorCategory::mc_Generic;
		ESqlErrorRetryClass RetryClass = ESqlErrorRetryClass::mc_Permanent;

		if (_Error.m_Code == "23505")
			Category = ESqlErrorCategory::mc_DuplicateKey;
		else if (_Error.m_Code == "23503")
			Category = ESqlErrorCategory::mc_ForeignKeyViolation;
		else if (_Error.m_Code.f_StartsWith("23"))
			Category = ESqlErrorCategory::mc_ConstraintViolation;
		else if (_Error.m_Code == "40P01")
		{
			Category = ESqlErrorCategory::mc_Deadlock;
			RetryClass = ESqlErrorRetryClass::mc_RetryTransaction;
		}
		else if (_Error.m_Code == "40001")
		{
			Category = ESqlErrorCategory::mc_SerializationFailure;
			RetryClass = ESqlErrorRetryClass::mc_RetryTransaction;
		}
		else if (_Error.m_Code.f_StartsWith("08"))
		{
			Category = ESqlErrorCategory::mc_ConnectionLoss;
			RetryClass = ESqlErrorRetryClass::mc_RetryConnection;
		}

		return fg_SqlErrorData
			(
				Category
				, RetryClass
				, "postgres"
				, _Error.m_Code
				, _Error.m_Message
				, _Error.m_Detail
				, _Error.m_Hint
			)
		;
	}

	CExceptionSql fg_PostgresSqlError(NStr::CStr const &_Message, CPostgresErrorResponse const &_Error)
	{
		return DMibErrorSqlInstance(_Message, fg_PostgresSqlErrorData(_Error));
	}

	namespace
	{
		umint fg_PostgresRemaining(CPostgresReadStream const &_Stream)
		{
			return umint(_Stream.f_GetLength() - _Stream.f_GetPosition());
		}

		void fg_PostgresWriteCString(CPostgresWriteStream &_Stream, NStr::CStr const &_Value)
		{
			_Stream.f_FeedBytes(_Value.f_GetStr(), _Value.f_GetLen());
			_Stream << uint8(0);
		}

		void fg_PostgresWriteStringBytes(CPostgresWriteStream &_Stream, NStr::CStr const &_Value)
		{
			_Stream.f_FeedBytes(_Value.f_GetStr(), _Value.f_GetLen());
		}

		void fg_PostgresWriteBytes(CPostgresWriteStream &_Stream, NContainer::CIOByteVector const &_Bytes)
		{
			_Stream.f_FeedBytes(_Bytes.f_GetArray(), _Bytes.f_GetLen());
		}

		void fg_PostgresBackfillInt32(CPostgresWriteStream &_Stream, NStream::CFilePos _Position, uint32 _Value)
		{
			NStream::CFilePos CurrentPosition = _Stream.f_GetPosition();
			_Stream.f_SetPosition(_Position);
			_Stream << _Value;
			_Stream.f_SetPosition(CurrentPosition);
		}

		void fg_PostgresBackfillMessageLength(CPostgresWriteStream &_Stream, NStream::CFilePos _LengthPosition)
		{
			fg_PostgresBackfillInt32(_Stream, _LengthPosition, uint32(_Stream.f_GetLength() - _LengthPosition));
		}

		NStream::CFilePos fg_PostgresBeginFrontendMessage(CPostgresWriteStream &_Stream, uint8 _Type)
		{
			_Stream << _Type;
			NStream::CFilePos LengthPosition = _Stream.f_GetPosition();
			_Stream << uint32(0);

			return LengthPosition;
		}

		void fg_PostgresFinishFrontendMessage(CPostgresWriteStream &_Stream, NStream::CFilePos _LengthPosition)
		{
			fg_PostgresBackfillMessageLength(_Stream, _LengthPosition);
		}

		NContainer::CIOByteVector fg_PostgresReadBytes(CPostgresReadStream &_Stream, umint _nBytes)
		{
			NContainer::CIOByteVector Data;
			{
				NStream::CBinaryStreamMemoryRef<NStream::CBinaryStreamBigEndian, NContainer::CIOByteVector> OutStream(Data);
				OutStream.f_FeedFromStream(_Stream, _nBytes);
			}

			return Data;
		}

		NConcurrency::TCWrapped<NStr::CStr> fg_PostgresReadCString(CPostgresReadStream &_Stream)
		{
			NStr::CStr Value;
			NStr::CStr::CAppender Appender(Value);

			while (!_Stream.f_IsAtEndOfStream())
			{
				uint8 Character;
				_Stream >> Character;

				if (Character == 0)
					return Appender.f_Commit().m_String;

				Appender += Character;
			}

			return DMibErrorDatabaseInstance("PostgreSQL message contains unterminated string");
		}

		NTime::CTime fg_PostgresEpoch()
		{
			return NTime::CTimeConvert::fs_CreateTime(2000, 1, 1);
		}

		int64 fg_PostgresFractionIntToMicroseconds(uint64 _Fraction)
		{
			constexpr uint64 c_MicrosecondsPerSecond = 1000000;
			constexpr uint64 c_Divisor = NTime::NPrivate::CConst::mc_FractionDividend / c_MicrosecondsPerSecond;

			return int64((_Fraction + c_Divisor / 2) / c_Divisor);
		}

		uint64 fg_PostgresMicrosecondsToFractionInt(int64 _Microseconds)
		{
			constexpr uint64 c_MicrosecondsPerSecond = 1000000;
			uint64 Microseconds = uint64(_Microseconds);

			return
				(NTime::NPrivate::CConst::mc_FractionDividend / c_MicrosecondsPerSecond) * Microseconds
				+ (NTime::NPrivate::CConst::mc_FractionDividend % c_MicrosecondsPerSecond) * Microseconds / c_MicrosecondsPerSecond
			;
		}

		NTime::CTimeSpan fg_PostgresMicrosecondsToTimeSpan(int64 _Microseconds)
		{
			constexpr int64 c_MicrosecondsPerSecond = 1000000;
			int64 Seconds = _Microseconds / c_MicrosecondsPerSecond;
			int64 Microseconds = _Microseconds - Seconds * c_MicrosecondsPerSecond;

			if (Microseconds < 0)
			{
				--Seconds;
				Microseconds += c_MicrosecondsPerSecond;
			}

			return NTime::CTimeSpan(Seconds, fg_PostgresMicrosecondsToFractionInt(Microseconds));
		}

		int64 fg_PostgresTimeToMicroseconds(NTime::CTime const &_Time)
		{
			NTime::CTimeSpan TimeSpan = _Time - fg_PostgresEpoch();

			return TimeSpan.f_GetSeconds() * 1000000 + fg_PostgresFractionIntToMicroseconds(TimeSpan.f_GetFractionInt());
		}

		int64 fg_PostgresTimeSpanToMicroseconds(NTime::CTimeSpan const &_TimeSpan)
		{
			return _TimeSpan.f_GetSeconds() * 1000000 + fg_PostgresFractionIntToMicroseconds(_TimeSpan.f_GetFractionInt());
		}

		NTime::CTime fg_PostgresMicrosecondsToTime(int64 _Microseconds)
		{
			return fg_PostgresEpoch() + fg_PostgresMicrosecondsToTimeSpan(_Microseconds);
		}

		int32 fg_PostgresTimeToDays(NTime::CTime const &_Time)
		{
			int64 Microseconds = fg_PostgresTimeToMicroseconds(_Time);
			int64 MicrosecondsPerDay = int64(24) * 60 * 60 * 1000000;
			// Use floor division, not C++ truncation toward zero: a date before the 2000-01-01 epoch with a non-midnight
			// time (for example 1999-12-31 12:00) has a negative offset whose truncated quotient is 0, which would store
			// the wrong calendar day. Flooring maps any time on that day to the correct (negative) day number.
			int64 Days = Microseconds / MicrosecondsPerDay;
			if (Microseconds < 0 && Microseconds % MicrosecondsPerDay != 0)
				--Days;

			return int32(Days);
		}

		NTime::CTime fg_PostgresDaysToTime(int32 _Days)
		{
			return fg_PostgresEpoch() + NTime::CTimeSpanConvert::fs_CreateDaySpan(_Days);
		}

		int64 fg_PostgresTimeOfDayToMicroseconds(NTime::CTime const &_Time)
		{
			NTime::CTimeConvert::CDateTime DateTime;
			NTime::CTimeConvert(_Time).f_ExtractDateTime(DateTime);
			int64 Seconds = (int64(DateTime.m_Hour) * 60 + DateTime.m_Minute) * 60 + DateTime.m_Second;

			return Seconds * 1000000 + fg_PostgresFractionIntToMicroseconds(DateTime.m_FractionInt);
		}

		NTime::CTime fg_PostgresMicrosecondsToTimeOfDay(int64 _Microseconds)
		{
			return NTime::CTimeConvert::fs_CreateTime(1970, 1, 1) + fg_PostgresMicrosecondsToTimeSpan(_Microseconds);
		}

		void fg_PostgresWriteSizedString(CPostgresWriteStream &_Stream, NStr::CStr const &_Value)
		{
			_Stream << uint32(_Value.f_GetLen());
			_Stream.f_FeedBytes(_Value.f_GetStr(), _Value.f_GetLen());
		}

		void fg_PostgresWriteSizedBytes(CPostgresWriteStream &_Stream, NContainer::CIOByteVector const &_Bytes)
		{
			_Stream << uint32(_Bytes.f_GetLen());
			_Stream.f_FeedBytes(_Bytes.f_GetArray(), _Bytes.f_GetLen());
		}

		void fg_PostgresWriteSizedUUID(CPostgresWriteStream &_Stream, NCryptography::CUniversallyUniqueIdentifier const &_UUID)
		{
			_Stream << uint32(16);
			_Stream << _UUID.m_TimeLow;
			_Stream << _UUID.m_TimeMid;
			_Stream << _UUID.m_TimeHiAndVersion;
			_Stream << _UUID.m_ClockSequenceHiAndReserved;
			_Stream << _UUID.m_ClockSquenceLow;

			for (uint8 Byte : _UUID.m_Node)
				_Stream << Byte;
		}

		void fg_PostgresWriteSizedJsonb(CPostgresWriteStream &_Stream, NEncoding::CJsonSorted const &_Json)
		{
			NStr::CStr Text = _Json.f_ToString(nullptr);
			_Stream << uint32(Text.f_GetLen() + 1);
			_Stream << uint8(1);
			_Stream.f_FeedBytes(Text.f_GetStr(), Text.f_GetLen());
		}

		void fg_PostgresWriteSizedInterval(CPostgresWriteStream &_Stream, CPostgresInterval const &_Interval)
		{
			_Stream << uint32(16);
			_Stream << fg_PostgresTimeSpanToMicroseconds(_Interval.m_Time);
			_Stream << _Interval.m_Days;
			_Stream << _Interval.m_Months;
		}

		void fg_PostgresWriteBinaryValue(CPostgresWriteStream &_Stream, int16 _Value)
		{
			_Stream << uint32(2);
			_Stream << _Value;
		}

		void fg_PostgresWriteBinaryValue(CPostgresWriteStream &_Stream, int32 _Value)
		{
			_Stream << uint32(4);
			_Stream << _Value;
		}

		void fg_PostgresWriteBinaryValue(CPostgresWriteStream &_Stream, int64 _Value)
		{
			_Stream << uint32(8);
			_Stream << _Value;
		}

		void fg_PostgresWriteBinaryValue(CPostgresWriteStream &_Stream, fp32 _Value)
		{
			_Stream << uint32(4);
			_Stream << fg_BitCast<uint32>(_Value);
		}

		void fg_PostgresWriteBinaryValue(CPostgresWriteStream &_Stream, fp64 _Value)
		{
			_Stream << uint32(8);
			_Stream << fg_BitCast<uint64>(_Value);
		}

		void fg_PostgresWriteBinaryValue(CPostgresWriteStream &_Stream, NStr::CStr const &_Value)
		{
			fg_PostgresWriteSizedString(_Stream, _Value);
		}

		void fg_PostgresWriteBinaryValue(CPostgresWriteStream &_Stream, bool _Value)
		{
			_Stream << uint32(1);
			_Stream << uint8(_Value ? 1 : 0);
		}

		void fg_PostgresWriteBinaryValue(CPostgresWriteStream &_Stream, NContainer::CIOByteVector const &_Value)
		{
			fg_PostgresWriteSizedBytes(_Stream, _Value);
		}

		void fg_PostgresWriteBinaryValue(CPostgresWriteStream &_Stream, CPostgresUnrecognizedValue const &_Value)
		{
			fg_PostgresWriteSizedBytes(_Stream, _Value.m_Bytes);
		}

		void fg_PostgresWriteBinaryValue(CPostgresWriteStream &_Stream, CPostgresDate const &_Value)
		{
			_Stream << uint32(4);
			_Stream << fg_PostgresTimeToDays(_Value.m_Time);
		}

		void fg_PostgresWriteBinaryValue(CPostgresWriteStream &_Stream, CPostgresTime const &_Value)
		{
			_Stream << uint32(8);
			_Stream << fg_PostgresTimeOfDayToMicroseconds(_Value.m_Time);
		}

		void fg_PostgresWriteBinaryValue(CPostgresWriteStream &_Stream, CPostgresTimestamp const &_Value)
		{
			_Stream << uint32(8);
			_Stream << fg_PostgresTimeToMicroseconds(_Value.m_Time);
		}

		void fg_PostgresWriteBinaryValue(CPostgresWriteStream &_Stream, CPostgresTimestampTz const &_Value)
		{
			_Stream << uint32(8);
			_Stream << fg_PostgresTimeToMicroseconds(_Value.m_Time);
		}

		void fg_PostgresWriteBinaryValue(CPostgresWriteStream &_Stream, NCryptography::CUniversallyUniqueIdentifier const &_Value)
		{
			fg_PostgresWriteSizedUUID(_Stream, _Value);
		}

		void fg_PostgresWriteBinaryValue(CPostgresWriteStream &_Stream, NEncoding::CJsonOrdered const &_Value)
		{
			fg_PostgresWriteSizedString(_Stream, _Value.f_ToString(nullptr));
		}

		void fg_PostgresWriteBinaryValue(CPostgresWriteStream &_Stream, NEncoding::CJsonSorted const &_Value)
		{
			fg_PostgresWriteSizedJsonb(_Stream, _Value);
		}

		void fg_PostgresWriteBinaryValue(CPostgresWriteStream &_Stream, CPostgresInterval const &_Value)
		{
			fg_PostgresWriteSizedInterval(_Stream, _Value);
		}

		template <typename t_CValue>
		umint fg_PostgresGetArrayValueCount(TCPostgresArray<t_CValue> const &_Array)
		{
			if (!_Array.m_Dimensions.f_IsEmpty())
			{
				umint nValues = 1;
				for (auto const &Dimension : _Array.m_Dimensions)
					nValues *= umint(Dimension.m_Length);

				return nValues;
			}

			return _Array.m_Values.f_GetLen();
		}

		template <typename t_CValue>
		NConcurrency::TCWrapped<void> fg_PostgresWriteSizedArray(CPostgresWriteStream &_Stream, TCPostgresArray<t_CValue> const &_Array, uint32 _ElementTypeOID)
		{
			CPostgresWriteStream ArrayStream;
			umint nValues = fg_PostgresGetArrayValueCount(_Array);
			if (nValues != _Array.m_Values.f_GetLen())
				return DMibErrorDatabaseInstance("PostgreSQL array dimensions do not match value count");

			NContainer::TCVector<CPostgresArrayDimension> Dimensions = _Array.m_Dimensions;
			if (Dimensions.f_IsEmpty() && !_Array.m_Values.f_IsEmpty())
				Dimensions.f_InsertLast(CPostgresArrayDimension{.m_Length = int32(_Array.m_Values.f_GetLen()), .m_LowerBound = 1});

			bool bHasNull = false;
			for (auto const &Value : _Array.m_Values)
				bHasNull |= !Value;

			ArrayStream << int32(Dimensions.f_GetLen());
			ArrayStream << int32(bHasNull ? 1 : 0);
			ArrayStream << _ElementTypeOID;
			for (auto const &Dimension : Dimensions)
			{
				ArrayStream << Dimension.m_Length;
				ArrayStream << Dimension.m_LowerBound;
			}

			for (auto const &Value : _Array.m_Values)
			{
				if (!Value)
				{
					ArrayStream << uint32(0xffffffff);
					continue;
				}

				fg_PostgresWriteBinaryValue(ArrayStream, *Value);
			}

			NContainer::CIOByteVector ArrayBytes = ArrayStream.f_MoveVector();
			_Stream << uint32(ArrayBytes.f_GetLen());
			_Stream.f_FeedBytes(ArrayBytes.f_GetArray(), ArrayBytes.f_GetLen());

			return {};
		}

		uint32 fg_PostgresGetParameterTypeOID(CPostgresValue const &_Value)
		{
			if (_Value.f_GetTypeID() == EPostgresValueType::mc_Unrecognized)
				return _Value.f_GetAsType<CPostgresUnrecognizedValue>().m_TypeOID;

			return fg_PostgresGetValueTypeOID(_Value.f_GetTypeID());
		}

		NConcurrency::TCWrapped<void> fg_PostgresWriteParameter(CPostgresWriteStream &_Stream, CPostgresValue const &_Value)
		{
			switch (_Value.f_GetTypeID())
			{
			case EPostgresValueType::mc_Null:
				_Stream << uint32(0xffffffff);
				break;
			case EPostgresValueType::mc_Integer16:
				fg_PostgresWriteBinaryValue(_Stream, _Value.f_GetAsType<int16>());
				break;
			case EPostgresValueType::mc_Integer32:
				fg_PostgresWriteBinaryValue(_Stream, _Value.f_GetAsType<int32>());
				break;
			case EPostgresValueType::mc_Integer64:
				fg_PostgresWriteBinaryValue(_Stream, _Value.f_GetAsType<int64>());
				break;
			case EPostgresValueType::mc_Float32:
				fg_PostgresWriteBinaryValue(_Stream, _Value.f_GetAsType<fp32>());
				break;
			case EPostgresValueType::mc_Float64:
				fg_PostgresWriteBinaryValue(_Stream, _Value.f_GetAsType<fp64>());
				break;
			case EPostgresValueType::mc_Text:
				fg_PostgresWriteBinaryValue(_Stream, _Value.f_GetAsType<NStr::CStr>());
				break;
			case EPostgresValueType::mc_Varchar:
				DMibNeverGetHere;
				break;
			case EPostgresValueType::mc_Boolean:
				fg_PostgresWriteBinaryValue(_Stream, _Value.f_GetAsType<bool>());
				break;
			case EPostgresValueType::mc_Bytes:
				fg_PostgresWriteBinaryValue(_Stream, _Value.f_GetAsType<NContainer::CIOByteVector>());
				break;
			case EPostgresValueType::mc_Unrecognized:
				fg_PostgresWriteBinaryValue(_Stream, _Value.f_GetAsType<CPostgresUnrecognizedValue>());
				break;
			case EPostgresValueType::mc_Date:
				fg_PostgresWriteBinaryValue(_Stream, _Value.f_GetAsType<CPostgresDate>());
				break;
			case EPostgresValueType::mc_Time:
				fg_PostgresWriteBinaryValue(_Stream, _Value.f_GetAsType<CPostgresTime>());
				break;
			case EPostgresValueType::mc_Timestamp:
				fg_PostgresWriteBinaryValue(_Stream, _Value.f_GetAsType<CPostgresTimestamp>());
				break;
			case EPostgresValueType::mc_TimestampTz:
				fg_PostgresWriteBinaryValue(_Stream, _Value.f_GetAsType<CPostgresTimestampTz>());
				break;
			case EPostgresValueType::mc_UUID:
				fg_PostgresWriteBinaryValue(_Stream, _Value.f_GetAsType<NCryptography::CUniversallyUniqueIdentifier>());
				break;
			case EPostgresValueType::mc_Json:
				fg_PostgresWriteBinaryValue(_Stream, _Value.f_GetAsType<NEncoding::CJsonOrdered>());
				break;
			case EPostgresValueType::mc_Jsonb:
				fg_PostgresWriteBinaryValue(_Stream, _Value.f_GetAsType<NEncoding::CJsonSorted>());
				break;
			case EPostgresValueType::mc_Interval:
				fg_PostgresWriteBinaryValue(_Stream, _Value.f_GetAsType<CPostgresInterval>());
				break;
			case EPostgresValueType::mc_Array_Integer16:
				return fg_PostgresWriteSizedArray(_Stream, _Value.f_GetAsType<TCPostgresArray<int16>>(), uint32(EPostgresValueType::mc_Integer16));
				break;
			case EPostgresValueType::mc_Array_Integer32:
				return fg_PostgresWriteSizedArray(_Stream, _Value.f_GetAsType<TCPostgresArray<int32>>(), uint32(EPostgresValueType::mc_Integer32));
				break;
			case EPostgresValueType::mc_Array_Integer64:
				return fg_PostgresWriteSizedArray(_Stream, _Value.f_GetAsType<TCPostgresArray<int64>>(), uint32(EPostgresValueType::mc_Integer64));
				break;
			case EPostgresValueType::mc_Array_Float32:
				return fg_PostgresWriteSizedArray(_Stream, _Value.f_GetAsType<TCPostgresArray<fp32>>(), uint32(EPostgresValueType::mc_Float32));
				break;
			case EPostgresValueType::mc_Array_Float64:
				return fg_PostgresWriteSizedArray(_Stream, _Value.f_GetAsType<TCPostgresArray<fp64>>(), uint32(EPostgresValueType::mc_Float64));
				break;
			case EPostgresValueType::mc_Array_Text:
				return fg_PostgresWriteSizedArray(_Stream, _Value.f_GetAsType<TCPostgresArray<NStr::CStr>>(), uint32(EPostgresValueType::mc_Text));
				break;
			case EPostgresValueType::mc_Array_Varchar:
				DMibNeverGetHere;
				break;
			case EPostgresValueType::mc_Array_Boolean:
				return fg_PostgresWriteSizedArray(_Stream, _Value.f_GetAsType<TCPostgresArray<bool>>(), uint32(EPostgresValueType::mc_Boolean));
				break;
			case EPostgresValueType::mc_Array_Bytes:
				return fg_PostgresWriteSizedArray(_Stream, _Value.f_GetAsType<TCPostgresArray<NContainer::CIOByteVector>>(), uint32(EPostgresValueType::mc_Bytes));
				break;
			case EPostgresValueType::mc_Array_Date:
				return fg_PostgresWriteSizedArray(_Stream, _Value.f_GetAsType<TCPostgresArray<CPostgresDate>>(), uint32(EPostgresValueType::mc_Date));
				break;
			case EPostgresValueType::mc_Array_Time:
				return fg_PostgresWriteSizedArray(_Stream, _Value.f_GetAsType<TCPostgresArray<CPostgresTime>>(), uint32(EPostgresValueType::mc_Time));
				break;
			case EPostgresValueType::mc_Array_Timestamp:
				return fg_PostgresWriteSizedArray(_Stream, _Value.f_GetAsType<TCPostgresArray<CPostgresTimestamp>>(), uint32(EPostgresValueType::mc_Timestamp));
				break;
			case EPostgresValueType::mc_Array_TimestampTz:
				return fg_PostgresWriteSizedArray(_Stream, _Value.f_GetAsType<TCPostgresArray<CPostgresTimestampTz>>(), uint32(EPostgresValueType::mc_TimestampTz));
				break;
			case EPostgresValueType::mc_Array_UUID:
				return fg_PostgresWriteSizedArray(_Stream, _Value.f_GetAsType<TCPostgresArray<NCryptography::CUniversallyUniqueIdentifier>>(), uint32(EPostgresValueType::mc_UUID));
				break;
			case EPostgresValueType::mc_Array_Json:
				return fg_PostgresWriteSizedArray(_Stream, _Value.f_GetAsType<TCPostgresArray<NEncoding::CJsonOrdered>>(), uint32(EPostgresValueType::mc_Json));
				break;
			case EPostgresValueType::mc_Array_Jsonb:
				return fg_PostgresWriteSizedArray(_Stream, _Value.f_GetAsType<TCPostgresArray<NEncoding::CJsonSorted>>(), uint32(EPostgresValueType::mc_Jsonb));
				break;
			case EPostgresValueType::mc_Array_Interval:
				return fg_PostgresWriteSizedArray(_Stream, _Value.f_GetAsType<TCPostgresArray<CPostgresInterval>>(), uint32(EPostgresValueType::mc_Interval));
				break;
			}

			return {};
		}

		NConcurrency::TCWrapped<NStr::CStr> fg_PostgresReadStringBytes(CPostgresReadStream &_Stream, uint32 _Length)
		{
			NStr::CStr Value;
			NStr::CStr::CAppender Appender(Value);
			for (uint32 i = 0; i < _Length; ++i)
			{
				uint8 Character;
				_Stream >> Character;
				Appender.f_AddChar(Character);
			}

			return Appender.f_Commit().m_String;
		}

		template <typename t_CValue>
		NConcurrency::TCWrapped<t_CValue> fg_PostgresReadBinaryValue(CPostgresReadStream &_Stream, uint32 _Length, uint32 _TypeOID)
		{
			return DMibErrorDatabaseInstance("PostgreSQL binary value type is not supported");
		}

		template <>
		NConcurrency::TCWrapped<bool> fg_PostgresReadBinaryValue<bool>(CPostgresReadStream &_Stream, uint32 _Length, uint32 _TypeOID)
		{
			if (_TypeOID != 16)
				return DMibErrorDatabaseInstance("PostgreSQL boolean value has unexpected type");

			if (_Length != 1)
				return DMibErrorDatabaseInstance("PostgreSQL boolean value has invalid binary length");

			uint8 BoolValue;
			_Stream >> BoolValue;

			return BoolValue != 0;
		}

		template <>
		NConcurrency::TCWrapped<int16> fg_PostgresReadBinaryValue<int16>(CPostgresReadStream &_Stream, uint32 _Length, uint32 _TypeOID)
		{
			if (_TypeOID != 21)
				return DMibErrorDatabaseInstance("PostgreSQL int2 value has unexpected type");

			if (_Length != 2)
				return DMibErrorDatabaseInstance("PostgreSQL int2 value has invalid binary length");

			int16 Value;
			_Stream >> Value;

			return Value;
		}

		template <>
		NConcurrency::TCWrapped<int32> fg_PostgresReadBinaryValue<int32>(CPostgresReadStream &_Stream, uint32 _Length, uint32 _TypeOID)
		{
			if (_TypeOID != 23)
				return DMibErrorDatabaseInstance("PostgreSQL int4 value has unexpected type");

			if (_Length != 4)
				return DMibErrorDatabaseInstance("PostgreSQL int4 value has invalid binary length");

			int32 Value;
			_Stream >> Value;

			return Value;
		}

		template <>
		NConcurrency::TCWrapped<int64> fg_PostgresReadBinaryValue<int64>(CPostgresReadStream &_Stream, uint32 _Length, uint32 _TypeOID)
		{
			if (_TypeOID != 20)
				return DMibErrorDatabaseInstance("PostgreSQL int8 value has unexpected type");

			if (_Length != 8)
				return DMibErrorDatabaseInstance("PostgreSQL int8 value has invalid binary length");

			int64 Value;
			_Stream >> Value;

			return Value;
		}

		template <>
		NConcurrency::TCWrapped<fp32> fg_PostgresReadBinaryValue<fp32>(CPostgresReadStream &_Stream, uint32 _Length, uint32 _TypeOID)
		{
			if (_TypeOID != 700)
				return DMibErrorDatabaseInstance("PostgreSQL float4 value has unexpected type");

			if (_Length != 4)
				return DMibErrorDatabaseInstance("PostgreSQL float4 value has invalid binary length");

			uint32 Value;
			_Stream >> Value;

			return fg_BitCast<fp32>(Value);
		}

		template <>
		NConcurrency::TCWrapped<fp64> fg_PostgresReadBinaryValue<fp64>(CPostgresReadStream &_Stream, uint32 _Length, uint32 _TypeOID)
		{
			if (_TypeOID != 701)
				return DMibErrorDatabaseInstance("PostgreSQL float8 value has unexpected type");

			if (_Length != 8)
				return DMibErrorDatabaseInstance("PostgreSQL float8 value has invalid binary length");

			uint64 Value;
			_Stream >> Value;

			return fg_BitCast<fp64>(Value);
		}

		template <>
		NConcurrency::TCWrapped<NStr::CStr> fg_PostgresReadBinaryValue<NStr::CStr>(CPostgresReadStream &_Stream, uint32 _Length, uint32 _TypeOID)
		{
			if (_TypeOID != 25 && _TypeOID != 1043)
				return DMibErrorDatabaseInstance("PostgreSQL string value has unexpected type");

			return fg_PostgresReadStringBytes(_Stream, _Length);
		}

		template <>
		NConcurrency::TCWrapped<NContainer::CIOByteVector> fg_PostgresReadBinaryValue<NContainer::CIOByteVector>(CPostgresReadStream &_Stream, uint32 _Length, uint32 _TypeOID)
		{
			if (_TypeOID != 17)
				return DMibErrorDatabaseInstance("PostgreSQL bytes value has unexpected type");

			return fg_PostgresReadBytes(_Stream, _Length);
		}

		template <>
		NConcurrency::TCWrapped<CPostgresDate> fg_PostgresReadBinaryValue<CPostgresDate>(CPostgresReadStream &_Stream, uint32 _Length, uint32 _TypeOID)
		{
			if (_TypeOID != 1082)
				return DMibErrorDatabaseInstance("PostgreSQL date value has unexpected type");

			if (_Length != 4)
				return DMibErrorDatabaseInstance("PostgreSQL date value has invalid binary length");

			int32 Days;
			_Stream >> Days;

			return CPostgresDate{.m_Time = fg_PostgresDaysToTime(Days)};
		}

		template <>
		NConcurrency::TCWrapped<CPostgresTime> fg_PostgresReadBinaryValue<CPostgresTime>(CPostgresReadStream &_Stream, uint32 _Length, uint32 _TypeOID)
		{
			if (_TypeOID != 1083)
				return DMibErrorDatabaseInstance("PostgreSQL time value has unexpected type");

			if (_Length != 8)
				return DMibErrorDatabaseInstance("PostgreSQL time value has invalid binary length");

			int64 Microseconds;
			_Stream >> Microseconds;

			return CPostgresTime{.m_Time = fg_PostgresMicrosecondsToTimeOfDay(Microseconds)};
		}

		template <>
		NConcurrency::TCWrapped<CPostgresTimestamp> fg_PostgresReadBinaryValue<CPostgresTimestamp>(CPostgresReadStream &_Stream, uint32 _Length, uint32 _TypeOID)
		{
			if (_TypeOID != 1114)
				return DMibErrorDatabaseInstance("PostgreSQL timestamp value has unexpected type");

			if (_Length != 8)
				return DMibErrorDatabaseInstance("PostgreSQL timestamp value has invalid binary length");

			int64 Microseconds;
			_Stream >> Microseconds;

			return CPostgresTimestamp{.m_Time = fg_PostgresMicrosecondsToTime(Microseconds)};
		}

		template <>
		NConcurrency::TCWrapped<CPostgresTimestampTz> fg_PostgresReadBinaryValue<CPostgresTimestampTz>(CPostgresReadStream &_Stream, uint32 _Length, uint32 _TypeOID)
		{
			if (_TypeOID != 1184)
				return DMibErrorDatabaseInstance("PostgreSQL timestamptz value has unexpected type");

			if (_Length != 8)
				return DMibErrorDatabaseInstance("PostgreSQL timestamptz value has invalid binary length");

			int64 Microseconds;
			_Stream >> Microseconds;

			return CPostgresTimestampTz{.m_Time = fg_PostgresMicrosecondsToTime(Microseconds)};
		}

		template <>
		auto fg_PostgresReadBinaryValue<NCryptography::CUniversallyUniqueIdentifier>(CPostgresReadStream &_Stream, uint32 _Length, uint32 _TypeOID)
			-> NConcurrency::TCWrapped<NCryptography::CUniversallyUniqueIdentifier>
		{
			if (_TypeOID != 2950)
				return DMibErrorDatabaseInstance("PostgreSQL uuid value has unexpected type");

			if (_Length != 16)
				return DMibErrorDatabaseInstance("PostgreSQL uuid value has invalid binary length");

			uint32 TimeLow;
			uint16 TimeMid;
			uint16 TimeHiAndVersion;
			uint8 ClockSequenceHiAndReserved;
			uint8 ClockSequenceLow;
			uint64 Node = 0;

			_Stream >> TimeLow;
			_Stream >> TimeMid;
			_Stream >> TimeHiAndVersion;
			_Stream >> ClockSequenceHiAndReserved;
			_Stream >> ClockSequenceLow;

			for (uint8 i = 0; i < 6; ++i)
			{
				uint8 Byte;
				_Stream >> Byte;
				Node = (Node << 8) | Byte;
			}

			return NCryptography::CUniversallyUniqueIdentifier(TimeLow, TimeMid, TimeHiAndVersion, uint16(ClockSequenceHiAndReserved << 8 | ClockSequenceLow), Node);
		}

		template <>
		NConcurrency::TCWrapped<NEncoding::CJsonOrdered> fg_PostgresReadBinaryValue<NEncoding::CJsonOrdered>(CPostgresReadStream &_Stream, uint32 _Length, uint32 _TypeOID)
		{
			if (_TypeOID != 114)
				return DMibErrorDatabaseInstance("PostgreSQL json value has unexpected type");

			NConcurrency::TCWrapped<NStr::CStr> String = fg_PostgresReadStringBytes(_Stream, _Length);
			if (!String)
				return fg_Move(String).f_GetException();

			return NEncoding::CJsonOrdered::fs_FromString(String.f_Move());
		}

		template <>
		NConcurrency::TCWrapped<NEncoding::CJsonSorted> fg_PostgresReadBinaryValue<NEncoding::CJsonSorted>(CPostgresReadStream &_Stream, uint32 _Length, uint32 _TypeOID)
		{
			if (_TypeOID != 3802)
				return DMibErrorDatabaseInstance("PostgreSQL jsonb value has unexpected type");

			if (_Length < 1)
				return DMibErrorDatabaseInstance("PostgreSQL jsonb value has invalid binary length");

			uint8 JsonbVersion;
			_Stream >> JsonbVersion;

			if (JsonbVersion != 1)
				return DMibErrorDatabaseInstance("PostgreSQL jsonb value has unsupported binary version");

			NConcurrency::TCWrapped<NStr::CStr> String = fg_PostgresReadStringBytes(_Stream, _Length - 1);
			if (!String)
				return fg_Move(String).f_GetException();

			return NEncoding::CJsonSorted::fs_FromString(String.f_Move());
		}

		template <>
		NConcurrency::TCWrapped<CPostgresInterval> fg_PostgresReadBinaryValue<CPostgresInterval>(CPostgresReadStream &_Stream, uint32 _Length, uint32 _TypeOID)
		{
			if (_TypeOID != 1186)
				return DMibErrorDatabaseInstance("PostgreSQL interval value has unexpected type");

			if (_Length != 16)
				return DMibErrorDatabaseInstance("PostgreSQL interval value has invalid binary length");

			int64 TimeMicroseconds;
			int32 Days;
			int32 Months;

			_Stream >> TimeMicroseconds;
			_Stream >> Days;
			_Stream >> Months;

			return CPostgresInterval
				{
					.m_Months = Months
					, .m_Days = Days
					, .m_Time = fg_PostgresMicrosecondsToTimeSpan(TimeMicroseconds)
				}
			;
		}

		NConcurrency::TCWrapped<CPostgresValue> fg_PostgresReadValue(CPostgresReadStream &_Stream, CPostgresFieldDescription _Field, uint32 _Length);

		template <typename t_CValue>
		NConcurrency::TCWrapped<TCPostgresArray<t_CValue>> fg_PostgresReadArray(CPostgresReadStream &_Stream, uint32 _Length, uint32 _ExpectedElementTypeOID)
		{
			NStream::CFilePos EndPosition = _Stream.f_GetPosition() + _Length;
			if (_Length < 12)
				return DMibErrorDatabaseInstance("PostgreSQL array value has invalid binary length");

			int32 nDimensions;
			int32 bHasNull;
			uint32 ElementTypeOID;
			_Stream >> nDimensions;
			_Stream >> bHasNull;
			_Stream >> ElementTypeOID;

			if (nDimensions < 0)
				return DMibErrorDatabaseInstance("PostgreSQL array value has invalid dimension count");

			if (ElementTypeOID != _ExpectedElementTypeOID)
				return DMibErrorDatabaseInstance("PostgreSQL array value has unexpected element type");

			TCPostgresArray<t_CValue> Array;
			umint nValues = 1;
			for (int32 i = 0; i < nDimensions; ++i)
			{
				if (fg_PostgresRemaining(_Stream) < 8)
					return DMibErrorDatabaseInstance("PostgreSQL array dimension is truncated");

				CPostgresArrayDimension Dimension;
				_Stream >> Dimension.m_Length;
				_Stream >> Dimension.m_LowerBound;

				if (Dimension.m_Length < 0)
					return DMibErrorDatabaseInstance("PostgreSQL array dimension has invalid length");

				Array.m_Dimensions.f_InsertLast(Dimension);
				nValues *= umint(Dimension.m_Length);
			}

			if (nDimensions == 0)
				nValues = 0;

			bool bFoundNull = false;
			for (umint i = 0; i < nValues; ++i)
			{
				if (fg_PostgresRemaining(_Stream) < 4)
					return DMibErrorDatabaseInstance("PostgreSQL array element length is truncated");

				int32 ElementLength;
				_Stream >> ElementLength;

				if (ElementLength < 0)
				{
					Array.m_Values.f_InsertLast(NStorage::TCOptional<t_CValue>());
					bFoundNull = true;
					continue;
				}

				if (fg_PostgresRemaining(_Stream) < uint32(ElementLength))
					return DMibErrorDatabaseInstance("PostgreSQL array element value is truncated");

				NConcurrency::TCWrapped<t_CValue> Value = fg_PostgresReadBinaryValue<t_CValue>(_Stream, uint32(ElementLength), ElementTypeOID);
				if (!Value)
					return fg_Move(Value).f_GetException();

				Array.m_Values.f_InsertLast(Value.f_Move());
			}

			if ((bHasNull != 0) != bFoundNull)
				return DMibErrorDatabaseInstance("PostgreSQL array null flag does not match contents");

			if (_Stream.f_GetPosition() != EndPosition)
				return DMibErrorDatabaseInstance("PostgreSQL array value has trailing data");

			return Array;
		}

		NConcurrency::TCWrapped<CPostgresValue> fg_PostgresReadValue(CPostgresReadStream &_Stream, CPostgresFieldDescription _Field, uint32 _Length)
		{
			if (_Field.m_Format != 1)
				return DMibErrorDatabaseInstance("PostgreSQL value is not in binary format");

			switch (EPostgresValueType(_Field.m_TypeOID))
			{
			case EPostgresValueType::mc_Boolean: return fg_PostgresReadBinaryValue<bool>(_Stream, _Length, _Field.m_TypeOID);
			case EPostgresValueType::mc_Integer16: return fg_PostgresReadBinaryValue<int16>(_Stream, _Length, _Field.m_TypeOID);
			case EPostgresValueType::mc_Integer32: return fg_PostgresReadBinaryValue<int32>(_Stream, _Length, _Field.m_TypeOID);
			case EPostgresValueType::mc_Integer64: return fg_PostgresReadBinaryValue<int64>(_Stream, _Length, _Field.m_TypeOID);
			case EPostgresValueType::mc_Bytes: return fg_PostgresReadBinaryValue<NContainer::CIOByteVector>(_Stream, _Length, _Field.m_TypeOID);
			case EPostgresValueType::mc_Float32: return fg_PostgresReadBinaryValue<fp32>(_Stream, _Length, _Field.m_TypeOID);
			case EPostgresValueType::mc_Float64: return fg_PostgresReadBinaryValue<fp64>(_Stream, _Length, _Field.m_TypeOID);
			case EPostgresValueType::mc_Text:
			case EPostgresValueType::mc_Varchar:
				return fg_PostgresReadBinaryValue<NStr::CStr>(_Stream, _Length, _Field.m_TypeOID);
			case EPostgresValueType::mc_Date: return fg_PostgresReadBinaryValue<CPostgresDate>(_Stream, _Length, _Field.m_TypeOID);
			case EPostgresValueType::mc_Time: return fg_PostgresReadBinaryValue<CPostgresTime>(_Stream, _Length, _Field.m_TypeOID);
			case EPostgresValueType::mc_Timestamp: return fg_PostgresReadBinaryValue<CPostgresTimestamp>(_Stream, _Length, _Field.m_TypeOID);
			case EPostgresValueType::mc_TimestampTz: return fg_PostgresReadBinaryValue<CPostgresTimestampTz>(_Stream, _Length, _Field.m_TypeOID);
			case EPostgresValueType::mc_UUID: return fg_PostgresReadBinaryValue<NCryptography::CUniversallyUniqueIdentifier>(_Stream, _Length, _Field.m_TypeOID);
			case EPostgresValueType::mc_Json: return fg_PostgresReadBinaryValue<NEncoding::CJsonOrdered>(_Stream, _Length, _Field.m_TypeOID);
			case EPostgresValueType::mc_Jsonb: return fg_PostgresReadBinaryValue<NEncoding::CJsonSorted>(_Stream, _Length, _Field.m_TypeOID);
			case EPostgresValueType::mc_Interval: return fg_PostgresReadBinaryValue<CPostgresInterval>(_Stream, _Length, _Field.m_TypeOID);
			case EPostgresValueType::mc_Array_Boolean: return fg_PostgresReadArray<bool>(_Stream, _Length, uint32(EPostgresValueType::mc_Boolean));
			case EPostgresValueType::mc_Array_Bytes: return fg_PostgresReadArray<NContainer::CIOByteVector>(_Stream, _Length, uint32(EPostgresValueType::mc_Bytes));
			case EPostgresValueType::mc_Array_Integer16: return fg_PostgresReadArray<int16>(_Stream, _Length, uint32(EPostgresValueType::mc_Integer16));
			case EPostgresValueType::mc_Array_Integer32: return fg_PostgresReadArray<int32>(_Stream, _Length, uint32(EPostgresValueType::mc_Integer32));
			case EPostgresValueType::mc_Array_Integer64: return fg_PostgresReadArray<int64>(_Stream, _Length, uint32(EPostgresValueType::mc_Integer64));
			case EPostgresValueType::mc_Array_Text: return fg_PostgresReadArray<NStr::CStr>(_Stream, _Length, uint32(EPostgresValueType::mc_Text));
			case EPostgresValueType::mc_Array_Varchar: return fg_PostgresReadArray<NStr::CStr>(_Stream, _Length, uint32(EPostgresValueType::mc_Varchar));
			case EPostgresValueType::mc_Array_Float32: return fg_PostgresReadArray<fp32>(_Stream, _Length, uint32(EPostgresValueType::mc_Float32));
			case EPostgresValueType::mc_Array_Float64: return fg_PostgresReadArray<fp64>(_Stream, _Length, uint32(EPostgresValueType::mc_Float64));
			case EPostgresValueType::mc_Array_Date: return fg_PostgresReadArray<CPostgresDate>(_Stream, _Length, uint32(EPostgresValueType::mc_Date));
			case EPostgresValueType::mc_Array_Time: return fg_PostgresReadArray<CPostgresTime>(_Stream, _Length, uint32(EPostgresValueType::mc_Time));
			case EPostgresValueType::mc_Array_Timestamp: return fg_PostgresReadArray<CPostgresTimestamp>(_Stream, _Length, uint32(EPostgresValueType::mc_Timestamp));
			case EPostgresValueType::mc_Array_TimestampTz: return fg_PostgresReadArray<CPostgresTimestampTz>(_Stream, _Length, uint32(EPostgresValueType::mc_TimestampTz));
			case EPostgresValueType::mc_Array_Interval: return fg_PostgresReadArray<CPostgresInterval>(_Stream, _Length, uint32(EPostgresValueType::mc_Interval));
			case EPostgresValueType::mc_Array_UUID: return fg_PostgresReadArray<NCryptography::CUniversallyUniqueIdentifier>(_Stream, _Length, uint32(EPostgresValueType::mc_UUID));
			case EPostgresValueType::mc_Array_Json: return fg_PostgresReadArray<NEncoding::CJsonOrdered>(_Stream, _Length, uint32(EPostgresValueType::mc_Json));
			case EPostgresValueType::mc_Array_Jsonb: return fg_PostgresReadArray<NEncoding::CJsonSorted>(_Stream, _Length, uint32(EPostgresValueType::mc_Jsonb));
			default: return CPostgresUnrecognizedValue{.m_TypeOID = _Field.m_TypeOID, .m_Bytes = fg_PostgresReadBytes(_Stream, _Length)};
			}
		}

		NStr::CStr fg_PostgresScramEscapeUser(NStr::CStr const &_User)
		{
			NStr::CStr Escaped;
			for (umint i = 0; i < _User.f_GetLen(); ++i)
			{
				ch8 Character = _User.f_GetStr()[i];

				if (Character == ',')
					Escaped += "=2C";
				else if (Character == '=')
					Escaped += "=3D";
				else
					Escaped += NStr::CStr(&Character, 1);
			}

			return Escaped;
		}

		NConcurrency::TCWrapped<NStr::CStr> fg_PostgresScramGetAttribute(NStr::CStr const &_Message, ch8 _Attribute, bool _bRequired)
		{
			umint Offset = 0;
			while (Offset < _Message.f_GetLen())
			{
				umint Start = Offset;
				while (Offset < _Message.f_GetLen() && _Message.f_GetStr()[Offset] != ',')
					++Offset;
				umint End = Offset;
				if (Offset < _Message.f_GetLen())
					++Offset;

				if (End > Start + 2 && _Message.f_GetStr()[Start] == _Attribute && _Message.f_GetStr()[Start + 1] == '=')
					return NStr::CStr(_Message.f_GetStr() + Start + 2, End - Start - 2);
			}

			if (_bRequired)
				return DMibErrorDatabaseInstance("PostgreSQL SCRAM message is missing required attribute");

			return NStr::CStr();
		}

		NConcurrency::TCWrapped<uint32> fg_PostgresScramParseIterations(NStr::CStr const &_Value)
		{
			uint32 Iterations = 0;
			for (umint i = 0; i < _Value.f_GetLen(); ++i)
			{
				ch8 Character = _Value.f_GetStr()[i];

				if (Character < '0' || Character > '9')
					return DMibErrorDatabaseInstance("PostgreSQL SCRAM iteration count is invalid");

				Iterations = Iterations * 10 + uint32(Character - '0');
			}

			if (Iterations == 0)
				return DMibErrorDatabaseInstance("PostgreSQL SCRAM iteration count is invalid");

			return Iterations;
		}

		NContainer::CByteVector fg_PostgresByteVectorFromDigest(NCryptography::CHashDigest_SHA256 const &_Digest)
		{
			CPostgresWriteStream Stream;
			Stream.f_FeedBytes(_Digest.f_GetData(), _Digest.mc_Size);

			return Stream.f_MoveVector();
		}
	}

	void fg_PostgresWriteSSLRequest(CPostgresWriteStream &_Stream)
	{
		_Stream << uint32(8);
		_Stream << uint32(80877103);
	}

	NContainer::CIOByteVector fg_PostgresBuildSSLRequest()
	{
		CPostgresWriteStream Message;
		fg_PostgresWriteSSLRequest(Message);

		return Message.f_MoveVector();
	}

	void fg_PostgresWriteStartupMessage(CPostgresWriteStream &_Stream, CPostgresConnectionSettings const &_Settings)
	{
		NStream::CFilePos LengthPosition = _Stream.f_GetPosition();
		_Stream << uint32(0);
		_Stream << uint32(196608); // Protocol version 3.0.
		fg_PostgresWriteCString(_Stream, "user");
		fg_PostgresWriteCString(_Stream, _Settings.m_User);
		fg_PostgresWriteCString(_Stream, "database");
		fg_PostgresWriteCString(_Stream, _Settings.m_Database);
		fg_PostgresWriteCString(_Stream, "application_name");
		fg_PostgresWriteCString(_Stream, _Settings.m_ApplicationName);
		fg_PostgresWriteCString(_Stream, "client_encoding");
		fg_PostgresWriteCString(_Stream, "UTF8");
		_Stream << uint8(0);
		fg_PostgresBackfillMessageLength(_Stream, LengthPosition);
	}

	NContainer::CIOByteVector fg_PostgresBuildStartupMessage(CPostgresConnectionSettings const &_Settings)
	{
		CPostgresWriteStream Message;
		fg_PostgresWriteStartupMessage(Message, _Settings);

		return Message.f_MoveVector();
	}

	void fg_PostgresWriteFrontendMessage(CPostgresWriteStream &_Stream, uint8 _Type, NContainer::CIOByteVector const &_Payload)
	{
		NStream::CFilePos LengthPosition = fg_PostgresBeginFrontendMessage(_Stream, _Type);
		fg_PostgresWriteBytes(_Stream, _Payload);
		fg_PostgresFinishFrontendMessage(_Stream, LengthPosition);
	}

	NContainer::CIOByteVector fg_PostgresBuildFrontendMessage(uint8 _Type, NContainer::CIOByteVector const &_Payload)
	{
		CPostgresWriteStream Message;
		fg_PostgresWriteFrontendMessage(Message, _Type, _Payload);

		return Message.f_MoveVector();
	}

	uint32 fg_PostgresGetValueTypeOID(EPostgresValueType _Type)
	{
		if (_Type == EPostgresValueType::mc_Unrecognized)
			return 0;

		return uint32(_Type);
	}

	NContainer::TCVector<uint32> fg_PostgresBuildParameterTypeOIDs(NContainer::TCVector<CPostgresValue> const &_Parameters)
	{
		NContainer::TCVector<uint32> ParameterTypeOIDs;
		for (auto const &Parameter : _Parameters)
			ParameterTypeOIDs.f_InsertLast(fg_PostgresGetParameterTypeOID(Parameter));

		return ParameterTypeOIDs;
	}

	void fg_PostgresWriteSASLInitialResponse(CPostgresWriteStream &_Stream, NStr::CStr const &_Mechanism, NStr::CStr const &_InitialResponse)
	{
		NStream::CFilePos LengthPosition = fg_PostgresBeginFrontendMessage(_Stream, 'p');
		fg_PostgresWriteCString(_Stream, _Mechanism);
		_Stream << uint32(_InitialResponse.f_GetLen());
		fg_PostgresWriteStringBytes(_Stream, _InitialResponse);
		fg_PostgresFinishFrontendMessage(_Stream, LengthPosition);
	}

	NContainer::CIOByteVector fg_PostgresBuildSASLInitialResponse(NStr::CStr const &_Mechanism, NStr::CStr const &_InitialResponse)
	{
		CPostgresWriteStream Message;
		fg_PostgresWriteSASLInitialResponse(Message, _Mechanism, _InitialResponse);

		return Message.f_MoveVector();
	}

	void fg_PostgresWriteSASLResponse(CPostgresWriteStream &_Stream, NStr::CStr const &_Response)
	{
		NStream::CFilePos LengthPosition = fg_PostgresBeginFrontendMessage(_Stream, 'p');
		fg_PostgresWriteStringBytes(_Stream, _Response);
		fg_PostgresFinishFrontendMessage(_Stream, LengthPosition);
	}

	NContainer::CIOByteVector fg_PostgresBuildSASLResponse(NStr::CStr const &_Response)
	{
		CPostgresWriteStream Message;
		fg_PostgresWriteSASLResponse(Message, _Response);

		return Message.f_MoveVector();
	}

	void fg_PostgresWriteParse(CPostgresWriteStream &_Stream, NStr::CStr const &_StatementName, NStr::CStr const &_Sql, NContainer::TCVector<uint32> const &_ParameterTypeOIDs)
	{
		NStream::CFilePos LengthPosition = fg_PostgresBeginFrontendMessage(_Stream, 'P');
		fg_PostgresWriteCString(_Stream, _StatementName);
		fg_PostgresWriteCString(_Stream, _Sql);
		_Stream << uint16(_ParameterTypeOIDs.f_GetLen());
		for (uint32 OID : _ParameterTypeOIDs)
			_Stream << OID;
		fg_PostgresFinishFrontendMessage(_Stream, LengthPosition);
	}

	NContainer::CIOByteVector fg_PostgresBuildParse(NStr::CStr const &_StatementName, NStr::CStr const &_Sql, NContainer::TCVector<uint32> const &_ParameterTypeOIDs)
	{
		CPostgresWriteStream Message;
		fg_PostgresWriteParse(Message, _StatementName, _Sql, _ParameterTypeOIDs);

		return Message.f_MoveVector();
	}

	NConcurrency::TCWrapped<void> fg_PostgresWriteBind
		(
			CPostgresWriteStream &_Stream
			, NStr::CStr const &_PortalName
			, NStr::CStr const &_StatementName
			, NContainer::TCVector<CPostgresValue> const &_Parameters
			, NContainer::TCVector<uint16> const &_ResultFormats
		)
	{
		NStream::CFilePos LengthPosition = fg_PostgresBeginFrontendMessage(_Stream, 'B');
		fg_PostgresWriteCString(_Stream, _PortalName);
		fg_PostgresWriteCString(_Stream, _StatementName);
		_Stream << uint16(_Parameters.f_GetLen());
		for (umint i = 0; i < _Parameters.f_GetLen(); ++i)
			_Stream << uint16(1);
		_Stream << uint16(_Parameters.f_GetLen());
		for (auto const &Parameter : _Parameters)
		{
			auto Result = fg_PostgresWriteParameter(_Stream, Parameter);
			if (!Result)
				return fg_Move(Result);
		}
		_Stream << uint16(_ResultFormats.f_GetLen());
		for (uint16 Format : _ResultFormats)
			_Stream << Format;
		fg_PostgresFinishFrontendMessage(_Stream, LengthPosition);

		return {};
	}

	NConcurrency::TCWrapped<NContainer::CIOByteVector> fg_PostgresBuildBind
		(
			NStr::CStr const &_PortalName
			, NStr::CStr const &_StatementName
			, NContainer::TCVector<CPostgresValue> const &_Parameters
			, NContainer::TCVector<uint16> const &_ResultFormats
		)
	{
		CPostgresWriteStream Message;
		auto Result = fg_PostgresWriteBind(Message, _PortalName, _StatementName, _Parameters, _ResultFormats);
		if (!Result)
			return fg_Move(Result).f_GetException();

		return Message.f_MoveVector();
	}

	void fg_PostgresWriteDescribe(CPostgresWriteStream &_Stream, EPostgresDescribeTarget _Target, NStr::CStr const &_Name)
	{
		NStream::CFilePos LengthPosition = fg_PostgresBeginFrontendMessage(_Stream, 'D');
		_Stream << (_Target == EPostgresDescribeTarget::mc_Statement ? uint8('S') : uint8('P'));
		fg_PostgresWriteCString(_Stream, _Name);
		fg_PostgresFinishFrontendMessage(_Stream, LengthPosition);
	}

	NContainer::CIOByteVector fg_PostgresBuildDescribe(EPostgresDescribeTarget _Target, NStr::CStr const &_Name)
	{
		CPostgresWriteStream Message;
		fg_PostgresWriteDescribe(Message, _Target, _Name);

		return Message.f_MoveVector();
	}

	void fg_PostgresWriteExecute(CPostgresWriteStream &_Stream, NStr::CStr const &_PortalName, uint32 _MaxRows)
	{
		NStream::CFilePos LengthPosition = fg_PostgresBeginFrontendMessage(_Stream, 'E');
		fg_PostgresWriteCString(_Stream, _PortalName);
		_Stream << _MaxRows;
		fg_PostgresFinishFrontendMessage(_Stream, LengthPosition);
	}

	void fg_PostgresWriteClose(CPostgresWriteStream &_Stream, EPostgresDescribeTarget _Target, NStr::CStr const &_Name)
	{
		NStream::CFilePos LengthPosition = fg_PostgresBeginFrontendMessage(_Stream, 'C');
		_Stream << (_Target == EPostgresDescribeTarget::mc_Statement ? uint8('S') : uint8('P'));
		fg_PostgresWriteCString(_Stream, _Name);
		fg_PostgresFinishFrontendMessage(_Stream, LengthPosition);
	}

	NContainer::CIOByteVector fg_PostgresBuildExecute(NStr::CStr const &_PortalName, uint32 _MaxRows)
	{
		CPostgresWriteStream Message;
		fg_PostgresWriteExecute(Message, _PortalName, _MaxRows);

		return Message.f_MoveVector();
	}

	void fg_PostgresWriteSync(CPostgresWriteStream &_Stream)
	{
		fg_PostgresWriteFrontendMessage(_Stream, 'S', {});
	}

	void fg_PostgresWriteFlush(CPostgresWriteStream &_Stream)
	{
		// Flush asks the backend to deliver the responses it has buffered so far without ending the implicit
		// transaction the way Sync does, so a long pipeline can drain backend responses while staying atomic.
		fg_PostgresWriteFrontendMessage(_Stream, 'H', {});
	}

	NContainer::CIOByteVector fg_PostgresBuildSync()
	{
		CPostgresWriteStream Message;
		fg_PostgresWriteSync(Message);

		return Message.f_MoveVector();
	}

	void fg_PostgresWriteTerminate(CPostgresWriteStream &_Stream)
	{
		fg_PostgresWriteFrontendMessage(_Stream, 'X', {});
	}

	NContainer::CIOByteVector fg_PostgresBuildTerminate()
	{
		CPostgresWriteStream Message;
		fg_PostgresWriteTerminate(Message);

		return Message.f_MoveVector();
	}

	CPostgresScramClientFirstMessage fg_PostgresScramBuildClientFirstMessage(NStr::CStr const &_User, NStr::CStr _ClientNonce, NStr::CStr const &_GS2Header)
	{
		if (_ClientNonce.f_IsEmpty())
			_ClientNonce = NCryptography::fg_HighEntropyRandomID(24);

		CPostgresScramClientFirstMessage Message;
		Message.m_ClientNonce = fg_Move(_ClientNonce);
		Message.m_GS2Header = _GS2Header;
		Message.m_BareMessage = "n=";
		Message.m_BareMessage += fg_PostgresScramEscapeUser(_User);
		Message.m_BareMessage += ",r=";
		Message.m_BareMessage += Message.m_ClientNonce;
		Message.m_Message = Message.m_GS2Header;
		Message.m_Message += Message.m_BareMessage;

		return Message;
	}

	NConcurrency::TCWrapped<CPostgresScramServerFirstMessage> fg_PostgresScramParseServerFirstMessage(NStr::CStr const &_Message, NStr::CStr const &_ClientNonce)
	{
		CPostgresScramServerFirstMessage Message;
		auto WrappedNonce = fg_PostgresScramGetAttribute(_Message, 'r', true);
		if (!WrappedNonce)
			return fg_Move(WrappedNonce).f_GetException();

		Message.m_Nonce = fg_Move(*WrappedNonce);
		if (!Message.m_Nonce.f_StartsWith(_ClientNonce))
			return DMibErrorDatabaseInstance("PostgreSQL SCRAM server nonce does not include the client nonce");

		auto WrappedSalt = fg_PostgresScramGetAttribute(_Message, 's', true);
		if (!WrappedSalt)
			return fg_Move(WrappedSalt).f_GetException();

		NEncoding::fg_Base64Decode(*WrappedSalt, Message.m_Salt);

		auto WrappedIterationsAttr = fg_PostgresScramGetAttribute(_Message, 'i', true);
		if (!WrappedIterationsAttr)
			return fg_Move(WrappedIterationsAttr).f_GetException();

		auto WrappedIterations = fg_PostgresScramParseIterations(*WrappedIterationsAttr);
		if (!WrappedIterations)
			return fg_Move(WrappedIterations).f_GetException();

		Message.m_Iterations = *WrappedIterations;

		return Message;
	}

	auto fg_PostgresScramBuildClientFinalMessage
		(
			NStr::CStrSecure const &_Password
			, CPostgresScramClientFirstMessage const &_ClientFirst
			, CPostgresScramServerFirstMessage const &_ServerFirst
			, NContainer::CByteVector const &_ChannelBindingData
		)
		-> CPostgresScramClientFinalMessage
	{
		CPostgresWriteStream ChannelBinding;
		ChannelBinding.f_FeedBytes(_ClientFirst.m_GS2Header.f_GetStr(), _ClientFirst.m_GS2Header.f_GetLen());
		ChannelBinding.f_FeedBytes(_ChannelBindingData.f_GetArray(), _ChannelBindingData.f_GetLen());

		CPostgresScramClientFinalMessage Message;
		Message.m_WithoutProof = "c=";
		Message.m_WithoutProof += NEncoding::fg_Base64Encode(ChannelBinding.f_MoveVector());
		Message.m_WithoutProof += ",r=";
		Message.m_WithoutProof += _ServerFirst.m_Nonce;
		Message.m_AuthMessage = _ClientFirst.m_BareMessage;
		Message.m_AuthMessage += ",";
		Message.m_AuthMessage += "r=";
		Message.m_AuthMessage += _ServerFirst.m_Nonce;
		Message.m_AuthMessage += ",s=";
		Message.m_AuthMessage += NEncoding::fg_Base64Encode(_ServerFirst.m_Salt);
		Message.m_AuthMessage += ",i=";
		Message.m_AuthMessage += NStr::CStr::CFormat("{}") << _ServerFirst.m_Iterations;
		Message.m_AuthMessage += ",";
		Message.m_AuthMessage += Message.m_WithoutProof;

		NCryptography::CScramSHA256Keys Keys = NCryptography::fg_ScramSHA256DeriveKeys(_Password, _ServerFirst.m_Salt, _ServerFirst.m_Iterations);
		NContainer::CByteVector Proof = NCryptography::fg_ScramSHA256ClientProof(Keys, Message.m_AuthMessage);
		Message.m_ServerSignature = fg_PostgresByteVectorFromDigest(NCryptography::fg_ScramSHA256ServerSignature(Keys, Message.m_AuthMessage));
		Message.m_Message = Message.m_WithoutProof;
		Message.m_Message += ",p=";
		Message.m_Message += NEncoding::fg_Base64Encode(Proof);

		return Message;
	}

	NConcurrency::TCWrapped<CPostgresScramServerFinalMessage> fg_PostgresScramParseServerFinalMessage(NStr::CStr const &_Message)
	{
		CPostgresScramServerFinalMessage Message;
		auto WrappedError = fg_PostgresScramGetAttribute(_Message, 'e', false);
		if (!WrappedError)
			return fg_Move(WrappedError).f_GetException();

		if (!(*WrappedError).f_IsEmpty())
			return DMibErrorDatabaseInstance("PostgreSQL SCRAM server rejected authentication");

		auto WrappedSignature = fg_PostgresScramGetAttribute(_Message, 'v', true);
		if (!WrappedSignature)
			return fg_Move(WrappedSignature).f_GetException();

		NEncoding::fg_Base64Decode(*WrappedSignature, Message.m_ServerSignature);

		return Message;
	}

	bool fg_PostgresScramVerifyServerFinalMessage(CPostgresScramClientFinalMessage const &_ClientFinal, CPostgresScramServerFinalMessage const &_ServerFinal)
	{
		if (_ClientFinal.m_ServerSignature.f_GetLen() != _ServerFinal.m_ServerSignature.f_GetLen())
			return false;

		return NCryptography::fg_CryptographyConstantTimeEquals
			(
				_ClientFinal.m_ServerSignature.f_GetArray()
				, _ServerFinal.m_ServerSignature.f_GetArray()
				, _ClientFinal.m_ServerSignature.f_GetLen()
			)
		;
	}

	NConcurrency::TCWrapped<CPostgresBackendMessage> fg_PostgresReadBackendMessage(NContainer::CIOByteVector _Message)
	{
		if (_Message.f_GetLen() < 5)
			return DMibErrorDatabaseInstance("PostgreSQL backend message is too short");

		uint8 const *pData = _Message.f_GetArray();
		uint8 Type = pData[0];
		uint32 Length = (uint32(pData[1]) << 24) | (uint32(pData[2]) << 16) | (uint32(pData[3]) << 8) | uint32(pData[4]);
		if (Length < 4)
			return DMibErrorDatabaseInstance("PostgreSQL backend message has invalid length");

		if (Length + 1 != _Message.f_GetLen())
			return DMibErrorDatabaseInstance("PostgreSQL backend message length does not match frame size");

		CPostgresBackendMessage Result;
		Result.m_Type = Type;
		Result.m_Payload.f_Reserve(Length - 4);
		Result.m_Payload.f_InsertLast(pData + 5, Length - 4);

		return Result;
	}

	// Internal decoders: caller has already extracted the message type byte and opened the
	// stream over the message's payload. They consume the payload bytes from the stream.

	NConcurrency::TCWrapped<CPostgresAuthenticationRequest> fg_PostgresDecodeAuthenticationRequest(CPostgresReadStream &_Stream)
	{
		if (_Stream.f_GetLength() < 4)
			return DMibErrorDatabaseInstance("PostgreSQL authentication request is too short");

		uint32 AuthType;
		_Stream >> AuthType;

		CPostgresAuthenticationRequest Request;
		Request.m_Type = EPostgresAuthenticationRequestType(AuthType);

		if (Request.m_Type == EPostgresAuthenticationRequestType::mc_SASL)
		{
			while (!_Stream.f_IsAtEndOfStream())
			{
				NConcurrency::TCWrapped<NStr::CStr> MechanismResult = fg_PostgresReadCString(_Stream);
				if (!MechanismResult)
					return fg_Move(MechanismResult).f_GetException();
				NStr::CStr Mechanism = MechanismResult.f_Move();
				if (Mechanism.f_IsEmpty())
					break;
				Request.m_SASLMechanisms.f_InsertLast(fg_Move(Mechanism));
			}
		}
		else if (Request.m_Type == EPostgresAuthenticationRequestType::mc_SASLContinue || Request.m_Type == EPostgresAuthenticationRequestType::mc_SASLFinal)
			Request.m_SASLData = fg_PostgresReadBytes(_Stream, fg_PostgresRemaining(_Stream));

		return Request;
	}

	NConcurrency::TCWrapped<CPostgresParameterStatus> fg_PostgresDecodeParameterStatus(CPostgresReadStream &_Stream)
	{
		CPostgresParameterStatus Status;

		NConcurrency::TCWrapped<NStr::CStr> Name = fg_PostgresReadCString(_Stream);

		if (!Name)
			return fg_Move(Name).f_GetException();

		NConcurrency::TCWrapped<NStr::CStr> Value = fg_PostgresReadCString(_Stream);

		if (!Value)
			return fg_Move(Value).f_GetException();

		Status.m_Name = Name.f_Move();
		Status.m_Value = Value.f_Move();

		return Status;
	}

	NConcurrency::TCWrapped<CPostgresBackendKeyData> fg_PostgresDecodeBackendKeyData(CPostgresReadStream &_Stream)
	{
		if (_Stream.f_GetLength() != 8)
			return DMibErrorDatabaseInstance("PostgreSQL backend key data has unexpected length");

		CPostgresBackendKeyData KeyData;
		_Stream >> KeyData.m_ProcessID >> KeyData.m_SecretKey;

		return KeyData;
	}

	NConcurrency::TCWrapped<EPostgresReadyForQueryStatus> fg_PostgresDecodeReadyForQuery(CPostgresReadStream &_Stream)
	{
		if (_Stream.f_GetLength() != 1)
			return DMibErrorDatabaseInstance("PostgreSQL ReadyForQuery has unexpected length");

		uint8 Status;
		_Stream >> Status;

		return EPostgresReadyForQueryStatus(Status);
	}

	NConcurrency::TCWrapped<CPostgresCommandComplete> fg_PostgresDecodeCommandComplete(CPostgresReadStream &_Stream)
	{
		NConcurrency::TCWrapped<NStr::CStr> Tag = fg_PostgresReadCString(_Stream);
		if (!Tag)
			return fg_Move(Tag).f_GetException();

		return CPostgresCommandComplete{.m_Tag = Tag.f_Move()};
	}

	NConcurrency::TCWrapped<void> fg_PostgresDecodeParseComplete(CPostgresReadStream &_Stream)
	{
		if (!_Stream.f_IsAtEndOfStream())
			return DMibErrorDatabaseInstance("PostgreSQL ParseComplete has unexpected payload");

		return {};
	}

	NConcurrency::TCWrapped<void> fg_PostgresDecodeBindComplete(CPostgresReadStream &_Stream)
	{
		if (!_Stream.f_IsAtEndOfStream())
			return DMibErrorDatabaseInstance("PostgreSQL BindComplete has unexpected payload");

		return {};
	}

	NConcurrency::TCWrapped<void> fg_PostgresDecodeCloseComplete(CPostgresReadStream &_Stream)
	{
		if (!_Stream.f_IsAtEndOfStream())
			return DMibErrorDatabaseInstance("PostgreSQL CloseComplete has unexpected payload");

		return {};
	}

	NConcurrency::TCWrapped<void> fg_PostgresDecodeNoData(CPostgresReadStream &_Stream)
	{
		if (!_Stream.f_IsAtEndOfStream())
			return DMibErrorDatabaseInstance("PostgreSQL NoData has unexpected payload");

		return {};
	}

	NConcurrency::TCWrapped<CPostgresErrorResponse> fg_PostgresDecodeErrorResponse(CPostgresReadStream &_Stream)
	{
		CPostgresErrorResponse Error;
		while (!_Stream.f_IsAtEndOfStream())
		{
			uint8 FieldType;
			_Stream >> FieldType;
			if (FieldType == 0)
				break;
			NConcurrency::TCWrapped<NStr::CStr> ValueResult = fg_PostgresReadCString(_Stream);
			if (!ValueResult)
				return fg_Move(ValueResult).f_GetException();
			NStr::CStr Value = ValueResult.f_Move();
			switch (FieldType)
			{
			case 'S':
				Error.m_Severity = fg_Move(Value);
				break;
			case 'C':
				Error.m_Code = fg_Move(Value);
				break;
			case 'M':
				Error.m_Message = fg_Move(Value);
				break;
			case 'D':
				Error.m_Detail = fg_Move(Value);
				break;
			case 'H':
				Error.m_Hint = fg_Move(Value);
				break;
			}
		}

		return Error;
	}

	NConcurrency::TCWrapped<CPostgresRowDescription> fg_PostgresDecodeRowDescription(CPostgresReadStream &_Stream)
	{
		if (_Stream.f_GetLength() < 2)
			return DMibErrorDatabaseInstance("PostgreSQL RowDescription is too short");

		CPostgresRowDescription Description;
		uint16 nFields;
		_Stream >> nFields;
		for (uint16 i = 0; i < nFields; ++i)
		{
			CPostgresFieldDescription &Field = Description.m_Fields.f_InsertLast();
			NConcurrency::TCWrapped<NStr::CStr> Name = fg_PostgresReadCString(_Stream);
			if (!Name)
				return fg_Move(Name).f_GetException();

			Field.m_Name = Name.f_Move();

			if (fg_PostgresRemaining(_Stream) < 18)
				return DMibErrorDatabaseInstance("PostgreSQL RowDescription field is truncated");

			_Stream >> Field.m_TableOID;
			_Stream >> Field.m_ColumnAttributeNumber;
			_Stream >> Field.m_TypeOID;
			_Stream >> Field.m_TypeSize;
			_Stream >> Field.m_TypeModifier;
			_Stream >> Field.m_Format;
		}

		return Description;
	}

	NConcurrency::TCWrapped<CPostgresDataRow> fg_PostgresDecodeDataRow(CPostgresReadStream &_Stream, CPostgresRowDescription const &_Description)
	{
		if (_Stream.f_GetLength() < 2)
			return DMibErrorDatabaseInstance("PostgreSQL DataRow is too short");

		CPostgresDataRow Row;
		uint16 nColumns;
		_Stream >> nColumns;
		if (nColumns != _Description.m_Fields.f_GetLen())
			return DMibErrorDatabaseInstance("PostgreSQL DataRow column count does not match RowDescription");

		for (uint16 i = 0; i < nColumns; ++i)
		{
			if (fg_PostgresRemaining(_Stream) < 4)
				return DMibErrorDatabaseInstance("PostgreSQL DataRow column length is truncated");

			uint32 Length;
			_Stream >> Length;
			if (Length == 0xffffffff)
			{
				Row.m_Values.f_InsertLast(CPostgresValue());
				continue;
			}

			if (fg_PostgresRemaining(_Stream) < Length)
				return DMibErrorDatabaseInstance("PostgreSQL DataRow column value is truncated");

			NConcurrency::TCWrapped<CPostgresValue> Value = fg_PostgresReadValue(_Stream, _Description.m_Fields[i], Length);
			if (!Value)
				return fg_Move(Value).f_GetException();
			Row.m_Values.f_InsertLast(Value.f_Move());
		}

		return Row;
	}

	// Test-facing wrappers: validate the message type, copy the payload into a temporary
	// paged buffer, and delegate to the internal stream decoder.

	NConcurrency::TCWrapped<CPostgresAuthenticationRequest> fg_PostgresDecodeAuthenticationRequest(CPostgresBackendMessage _Message)
	{
		if (_Message.m_Type != 'R')
			return DMibErrorDatabaseInstance("PostgreSQL message is not an authentication request");

		NContainer::CPagedByteVector Buffer;
		if (!_Message.m_Payload.f_IsEmpty())
			Buffer.f_InsertBack(_Message.m_Payload.f_GetArray(), _Message.m_Payload.f_GetLen());
		CPostgresReadStream Stream;
		Stream.f_OpenRead(Buffer);

		return fg_PostgresDecodeAuthenticationRequest(Stream);
	}

	NConcurrency::TCWrapped<CPostgresParameterStatus> fg_PostgresDecodeParameterStatus(CPostgresBackendMessage _Message)
	{
		if (_Message.m_Type != 'S')
			return DMibErrorDatabaseInstance("PostgreSQL message is not a parameter status");

		NContainer::CPagedByteVector Buffer;
		if (!_Message.m_Payload.f_IsEmpty())
			Buffer.f_InsertBack(_Message.m_Payload.f_GetArray(), _Message.m_Payload.f_GetLen());
		CPostgresReadStream Stream;
		Stream.f_OpenRead(Buffer);

		return fg_PostgresDecodeParameterStatus(Stream);
	}

	NConcurrency::TCWrapped<CPostgresBackendKeyData> fg_PostgresDecodeBackendKeyData(CPostgresBackendMessage _Message)
	{
		if (_Message.m_Type != 'K')
			return DMibErrorDatabaseInstance("PostgreSQL message is not backend key data");

		NContainer::CPagedByteVector Buffer;
		if (!_Message.m_Payload.f_IsEmpty())
			Buffer.f_InsertBack(_Message.m_Payload.f_GetArray(), _Message.m_Payload.f_GetLen());
		CPostgresReadStream Stream;
		Stream.f_OpenRead(Buffer);

		return fg_PostgresDecodeBackendKeyData(Stream);
	}

	NConcurrency::TCWrapped<EPostgresReadyForQueryStatus> fg_PostgresDecodeReadyForQuery(CPostgresBackendMessage _Message)
	{
		if (_Message.m_Type != 'Z')
			return DMibErrorDatabaseInstance("PostgreSQL message is not ReadyForQuery");

		NContainer::CPagedByteVector Buffer;
		if (!_Message.m_Payload.f_IsEmpty())
			Buffer.f_InsertBack(_Message.m_Payload.f_GetArray(), _Message.m_Payload.f_GetLen());
		CPostgresReadStream Stream;
		Stream.f_OpenRead(Buffer);

		return fg_PostgresDecodeReadyForQuery(Stream);
	}

	NConcurrency::TCWrapped<CPostgresCommandComplete> fg_PostgresDecodeCommandComplete(CPostgresBackendMessage _Message)
	{
		if (_Message.m_Type != 'C')
			return DMibErrorDatabaseInstance("PostgreSQL message is not CommandComplete");

		NContainer::CPagedByteVector Buffer;
		if (!_Message.m_Payload.f_IsEmpty())
			Buffer.f_InsertBack(_Message.m_Payload.f_GetArray(), _Message.m_Payload.f_GetLen());
		CPostgresReadStream Stream;
		Stream.f_OpenRead(Buffer);

		return fg_PostgresDecodeCommandComplete(Stream);
	}

	NConcurrency::TCWrapped<void> fg_PostgresDecodeParseComplete(CPostgresBackendMessage _Message)
	{
		if (_Message.m_Type != '1')
			return DMibErrorDatabaseInstance("PostgreSQL message is not ParseComplete");

		NContainer::CPagedByteVector Buffer;
		if (!_Message.m_Payload.f_IsEmpty())
			Buffer.f_InsertBack(_Message.m_Payload.f_GetArray(), _Message.m_Payload.f_GetLen());
		CPostgresReadStream Stream;
		Stream.f_OpenRead(Buffer);

		return fg_PostgresDecodeParseComplete(Stream);
	}

	NConcurrency::TCWrapped<void> fg_PostgresDecodeBindComplete(CPostgresBackendMessage _Message)
	{
		if (_Message.m_Type != '2')
			return DMibErrorDatabaseInstance("PostgreSQL message is not BindComplete");

		NContainer::CPagedByteVector Buffer;
		if (!_Message.m_Payload.f_IsEmpty())
			Buffer.f_InsertBack(_Message.m_Payload.f_GetArray(), _Message.m_Payload.f_GetLen());
		CPostgresReadStream Stream;
		Stream.f_OpenRead(Buffer);

		return fg_PostgresDecodeBindComplete(Stream);
	}

	NConcurrency::TCWrapped<void> fg_PostgresDecodeCloseComplete(CPostgresBackendMessage _Message)
	{
		if (_Message.m_Type != '3')
			return DMibErrorDatabaseInstance("PostgreSQL message is not CloseComplete");

		NContainer::CPagedByteVector Buffer;
		if (!_Message.m_Payload.f_IsEmpty())
			Buffer.f_InsertBack(_Message.m_Payload.f_GetArray(), _Message.m_Payload.f_GetLen());
		CPostgresReadStream Stream;
		Stream.f_OpenRead(Buffer);

		return fg_PostgresDecodeCloseComplete(Stream);
	}

	NConcurrency::TCWrapped<void> fg_PostgresDecodeNoData(CPostgresBackendMessage _Message)
	{
		if (_Message.m_Type != 'n')
			return DMibErrorDatabaseInstance("PostgreSQL message is not NoData");

		NContainer::CPagedByteVector Buffer;
		if (!_Message.m_Payload.f_IsEmpty())
			Buffer.f_InsertBack(_Message.m_Payload.f_GetArray(), _Message.m_Payload.f_GetLen());
		CPostgresReadStream Stream;
		Stream.f_OpenRead(Buffer);

		return fg_PostgresDecodeNoData(Stream);
	}

	NConcurrency::TCWrapped<CPostgresErrorResponse> fg_PostgresDecodeErrorResponse(CPostgresBackendMessage _Message)
	{
		if (_Message.m_Type != 'E' && _Message.m_Type != 'N')
			return DMibErrorDatabaseInstance("PostgreSQL message is not ErrorResponse or NoticeResponse");

		NContainer::CPagedByteVector Buffer;
		if (!_Message.m_Payload.f_IsEmpty())
			Buffer.f_InsertBack(_Message.m_Payload.f_GetArray(), _Message.m_Payload.f_GetLen());
		CPostgresReadStream Stream;
		Stream.f_OpenRead(Buffer);

		return fg_PostgresDecodeErrorResponse(Stream);
	}

	NConcurrency::TCWrapped<CPostgresRowDescription> fg_PostgresDecodeRowDescription(CPostgresBackendMessage _Message)
	{
		if (_Message.m_Type != 'T')
			return DMibErrorDatabaseInstance("PostgreSQL message is not RowDescription");

		NContainer::CPagedByteVector Buffer;
		if (!_Message.m_Payload.f_IsEmpty())
			Buffer.f_InsertBack(_Message.m_Payload.f_GetArray(), _Message.m_Payload.f_GetLen());
		CPostgresReadStream Stream;
		Stream.f_OpenRead(Buffer);

		return fg_PostgresDecodeRowDescription(Stream);
	}

	NConcurrency::TCWrapped<CPostgresDataRow> fg_PostgresDecodeDataRow(CPostgresBackendMessage _Message, CPostgresRowDescription _Description)
	{
		if (_Message.m_Type != 'D')
			return DMibErrorDatabaseInstance("PostgreSQL message is not DataRow");

		NContainer::CPagedByteVector Buffer;
		if (!_Message.m_Payload.f_IsEmpty())
			Buffer.f_InsertBack(_Message.m_Payload.f_GetArray(), _Message.m_Payload.f_GetLen());
		CPostgresReadStream Stream;
		Stream.f_OpenRead(Buffer);

		return fg_PostgresDecodeDataRow(Stream, _Description);
	}
}
