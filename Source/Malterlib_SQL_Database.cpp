// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#include <Mib/SQL/Database>

namespace NMib::NSQL
{
	NStr::CStr fg_SqlColumnTypeName(ESqlColumnType _Type)
	{
		switch (_Type)
		{
		case ESqlColumnType::mc_Invalid: return NStr::gc_Str<"Invalid">;
		case ESqlColumnType::mc_Integer8: return NStr::gc_Str<"Integer8">;
		case ESqlColumnType::mc_Integer16: return NStr::gc_Str<"Integer16">;
		case ESqlColumnType::mc_Integer32: return NStr::gc_Str<"Integer32">;
		case ESqlColumnType::mc_Integer64: return NStr::gc_Str<"Integer64">;
		case ESqlColumnType::mc_UnsignedInteger8: return NStr::gc_Str<"UnsignedInteger8">;
		case ESqlColumnType::mc_UnsignedInteger16: return NStr::gc_Str<"UnsignedInteger16">;
		case ESqlColumnType::mc_UnsignedInteger32: return NStr::gc_Str<"UnsignedInteger32">;
		case ESqlColumnType::mc_UnsignedInteger64: return NStr::gc_Str<"UnsignedInteger64">;
		case ESqlColumnType::mc_Float32: return NStr::gc_Str<"Float32">;
		case ESqlColumnType::mc_Float64: return NStr::gc_Str<"Float64">;
		case ESqlColumnType::mc_Text: return NStr::gc_Str<"Text">;
		case ESqlColumnType::mc_Blob: return NStr::gc_Str<"Blob">;
		case ESqlColumnType::mc_Boolean: return NStr::gc_Str<"Boolean">;
		case ESqlColumnType::mc_Time: return NStr::gc_Str<"Time">;
		case ESqlColumnType::mc_UUID: return NStr::gc_Str<"UUID">;
		case ESqlColumnType::mc_Date: return NStr::gc_Str<"Date">;
		case ESqlColumnType::mc_TimeOfDay: return NStr::gc_Str<"TimeOfDay">;
		case ESqlColumnType::mc_Timestamp: return NStr::gc_Str<"Timestamp">;
		case ESqlColumnType::mc_TimestampTz: return NStr::gc_Str<"TimestampTz">;
		case ESqlColumnType::mc_Interval: return NStr::gc_Str<"Interval">;
		case ESqlColumnType::mc_Json: return NStr::gc_Str<"Json">;
		case ESqlColumnType::mc_Jsonb: return NStr::gc_Str<"Jsonb">;
		case ESqlColumnType::mc_Array_Integer16: return NStr::gc_Str<"Array_Integer16">;
		case ESqlColumnType::mc_Array_Integer32: return NStr::gc_Str<"Array_Integer32">;
		case ESqlColumnType::mc_Array_Integer64: return NStr::gc_Str<"Array_Integer64">;
		case ESqlColumnType::mc_Array_Float32: return NStr::gc_Str<"Array_Float32">;
		case ESqlColumnType::mc_Array_Float64: return NStr::gc_Str<"Array_Float64">;
		case ESqlColumnType::mc_Array_Text: return NStr::gc_Str<"Array_Text">;
		case ESqlColumnType::mc_Array_Boolean: return NStr::gc_Str<"Array_Boolean">;
		case ESqlColumnType::mc_Array_Bytes: return NStr::gc_Str<"Array_Bytes">;
		case ESqlColumnType::mc_Array_Date: return NStr::gc_Str<"Array_Date">;
		case ESqlColumnType::mc_Array_TimeOfDay: return NStr::gc_Str<"Array_TimeOfDay">;
		case ESqlColumnType::mc_Array_Timestamp: return NStr::gc_Str<"Array_Timestamp">;
		case ESqlColumnType::mc_Array_TimestampTz: return NStr::gc_Str<"Array_TimestampTz">;
		case ESqlColumnType::mc_Array_UUID: return NStr::gc_Str<"Array_UUID">;
		case ESqlColumnType::mc_Array_Json: return NStr::gc_Str<"Array_Json">;
		case ESqlColumnType::mc_Array_Jsonb: return NStr::gc_Str<"Array_Jsonb">;
		case ESqlColumnType::mc_Array_Interval: return NStr::gc_Str<"Array_Interval">;
		}

		return NStr::gc_Str<"Unknown">;
	}

	NStr::CStr fg_SqlValueTypeName(ESqlValueType _Type)
	{
		switch (_Type)
		{
		case ESqlValueType::mc_Null: return NStr::gc_Str<"Null">;
		case ESqlValueType::mc_Integer8: return NStr::gc_Str<"Integer8">;
		case ESqlValueType::mc_Integer16: return NStr::gc_Str<"Integer16">;
		case ESqlValueType::mc_Integer32: return NStr::gc_Str<"Integer32">;
		case ESqlValueType::mc_Integer64: return NStr::gc_Str<"Integer64">;
		case ESqlValueType::mc_UnsignedInteger8: return NStr::gc_Str<"UnsignedInteger8">;
		case ESqlValueType::mc_UnsignedInteger16: return NStr::gc_Str<"UnsignedInteger16">;
		case ESqlValueType::mc_UnsignedInteger32: return NStr::gc_Str<"UnsignedInteger32">;
		case ESqlValueType::mc_UnsignedInteger64: return NStr::gc_Str<"UnsignedInteger64">;
		case ESqlValueType::mc_Float32: return NStr::gc_Str<"Float32">;
		case ESqlValueType::mc_Float64: return NStr::gc_Str<"Float64">;
		case ESqlValueType::mc_Text: return NStr::gc_Str<"Text">;
		case ESqlValueType::mc_Blob: return NStr::gc_Str<"Blob">;
		case ESqlValueType::mc_Boolean: return NStr::gc_Str<"Boolean">;
		case ESqlValueType::mc_Time: return NStr::gc_Str<"Time">;
		case ESqlValueType::mc_UUID: return NStr::gc_Str<"UUID">;
		case ESqlValueType::mc_Date: return NStr::gc_Str<"Date">;
		case ESqlValueType::mc_TimeOfDay: return NStr::gc_Str<"TimeOfDay">;
		case ESqlValueType::mc_Timestamp: return NStr::gc_Str<"Timestamp">;
		case ESqlValueType::mc_TimestampTz: return NStr::gc_Str<"TimestampTz">;
		case ESqlValueType::mc_Interval: return NStr::gc_Str<"Interval">;
		case ESqlValueType::mc_Json: return NStr::gc_Str<"Json">;
		case ESqlValueType::mc_Jsonb: return NStr::gc_Str<"Jsonb">;
		case ESqlValueType::mc_UnrecognizedBackend: return NStr::gc_Str<"UnrecognizedBackend">;
		case ESqlValueType::mc_Array_Integer16: return NStr::gc_Str<"Array_Integer16">;
		case ESqlValueType::mc_Array_Integer32: return NStr::gc_Str<"Array_Integer32">;
		case ESqlValueType::mc_Array_Integer64: return NStr::gc_Str<"Array_Integer64">;
		case ESqlValueType::mc_Array_Float32: return NStr::gc_Str<"Array_Float32">;
		case ESqlValueType::mc_Array_Float64: return NStr::gc_Str<"Array_Float64">;
		case ESqlValueType::mc_Array_Text: return NStr::gc_Str<"Array_Text">;
		case ESqlValueType::mc_Array_Boolean: return NStr::gc_Str<"Array_Boolean">;
		case ESqlValueType::mc_Array_Bytes: return NStr::gc_Str<"Array_Bytes">;
		case ESqlValueType::mc_Array_Date: return NStr::gc_Str<"Array_Date">;
		case ESqlValueType::mc_Array_TimeOfDay: return NStr::gc_Str<"Array_TimeOfDay">;
		case ESqlValueType::mc_Array_Timestamp: return NStr::gc_Str<"Array_Timestamp">;
		case ESqlValueType::mc_Array_TimestampTz: return NStr::gc_Str<"Array_TimestampTz">;
		case ESqlValueType::mc_Array_UUID: return NStr::gc_Str<"Array_UUID">;
		case ESqlValueType::mc_Array_Json: return NStr::gc_Str<"Array_Json">;
		case ESqlValueType::mc_Array_Jsonb: return NStr::gc_Str<"Array_Jsonb">;
		case ESqlValueType::mc_Array_Interval: return NStr::gc_Str<"Array_Interval">;
		}

		return NStr::gc_Str<"Unknown">;
	}

	bool CSqlDatabaseBackendCapabilities::f_SupportsColumnType(ESqlColumnType _Type) const
	{
		switch (_Type)
		{
		case ESqlColumnType::mc_Invalid:
			return false;

		case ESqlColumnType::mc_Integer8:
		case ESqlColumnType::mc_Integer16:
		case ESqlColumnType::mc_Integer32:
		case ESqlColumnType::mc_Integer64:
		case ESqlColumnType::mc_UnsignedInteger8:
		case ESqlColumnType::mc_UnsignedInteger16:
		case ESqlColumnType::mc_UnsignedInteger32:
		case ESqlColumnType::mc_UnsignedInteger64:
		case ESqlColumnType::mc_Float32:
		case ESqlColumnType::mc_Float64:
		case ESqlColumnType::mc_Text:
		case ESqlColumnType::mc_Blob:
		case ESqlColumnType::mc_Boolean:
		case ESqlColumnType::mc_Time:
			return true;

		case ESqlColumnType::mc_UUID:
			return m_bUUID;
		case ESqlColumnType::mc_Date:
			return m_bDate;
		case ESqlColumnType::mc_TimeOfDay:
			return m_bTimeOfDay;
		case ESqlColumnType::mc_Timestamp:
			return m_bTimestamp;
		case ESqlColumnType::mc_TimestampTz:
			return m_bTimestampTz;
		case ESqlColumnType::mc_Interval:
			return m_bInterval;
		case ESqlColumnType::mc_Json:
			return m_bJSON;
		case ESqlColumnType::mc_Jsonb:
			return m_bJSONB;

		case ESqlColumnType::mc_Array_Integer16:
		case ESqlColumnType::mc_Array_Integer32:
		case ESqlColumnType::mc_Array_Integer64:
		case ESqlColumnType::mc_Array_Float32:
		case ESqlColumnType::mc_Array_Float64:
		case ESqlColumnType::mc_Array_Text:
		case ESqlColumnType::mc_Array_Boolean:
		case ESqlColumnType::mc_Array_Bytes:
			return m_bArrays;

		case ESqlColumnType::mc_Array_Date:
			return m_bArrays && m_bDate;
		case ESqlColumnType::mc_Array_TimeOfDay:
			return m_bArrays && m_bTimeOfDay;
		case ESqlColumnType::mc_Array_Timestamp:
			return m_bArrays && m_bTimestamp;
		case ESqlColumnType::mc_Array_TimestampTz:
			return m_bArrays && m_bTimestampTz;
		case ESqlColumnType::mc_Array_UUID:
			return m_bArrays && m_bUUID;
		case ESqlColumnType::mc_Array_Json:
			return m_bArrays && m_bJSON;
		case ESqlColumnType::mc_Array_Jsonb:
			return m_bArrays && m_bJSONB;
		case ESqlColumnType::mc_Array_Interval:
			return m_bArrays && m_bInterval;
		}

		return false;
	}

	bool CSqlDatabaseBackendCapabilities::f_SupportsValueType(ESqlValueType _Type) const
	{
		switch (_Type)
		{
		case ESqlValueType::mc_Null:
		case ESqlValueType::mc_Integer8:
		case ESqlValueType::mc_Integer16:
		case ESqlValueType::mc_Integer32:
		case ESqlValueType::mc_Integer64:
		case ESqlValueType::mc_UnsignedInteger8:
		case ESqlValueType::mc_UnsignedInteger16:
		case ESqlValueType::mc_UnsignedInteger32:
		case ESqlValueType::mc_UnsignedInteger64:
		case ESqlValueType::mc_Float32:
		case ESqlValueType::mc_Float64:
		case ESqlValueType::mc_Text:
		case ESqlValueType::mc_Blob:
		case ESqlValueType::mc_Boolean:
		case ESqlValueType::mc_Time:
			return true;

		case ESqlValueType::mc_UUID:
			return m_bUUID;
		case ESqlValueType::mc_Date:
			return m_bDate;
		case ESqlValueType::mc_TimeOfDay:
			return m_bTimeOfDay;
		case ESqlValueType::mc_Timestamp:
			return m_bTimestamp;
		case ESqlValueType::mc_TimestampTz:
			return m_bTimestampTz;
		case ESqlValueType::mc_Interval:
			return m_bInterval;
		case ESqlValueType::mc_Json:
			return m_bJSON;
		case ESqlValueType::mc_Jsonb:
			return m_bJSONB;
		case ESqlValueType::mc_UnrecognizedBackend:
			return m_bUnrecognizedBackend;

		case ESqlValueType::mc_Array_Integer16:
		case ESqlValueType::mc_Array_Integer32:
		case ESqlValueType::mc_Array_Integer64:
		case ESqlValueType::mc_Array_Float32:
		case ESqlValueType::mc_Array_Float64:
		case ESqlValueType::mc_Array_Text:
		case ESqlValueType::mc_Array_Boolean:
		case ESqlValueType::mc_Array_Bytes:
			return m_bArrays;

		case ESqlValueType::mc_Array_Date:
			return m_bArrays && m_bDate;
		case ESqlValueType::mc_Array_TimeOfDay:
			return m_bArrays && m_bTimeOfDay;
		case ESqlValueType::mc_Array_Timestamp:
			return m_bArrays && m_bTimestamp;
		case ESqlValueType::mc_Array_TimestampTz:
			return m_bArrays && m_bTimestampTz;
		case ESqlValueType::mc_Array_UUID:
			return m_bArrays && m_bUUID;
		case ESqlValueType::mc_Array_Json:
			return m_bArrays && m_bJSON;
		case ESqlValueType::mc_Array_Jsonb:
			return m_bArrays && m_bJSONB;
		case ESqlValueType::mc_Array_Interval:
			return m_bArrays && m_bInterval;
		}

		return false;
	}

	CSqlDatabaseClient::CSqlDatabaseClient(NConcurrency::TCActor<ICSqlDatabaseBackendActor> _Backend)
		: mp_Backend(fg_Move(_Backend))
	{
	}

	NConcurrency::TCFuture<void> CSqlDatabaseClient::f_Open()
	{
		return mp_Backend(&ICSqlDatabaseBackendActor::f_Open);
	}

	NConcurrency::TCFuture<void> CSqlDatabaseClient::f_ApplySchema()
	{
		return mp_Backend(&ICSqlDatabaseBackendActor::f_ApplySchema);
	}

	NConcurrency::TCFuture<CSqlDatabaseClient::CTransaction> CSqlDatabaseClient::f_BeginTransaction(CSqlTransactionSettings _Settings)
	{
		return fsp_BeginTransaction(mp_Backend, fg_Move(_Settings), m_nPipelineLength);
	}

	NConcurrency::TCFuture<CSqlDatabaseClient::CTransaction> CSqlDatabaseClient::f_BeginReadTransaction(CSqlTransactionSettings _Settings)
	{
		return fsp_BeginReadTransaction(mp_Backend, fg_Move(_Settings), m_nPipelineLength);
	}

	auto CSqlDatabaseClient::fsp_BeginTransaction(NConcurrency::TCActor<ICSqlDatabaseBackendActor> _Backend, CSqlTransactionSettings _Settings, uint32 _nPipelineLength)
		-> NConcurrency::TCFuture<CSqlDatabaseClient::CTransaction>
	{
		CTransaction Transaction(co_await _Backend(&ICSqlDatabaseBackendActor::f_BeginTransaction, fg_Move(_Settings)));
		Transaction.m_nPipelineLength = _nPipelineLength;
		co_return Transaction;
	}

	auto CSqlDatabaseClient::fsp_BeginReadTransaction(NConcurrency::TCActor<ICSqlDatabaseBackendActor> _Backend, CSqlTransactionSettings _Settings, uint32 _nPipelineLength)
		-> NConcurrency::TCFuture<CSqlDatabaseClient::CTransaction>
	{
		CTransaction Transaction(co_await _Backend(&ICSqlDatabaseBackendActor::f_BeginReadTransaction, fg_Move(_Settings)));
		Transaction.m_nPipelineLength = _nPipelineLength;
		co_return Transaction;
	}

	CSqlTransaction::CSqlTransaction(CSqlTransactionInterface _Transaction)
		: mp_Transaction(fg_Move(_Transaction))
	{
	}

	NConcurrency::TCFuture<void> CSqlTransaction::f_Commit()
	{
		return mp_Transaction(&ICSqlTransactionActor::f_CommitTransaction);
	}

	NConcurrency::TCFuture<void> CSqlTransaction::f_Rollback()
	{
		return mp_Transaction(&ICSqlTransactionActor::f_RollbackTransaction);
	}

	NConcurrency::TCFuture<NStr::CStr> CSqlTransaction::f_CreateSavepoint()
	{
		return mp_Transaction(&ICSqlTransactionActor::f_CreateSavepoint);
	}

	NConcurrency::TCFuture<void> CSqlTransaction::f_ReleaseSavepoint(NStr::CStr _Name)
	{
		return mp_Transaction(&ICSqlTransactionActor::f_ReleaseSavepoint, fg_Move(_Name));
	}

	NConcurrency::TCFuture<void> CSqlTransaction::f_RollbackToSavepoint(NStr::CStr _Name)
	{
		return mp_Transaction(&ICSqlTransactionActor::f_RollbackToSavepoint, fg_Move(_Name));
	}

	CSqlTransaction CSqlTransaction::fp_CopyHandleForScope() const
	{
		CSqlTransaction Transaction(CSqlTransactionInterface(NConcurrency::TCActor<ICSqlTransactionActor>(mp_Transaction.f_GetActor())));
		Transaction.m_nPipelineLength = m_nPipelineLength;
		return Transaction;
	}

	CSqlRawOperation fg_SqlRaw(NStr::CStr _Sql, NContainer::TCVector<CSqlValue> _Parameters)
	{
		return
			{
				.m_Sql = fg_Move(_Sql)
				, .m_Parameters = fg_Move(_Parameters)
				, .m_BackendRequirement = ESqlRawBackend::mc_Any
			}
		;
	}

	CSqlRawOperation fg_SqlPostgresRaw(NStr::CStr _Sql, NContainer::TCVector<CSqlValue> _Parameters)
	{
		return
			{
				.m_Sql = fg_Move(_Sql)
				, .m_Parameters = fg_Move(_Parameters)
				, .m_BackendRequirement = ESqlRawBackend::mc_Postgres
			}
		;
	}

	CSqlRawOperation fg_SqlSqliteRaw(NStr::CStr _Sql, NContainer::TCVector<CSqlValue> _Parameters)
	{
		return
			{
				.m_Sql = fg_Move(_Sql)
				, .m_Parameters = fg_Move(_Parameters)
				, .m_BackendRequirement = ESqlRawBackend::mc_SQLite
			}
		;
	}

	NConcurrency::TCFuture<umint> CSqlDatabaseClient::f_ExecuteRaw(CSqlRawOperation _Operation)
	{
		return mp_Backend(&ICSqlDatabaseBackendActor::f_ExecuteRaw, fg_Move(_Operation));
	}

	NConcurrency::TCFuture<umint> CSqlDatabaseClient::f_ExecuteRaw(NStr::CStr _Sql, NContainer::TCVector<CSqlValue> _Parameters)
	{
		return f_ExecuteRaw(fg_SqlRaw(fg_Move(_Sql), fg_Move(_Parameters)));
	}

	NConcurrency::TCFuture<CSqlRawResult> CSqlDatabaseClient::f_QueryRaw(CSqlRawOperation _Operation)
	{
		return mp_Backend(&ICSqlDatabaseBackendActor::f_QueryRaw, fg_Move(_Operation));
	}

	NConcurrency::TCFuture<CSqlRawResult> CSqlDatabaseClient::f_QueryRaw(NStr::CStr _Sql, NContainer::TCVector<CSqlValue> _Parameters)
	{
		return f_QueryRaw(fg_SqlRaw(fg_Move(_Sql), fg_Move(_Parameters)));
	}

	NConcurrency::TCFuture<CSqlRawStream> CSqlDatabaseClient::f_QueryRawStream(CSqlRawOperation _Operation)
	{
		return mp_Backend(&ICSqlDatabaseBackendActor::f_QueryRawStream, fg_Move(_Operation));
	}

	NConcurrency::TCFuture<CSqlRawStream> CSqlDatabaseClient::f_QueryRawStream(NStr::CStr _Sql, NContainer::TCVector<CSqlValue> _Parameters)
	{
		return f_QueryRawStream(fg_SqlRaw(fg_Move(_Sql), fg_Move(_Parameters)));
	}

	NConcurrency::TCFuture<umint> CSqlTransaction::f_ExecuteRaw(CSqlRawOperation _Operation)
	{
		return mp_Transaction(&ICSqlTransactionActor::f_ExecuteRaw, fg_Move(_Operation));
	}

	NConcurrency::TCFuture<umint> CSqlTransaction::f_ExecuteRaw(NStr::CStr _Sql, NContainer::TCVector<CSqlValue> _Parameters)
	{
		return f_ExecuteRaw(fg_SqlRaw(fg_Move(_Sql), fg_Move(_Parameters)));
	}

	NConcurrency::TCFuture<CSqlRawResult> CSqlTransaction::f_QueryRaw(CSqlRawOperation _Operation)
	{
		return mp_Transaction(&ICSqlTransactionActor::f_QueryRaw, fg_Move(_Operation));
	}

	NConcurrency::TCFuture<CSqlRawResult> CSqlTransaction::f_QueryRaw(NStr::CStr _Sql, NContainer::TCVector<CSqlValue> _Parameters)
	{
		return f_QueryRaw(fg_SqlRaw(fg_Move(_Sql), fg_Move(_Parameters)));
	}

	NConcurrency::TCFuture<CSqlRawStream> CSqlTransaction::f_QueryRawStream(CSqlRawOperation _Operation)
	{
		return mp_Transaction(&ICSqlTransactionActor::f_QueryRawStream, fg_Move(_Operation));
	}

	NConcurrency::TCFuture<CSqlRawStream> CSqlTransaction::f_QueryRawStream(NStr::CStr _Sql, NContainer::TCVector<CSqlValue> _Parameters)
	{
		return f_QueryRawStream(fg_SqlRaw(fg_Move(_Sql), fg_Move(_Parameters)));
	}
}
