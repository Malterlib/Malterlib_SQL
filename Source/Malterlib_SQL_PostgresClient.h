// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#pragma once

#include <Mib/SQL/SQL>
#include <Mib/Concurrency/AsyncGenerator>
#include <Mib/Concurrency/ConcurrencyManager>
#include <Mib/Cryptography/UUID>
#include <Mib/Encoding/Json>
#include <Mib/Network/SSL>
#include <Mib/Stream/ByteVector>
#include <Mib/Storage/Optional>
#include <Mib/Storage/UniquePointer>
#include <Mib/Storage/Variant>
#include <Mib/Time/Time>

namespace NMib::NSQL
{
	struct CPostgresDataRow;
	struct CPostgresQueryResult;
	struct CPostgresRowStreamItem;
	constexpr umint gc_nPostgresDefaultRowsPerBatch = 128;

	enum class EPostgresValueType : uint32
	{
		mc_Null = 0
		, mc_Integer16 = 21
		, mc_Integer32 = 23
		, mc_Integer64 = 20
		, mc_Float32 = 700
		, mc_Float64 = 701
		, mc_Text = 25
		, mc_Varchar = 1043
		, mc_Boolean = 16
		, mc_Bytes = 17
		, mc_Unrecognized = 0x7FFFFFFF
		, mc_Date = 1082
		, mc_Time = 1083
		, mc_Timestamp = 1114
		, mc_TimestampTz = 1184
		, mc_UUID = 2950
		, mc_Json = 114
		, mc_Jsonb = 3802
		, mc_Interval = 1186
		, mc_Array_Integer16 = 1005
		, mc_Array_Integer32 = 1007
		, mc_Array_Integer64 = 1016
		, mc_Array_Float32 = 1021
		, mc_Array_Float64 = 1022
		, mc_Array_Text = 1009
		, mc_Array_Varchar = 1015
		, mc_Array_Boolean = 1000
		, mc_Array_Bytes = 1001
		, mc_Array_Date = 1182
		, mc_Array_Time = 1183
		, mc_Array_Timestamp = 1115
		, mc_Array_TimestampTz = 1185
		, mc_Array_UUID = 2951
		, mc_Array_Json = 199
		, mc_Array_Jsonb = 3807
		, mc_Array_Interval = 1187
	};

	struct CPostgresArrayDimension
	{
		constexpr auto operator <=> (CPostgresArrayDimension const &_Other) const noexcept = default;

		int32 m_Length = 0;
		int32 m_LowerBound = 1;
	};

	template <typename t_CValue>
	struct TCPostgresArray
	{
		constexpr auto operator <=> (TCPostgresArray const &_Other) const noexcept = default;

		NContainer::TCVector<CPostgresArrayDimension> m_Dimensions;
		NContainer::TCVector<NStorage::TCOptional<t_CValue>> m_Values;
	};

	struct CPostgresDate
	{
		constexpr auto operator <=> (CPostgresDate const &_Other) const noexcept = default;

		NTime::CTime m_Time;
	};

	struct CPostgresTime
	{
		constexpr auto operator <=> (CPostgresTime const &_Other) const noexcept = default;

		NTime::CTime m_Time;
	};

	struct CPostgresTimestamp
	{
		constexpr auto operator <=> (CPostgresTimestamp const &_Other) const noexcept = default;

		NTime::CTime m_Time;
	};

	struct CPostgresTimestampTz
	{
		constexpr auto operator <=> (CPostgresTimestampTz const &_Other) const noexcept = default;

		NTime::CTime m_Time;
	};

	struct CPostgresInterval
	{
		constexpr auto operator <=> (CPostgresInterval const &_Other) const noexcept = default;

		int32 m_Months = 0;
		int32 m_Days = 0;
		NTime::CTimeSpan m_Time;
	};

	struct CPostgresUnrecognizedValue
	{
		constexpr auto operator <=> (CPostgresUnrecognizedValue const &_Other) const noexcept = default;

		uint32 m_TypeOID = 0;
		NContainer::CIOByteVector m_Bytes;
	};

	using CPostgresValue = NStorage::TCStreamableVariant
		<
			EPostgresValueType
			, NStorage::TCMember<void, EPostgresValueType::mc_Null>
			, NStorage::TCMember<int16, EPostgresValueType::mc_Integer16>
			, NStorage::TCMember<int32, EPostgresValueType::mc_Integer32>
			, NStorage::TCMember<int64, EPostgresValueType::mc_Integer64>
			, NStorage::TCMember<fp32, EPostgresValueType::mc_Float32>
			, NStorage::TCMember<fp64, EPostgresValueType::mc_Float64>
			, NStorage::TCMember<NStr::CStr, EPostgresValueType::mc_Text>
			, NStorage::TCMember<bool, EPostgresValueType::mc_Boolean>
			, NStorage::TCMember<NContainer::CIOByteVector, EPostgresValueType::mc_Bytes>
			, NStorage::TCMember<CPostgresUnrecognizedValue, EPostgresValueType::mc_Unrecognized>
			, NStorage::TCMember<CPostgresDate, EPostgresValueType::mc_Date>
			, NStorage::TCMember<CPostgresTime, EPostgresValueType::mc_Time>
			, NStorage::TCMember<CPostgresTimestamp, EPostgresValueType::mc_Timestamp>
			, NStorage::TCMember<CPostgresTimestampTz, EPostgresValueType::mc_TimestampTz>
			, NStorage::TCMember<NCryptography::CUniversallyUniqueIdentifier, EPostgresValueType::mc_UUID>
			, NStorage::TCMember<NEncoding::CJsonOrdered, EPostgresValueType::mc_Json>
			, NStorage::TCMember<NEncoding::CJsonSorted, EPostgresValueType::mc_Jsonb>
			, NStorage::TCMember<CPostgresInterval, EPostgresValueType::mc_Interval>
			, NStorage::TCMember<TCPostgresArray<int16>, EPostgresValueType::mc_Array_Integer16>
			, NStorage::TCMember<TCPostgresArray<int32>, EPostgresValueType::mc_Array_Integer32>
			, NStorage::TCMember<TCPostgresArray<int64>, EPostgresValueType::mc_Array_Integer64>
			, NStorage::TCMember<TCPostgresArray<fp32>, EPostgresValueType::mc_Array_Float32>
			, NStorage::TCMember<TCPostgresArray<fp64>, EPostgresValueType::mc_Array_Float64>
			, NStorage::TCMember<TCPostgresArray<NStr::CStr>, EPostgresValueType::mc_Array_Text>
			, NStorage::TCMember<TCPostgresArray<bool>, EPostgresValueType::mc_Array_Boolean>
			, NStorage::TCMember<TCPostgresArray<NContainer::CIOByteVector>, EPostgresValueType::mc_Array_Bytes>
			, NStorage::TCMember<TCPostgresArray<CPostgresDate>, EPostgresValueType::mc_Array_Date>
			, NStorage::TCMember<TCPostgresArray<CPostgresTime>, EPostgresValueType::mc_Array_Time>
			, NStorage::TCMember<TCPostgresArray<CPostgresTimestamp>, EPostgresValueType::mc_Array_Timestamp>
			, NStorage::TCMember<TCPostgresArray<CPostgresTimestampTz>, EPostgresValueType::mc_Array_TimestampTz>
			, NStorage::TCMember<TCPostgresArray<NCryptography::CUniversallyUniqueIdentifier>, EPostgresValueType::mc_Array_UUID>
			, NStorage::TCMember<TCPostgresArray<NEncoding::CJsonOrdered>, EPostgresValueType::mc_Array_Json>
			, NStorage::TCMember<TCPostgresArray<NEncoding::CJsonSorted>, EPostgresValueType::mc_Array_Jsonb>
			, NStorage::TCMember<TCPostgresArray<CPostgresInterval>, EPostgresValueType::mc_Array_Interval>
		>
	;

	struct CPostgresDataRow
	{
		NContainer::TCVector<CPostgresValue> m_Values;
	};

	using CPostgresDataRowBatch = NContainer::TCVector<CPostgresDataRow>;

	struct CPostgresConnectionSettings
	{
		NStr::CStr m_Host;
		uint16 m_Port = 5432;
		NStr::CStr m_Database;
		NStr::CStr m_User;
		NStr::CStrSecure m_Password;
		NStr::CStr m_ApplicationName = "Malterlib";
		NNetwork::CSSLSettings m_TLSSettings;
		fp64 m_SocketTimeout = 60.0;
		fp64 m_KeepAliveInterval = 30.0;
		uint32 m_nPipelineLength = 5;
		bool m_bRequireTLS = true;
		bool m_bVerifyTLS = true;
	};

	enum class EPostgresReadyForQueryStatus : uint8
	{
		mc_Idle = 'I'
		, mc_InTransaction = 'T'
		, mc_FailedTransaction = 'E'
	};

	struct CPostgresCommandComplete
	{
		NStr::CStr m_Tag;
	};

	struct CPostgresErrorResponse
	{
		NStr::CStr m_Severity;
		NStr::CStr m_Code;
		NStr::CStr m_Message;
		NStr::CStr m_Detail;
		NStr::CStr m_Hint;
	};

	CSqlErrorData fg_PostgresSqlErrorData(CPostgresErrorResponse const &_Error);
	CExceptionSql fg_PostgresSqlError(NStr::CStr const &_Message, CPostgresErrorResponse const &_Error);

	struct CPostgresFieldDescription
	{
		NStr::CStr m_Name;
		uint32 m_TableOID = 0;
		uint16 m_ColumnAttributeNumber = 0;
		uint32 m_TypeOID = 0;
		uint16 m_TypeSize = 0;
		uint32 m_TypeModifier = 0;
		uint16 m_Format = 0;
	};

	struct CPostgresRowDescription
	{
		NContainer::TCVector<CPostgresFieldDescription> m_Fields;
	};

	struct CPostgresQueryResult
	{
		NStorage::TCOptional<CPostgresRowDescription> m_RowDescription;
		NContainer::TCVector<CPostgresDataRow> m_Rows;
		NStorage::TCOptional<CPostgresCommandComplete> m_CommandComplete;
		EPostgresReadyForQueryStatus m_ReadyStatus = EPostgresReadyForQueryStatus::mc_Idle;
	};

	struct CPostgresRowStreamItem
	{
		NStorage::TCOptional<CPostgresRowDescription> m_Description;
		CPostgresDataRowBatch m_Rows;
	};

	struct CPostgresClientActor : public NConcurrency::CActor
	{
		CPostgresClientActor();
		~CPostgresClientActor();

		NConcurrency::TCFuture<void> f_Connect(CPostgresConnectionSettings _Settings);
		NConcurrency::TCFuture<void> f_Close();
		NConcurrency::TCFuture<CPostgresQueryResult> f_Execute(NStr::CStr _Sql);
		NConcurrency::TCFuture<CPostgresQueryResult> f_ExecuteWithParameters(NStr::CStr _Sql, NContainer::TCVector<CPostgresValue> _Parameters);
		auto f_ExecuteRows(NStr::CStr _Sql, NContainer::TCVector<CPostgresValue> _Parameters = {}, umint _nRowsPerBatch = gc_nPostgresDefaultRowsPerBatch)
			-> NConcurrency::TCAsyncGenerator<CPostgresDataRowBatch>
		;
		auto f_ExecuteRowsStream(NStr::CStr _Sql, NContainer::TCVector<CPostgresValue> _Parameters = {}, umint _nRowsPerBatch = gc_nPostgresDefaultRowsPerBatch)
			-> NConcurrency::TCAsyncGenerator<CPostgresRowStreamItem>
		;
		NConcurrency::TCFuture<void> f_PrepareStatement(NStr::CStr _Name, NStr::CStr _Sql, NContainer::TCVector<EPostgresValueType> _ParameterTypes = {});
		NConcurrency::TCFuture<CPostgresQueryResult> f_ExecutePrepared(NStr::CStr _Name, NContainer::TCVector<CPostgresValue> _Parameters = {});
		auto f_ExecutePreparedBulk
			(
				NStr::CStr _Name
				, NConcurrency::TCAsyncGenerator<NContainer::TCVector<NContainer::TCVector<CPostgresValue>>> _ParameterBatches
			)
			-> NConcurrency::TCFuture<umint>
		;
		auto f_ExecutePreparedRows(NStr::CStr _Name, NContainer::TCVector<CPostgresValue> _Parameters = {}, umint _nRowsPerBatch = gc_nPostgresDefaultRowsPerBatch)
			-> NConcurrency::TCAsyncGenerator<CPostgresDataRowBatch>
		;
		NConcurrency::TCFuture<void> f_DeallocatePrepared(NStr::CStr _Name);
		NConcurrency::TCFuture<void> f_BeginTransaction(bool _bReadOnly = false, CSqlTransactionSettings _Settings = {});
		NConcurrency::TCFuture<void> f_CommitTransaction();
		NConcurrency::TCFuture<void> f_RollbackTransaction();
		// True when the last decoded ReadyForQuery left the session inside a transaction (or a failed transaction).
		// Lets a caller decide whether a connection is safe to return to the pool after an operation whose result does
		// not itself carry the ready status (e.g. a drained row stream).
		NConcurrency::TCFuture<bool> f_IsInTransaction();

	private:
		struct CInternal;

		struct CBackendMessageRef
		{
			uint8 m_Type;
			umint m_PayloadOffset;
			umint m_PayloadLength;
			umint m_TotalLength;
		};

		// One decoded backend message from a pipelined bulk execution. Returned by value so the reader never holds a
		// reference into the receive buffer across a suspension (a reference-capturing coroutine here is unsafe).
		struct CBulkResponse
		{
			uint8 m_Type = 0;
			umint m_AffectedRows = 0;
			bool m_bAffectedRowsValid = false;
			NStorage::TCOptional<CPostgresErrorResponse> m_ErrorResponse;
			NException::CExceptionPointer m_ProtocolError;
		};

		NConcurrency::TCFuture<void> fp_Destroy() override;
		NConcurrency::TCFuture<void> fp_OnSocketData(NStorage::TCSharedPointer<NContainer::CIOByteVector> _pData);
		NConcurrency::TCFuture<void> fp_OnSocketClose();
		NConcurrency::TCFuture<void> fp_KeepAlive();
		NConcurrency::TCFuture<void> fp_Send(NContainer::CIOByteVector _Data);
		NConcurrency::TCFuture<void> fp_WaitForReceiveBytes(umint _nBytes);
		NConcurrency::TCFuture<NContainer::CIOByteVector> fp_ReadBytes(umint _nBytes);
		NConcurrency::TCFuture<CBackendMessageRef> fp_ReadBackendMessageRef();
		void fp_ConsumeBackendMessage(CBackendMessageRef const &_Ref);
		NConcurrency::TCFuture<CBulkResponse> fp_DecodeBulkResponse(CBackendMessageRef _Ref);
		NConcurrency::TCFuture<CBulkResponse> fp_ReadBulkResponse();
		// Non-blocking variant: decode and return one response only if a complete backend message is already buffered;
		// otherwise return an empty optional without suspending. Lets the bulk loop drain whatever has already arrived
		// before falling back to the blocking read when the in-flight window is full.
		NConcurrency::TCFuture<NStorage::TCOptional<CBulkResponse>> fp_TryReadBulkResponse();

		NStorage::TCUniquePointer<CInternal> mp_pInternal;
	};

	NConcurrency::TCActor<CPostgresClientActor> fg_CreatePostgresClient();
}

#ifndef DMibPNoShortCuts
	using namespace NMib::NSQL;
#endif
