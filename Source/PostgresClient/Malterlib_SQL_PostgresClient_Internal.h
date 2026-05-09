// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#pragma once

#include "../Malterlib_SQL_PostgresClientProtocol.h"

#include <Mib/Container/PagedByteVector>

namespace NMib::NSQL
{
	using CPostgresReadStream = NContainer::TCBinaryStreamPagedByteVectorPtr<NStream::CBinaryStreamBigEndian>;

	uint32 fg_PostgresGetValueTypeOID(EPostgresValueType _Type);
	NContainer::TCVector<uint32> fg_PostgresBuildParameterTypeOIDs(NContainer::TCVector<CPostgresValue> const &_Parameters);

	void fg_PostgresWriteSSLRequest(CPostgresWriteStream &_Stream);
	void fg_PostgresWriteStartupMessage(CPostgresWriteStream &_Stream, CPostgresConnectionSettings const &_Settings);
	void fg_PostgresWriteFrontendMessage(CPostgresWriteStream &_Stream, uint8 _Type, NContainer::CIOByteVector const &_Payload);
	void fg_PostgresWriteSASLInitialResponse(CPostgresWriteStream &_Stream, NStr::CStr const &_Mechanism, NStr::CStr const &_InitialResponse);
	void fg_PostgresWriteSASLResponse(CPostgresWriteStream &_Stream, NStr::CStr const &_Response);
	void fg_PostgresWriteParse(CPostgresWriteStream &_Stream, NStr::CStr const &_StatementName, NStr::CStr const &_Sql, NContainer::TCVector<uint32> const &_ParameterTypeOIDs = {});
	NConcurrency::TCWrapped<void> fg_PostgresWriteBind
		(
			CPostgresWriteStream &_Stream
			, NStr::CStr const &_PortalName
			, NStr::CStr const &_StatementName
			, NContainer::TCVector<CPostgresValue> const &_Parameters
			, NContainer::TCVector<uint16> const &_ResultFormats
		)
	;
	void fg_PostgresWriteDescribe(CPostgresWriteStream &_Stream, EPostgresDescribeTarget _Target, NStr::CStr const &_Name);
	void fg_PostgresWriteExecute(CPostgresWriteStream &_Stream, NStr::CStr const &_PortalName, uint32 _MaxRows = 0);
	void fg_PostgresWriteClose(CPostgresWriteStream &_Stream, EPostgresDescribeTarget _Target, NStr::CStr const &_Name);
	void fg_PostgresWriteSync(CPostgresWriteStream &_Stream);
	void fg_PostgresWriteFlush(CPostgresWriteStream &_Stream);
	void fg_PostgresWriteTerminate(CPostgresWriteStream &_Stream);

	// Internal decoders: caller has already extracted the message type byte and opened the
	// stream over the message's payload. They consume the payload bytes from the stream.
	NConcurrency::TCWrapped<CPostgresAuthenticationRequest> fg_PostgresDecodeAuthenticationRequest(CPostgresReadStream &_Stream);
	NConcurrency::TCWrapped<CPostgresParameterStatus> fg_PostgresDecodeParameterStatus(CPostgresReadStream &_Stream);
	NConcurrency::TCWrapped<CPostgresBackendKeyData> fg_PostgresDecodeBackendKeyData(CPostgresReadStream &_Stream);
	NConcurrency::TCWrapped<EPostgresReadyForQueryStatus> fg_PostgresDecodeReadyForQuery(CPostgresReadStream &_Stream);
	NConcurrency::TCWrapped<void> fg_PostgresDecodeParseComplete(CPostgresReadStream &_Stream);
	NConcurrency::TCWrapped<void> fg_PostgresDecodeBindComplete(CPostgresReadStream &_Stream);
	NConcurrency::TCWrapped<void> fg_PostgresDecodeCloseComplete(CPostgresReadStream &_Stream);
	NConcurrency::TCWrapped<void> fg_PostgresDecodeNoData(CPostgresReadStream &_Stream);
	NConcurrency::TCWrapped<CPostgresCommandComplete> fg_PostgresDecodeCommandComplete(CPostgresReadStream &_Stream);
	NConcurrency::TCWrapped<CPostgresErrorResponse> fg_PostgresDecodeErrorResponse(CPostgresReadStream &_Stream);
	NConcurrency::TCWrapped<CPostgresRowDescription> fg_PostgresDecodeRowDescription(CPostgresReadStream &_Stream);
	NConcurrency::TCWrapped<CPostgresDataRow> fg_PostgresDecodeDataRow(CPostgresReadStream &_Stream, CPostgresRowDescription const &_Description);
}
