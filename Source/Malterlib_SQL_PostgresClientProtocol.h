// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#pragma once

#include "Malterlib_SQL_PostgresClient.h"

namespace NMib::NSQL
{
	using CPostgresWriteStream = NStream::CBinaryStreamMemory<NStream::CBinaryStreamBigEndian, NContainer::CIOByteVector>;

	struct CPostgresBackendMessage
	{
		uint8 m_Type = 0;
		NContainer::CIOByteVector m_Payload;
	};

	enum class EPostgresAuthenticationRequestType : uint32
	{
		mc_OK = 0
		, mc_KerberosV5 = 2
		, mc_CleartextPassword = 3
		, mc_MD5Password = 5
		, mc_SCMCredential = 6
		, mc_GSS = 7
		, mc_GSSContinue = 8
		, mc_SSPI = 9
		, mc_SASL = 10
		, mc_SASLContinue = 11
		, mc_SASLFinal = 12
	};

	struct CPostgresAuthenticationRequest
	{
		EPostgresAuthenticationRequestType m_Type = EPostgresAuthenticationRequestType::mc_OK;
		NContainer::TCVector<NStr::CStr> m_SASLMechanisms;
		NContainer::CIOByteVector m_SASLData;
	};

	struct CPostgresParameterStatus
	{
		NStr::CStr m_Name;
		NStr::CStr m_Value;
	};

	struct CPostgresBackendKeyData
	{
		uint32 m_ProcessID = 0;
		uint32 m_SecretKey = 0;
	};

	struct CPostgresScramClientFirstMessage
	{
		NStr::CStr m_ClientNonce;
		NStr::CStr m_GS2Header;
		NStr::CStr m_BareMessage;
		NStr::CStr m_Message;
	};

	struct CPostgresScramServerFirstMessage
	{
		NStr::CStr m_Nonce;
		NContainer::CByteVector m_Salt;
		uint32 m_Iterations = 0;
	};

	struct CPostgresScramClientFinalMessage
	{
		NStr::CStr m_WithoutProof;
		NStr::CStr m_AuthMessage;
		NStr::CStr m_Message;
		NContainer::CByteVector m_ServerSignature;
	};

	struct CPostgresScramServerFinalMessage
	{
		NContainer::CByteVector m_ServerSignature;
	};

	enum class EPostgresDescribeTarget : uint8
	{
		mc_Statement
		, mc_Portal
	};

	NContainer::CIOByteVector fg_PostgresBuildSSLRequest();
	NContainer::CIOByteVector fg_PostgresBuildStartupMessage(CPostgresConnectionSettings const &_Settings);
	NContainer::CIOByteVector fg_PostgresBuildFrontendMessage(uint8 _Type, NContainer::CIOByteVector const &_Payload);
	NContainer::CIOByteVector fg_PostgresBuildSASLInitialResponse(NStr::CStr const &_Mechanism, NStr::CStr const &_InitialResponse);
	NContainer::CIOByteVector fg_PostgresBuildSASLResponse(NStr::CStr const &_Response);
	NContainer::CIOByteVector fg_PostgresBuildParse(NStr::CStr const &_StatementName, NStr::CStr const &_Sql, NContainer::TCVector<uint32> const &_ParameterTypeOIDs = {});
	NConcurrency::TCWrapped<NContainer::CIOByteVector> fg_PostgresBuildBind
		(
			NStr::CStr const &_PortalName
			, NStr::CStr const &_StatementName
			, NContainer::TCVector<CPostgresValue> const &_Parameters
			, NContainer::TCVector<uint16> const &_ResultFormats
		)
	;
	NContainer::CIOByteVector fg_PostgresBuildDescribe(EPostgresDescribeTarget _Target, NStr::CStr const &_Name);
	NContainer::CIOByteVector fg_PostgresBuildExecute(NStr::CStr const &_PortalName, uint32 _MaxRows = 0);
	NContainer::CIOByteVector fg_PostgresBuildSync();
	NContainer::CIOByteVector fg_PostgresBuildTerminate();
	CPostgresScramClientFirstMessage fg_PostgresScramBuildClientFirstMessage
		(
			NStr::CStr const &_User
			, NStr::CStr _ClientNonce = NStr::CStr()
			, NStr::CStr const &_GS2Header = "n,,"
		)
	;
	NConcurrency::TCWrapped<CPostgresScramServerFirstMessage> fg_PostgresScramParseServerFirstMessage(NStr::CStr const &_Message, NStr::CStr const &_ClientNonce);
	auto fg_PostgresScramBuildClientFinalMessage
		(
			NStr::CStrSecure const &_Password
			, CPostgresScramClientFirstMessage const &_ClientFirst
			, CPostgresScramServerFirstMessage const &_ServerFirst
			, NContainer::CByteVector const &_ChannelBindingData = {}
		)
		-> CPostgresScramClientFinalMessage
	;
	NConcurrency::TCWrapped<CPostgresScramServerFinalMessage> fg_PostgresScramParseServerFinalMessage(NStr::CStr const &_Message);
	bool fg_PostgresScramVerifyServerFinalMessage(CPostgresScramClientFinalMessage const &_ClientFinal, CPostgresScramServerFinalMessage const &_ServerFinal);
	NConcurrency::TCWrapped<CPostgresBackendMessage> fg_PostgresReadBackendMessage(NContainer::CIOByteVector _Message);
	NConcurrency::TCWrapped<CPostgresAuthenticationRequest> fg_PostgresDecodeAuthenticationRequest(CPostgresBackendMessage _Message);
	NConcurrency::TCWrapped<CPostgresParameterStatus> fg_PostgresDecodeParameterStatus(CPostgresBackendMessage _Message);
	NConcurrency::TCWrapped<CPostgresBackendKeyData> fg_PostgresDecodeBackendKeyData(CPostgresBackendMessage _Message);
	NConcurrency::TCWrapped<EPostgresReadyForQueryStatus> fg_PostgresDecodeReadyForQuery(CPostgresBackendMessage _Message);
	NConcurrency::TCWrapped<void> fg_PostgresDecodeParseComplete(CPostgresBackendMessage _Message);
	NConcurrency::TCWrapped<void> fg_PostgresDecodeBindComplete(CPostgresBackendMessage _Message);
	NConcurrency::TCWrapped<void> fg_PostgresDecodeCloseComplete(CPostgresBackendMessage _Message);
	NConcurrency::TCWrapped<void> fg_PostgresDecodeNoData(CPostgresBackendMessage _Message);
	NConcurrency::TCWrapped<CPostgresCommandComplete> fg_PostgresDecodeCommandComplete(CPostgresBackendMessage _Message);
	NConcurrency::TCWrapped<CPostgresErrorResponse> fg_PostgresDecodeErrorResponse(CPostgresBackendMessage _Message);
	NConcurrency::TCWrapped<CPostgresRowDescription> fg_PostgresDecodeRowDescription(CPostgresBackendMessage _Message);
	NConcurrency::TCWrapped<CPostgresDataRow> fg_PostgresDecodeDataRow(CPostgresBackendMessage _Message, CPostgresRowDescription _Description);
}
