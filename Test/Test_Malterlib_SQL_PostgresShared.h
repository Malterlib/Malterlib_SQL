// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#pragma once

#include <Mib/SQL/PostgresClient>
#include <Mib/Concurrency/ActorFunctor>

namespace NMib::NSQL::NTest::NPostgres
{
	constexpr fp64 gc_Timeout = 10.0;

	struct CPostgresTestCertificates
	{
		NContainer::CByteVector m_CACertificate;
		NContainer::CByteVector m_ServerCertificate;
		NContainer::CSecureByteVector m_ServerKey;
		NContainer::CByteVector m_ClientCertificate;
		NContainer::CSecureByteVector m_ClientKey;
	};

	struct CPostgresTestServerScenario
	{
		NStr::CStr m_DirectoryName;
		uint16 m_Port = 0;
		bool m_bTLS = false;
		bool m_bClientCertificate = false;
		NStr::CStr m_MissingExecutableWarning = "Warning: Failed to find postgres executables, disabling PostgreSQL tests\n";
	};

	using FPostgresTestServerCallback = NConcurrency::TCActorFunctor<NConcurrency::TCFuture<void> (CPostgresConnectionSettings)>;

	NContainer::TCVector<NStr::CStr> fg_PostgresTestParams(std::initializer_list<NStr::CStr> _Params);
	NConcurrency::TCFuture<void> fg_StopPostgresTestServer(NStr::CStr _PGControl, NStr::CStr _DataDir);
	void fg_GeneratePostgresTestCertificates(CPostgresTestCertificates &o_Certificates);
	NConcurrency::TCFuture<void> fg_WithPostgresTestServer(CPostgresTestServerScenario _Scenario, FPostgresTestServerCallback _fCallback);
}
