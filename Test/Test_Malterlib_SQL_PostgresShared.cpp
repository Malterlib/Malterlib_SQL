// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#include "Test_Malterlib_SQL_PostgresShared.h"

#include <Mib/Concurrency/AsyncDestroy>
#include <Mib/Cryptography/Certificate>
#include <Mib/File/File>
#include <Mib/Process/Platform>
#include <Mib/Process/ProcessLaunchActor>
#include <Mib/Test/Test>

namespace NMib::NSQL::NTest::NPostgres
{
	NContainer::TCVector<NStr::CStr> fg_PostgresTestParams(std::initializer_list<NStr::CStr> _Params)
	{
		NContainer::TCVector<NStr::CStr> Params;

		for (NStr::CStr const &Param : _Params)
			Params.f_InsertLast(Param);

		return Params;
	}

	namespace
	{
		NConcurrency::TCFuture<NProcess::CProcessLaunchActor::CSimpleLaunchResult> fg_RunTool
			(
				NStr::CStr _Executable
				, NContainer::TCVector<NStr::CStr> _Params
				, NStr::CStr _WorkingDirectory = NStr::CStr()
			)
		{
			co_return co_await NProcess::CProcessLaunchActor::fs_LaunchSimple
				(
					NProcess::CProcessLaunchActor::CSimpleLaunch
					(
						_Executable
						, _Params
						, _WorkingDirectory
						, NProcess::CProcessLaunchActor::ESimpleLaunchFlag_GenerateExceptionOnNonZeroExitCode
					)
				)
			;
		}
	}

	NConcurrency::TCFuture<void> fg_StopPostgresTestServer(NStr::CStr _PGControl, NStr::CStr _DataDir)
	{
		if (!NFile::CFile::fs_FileExists(_DataDir))
			co_return {};

		auto StopResult = co_await fg_RunTool(_PGControl, fg_PostgresTestParams({"-D", _DataDir, "-m", "fast", "stop"})).f_Wrap();
		(void)StopResult;

		co_return {};
	}

	void fg_GeneratePostgresTestCertificates(CPostgresTestCertificates &o_Certificates)
	{
		NCryptography::CPublicKeySetting KeySetting = NCryptography::CPublicKeySettings_EC_secp256r1{};

		NContainer::CSecureByteVector CAKey;
		NCryptography::CCertificateOptions CAOptions;
		CAOptions.m_CommonName = "Malterlib PostgreSQL test CA";
		CAOptions.m_KeySetting = KeySetting;
		CAOptions.f_MakeCA();

		NCryptography::CCertificateSignOptions CASignOptions;
		CASignOptions.m_Days = 1;
		NCryptography::CCertificate::fs_GenerateSelfSignedCertAndKey(CAOptions, o_Certificates.m_CACertificate, CAKey, CASignOptions);

		NContainer::CByteVector ServerRequestData;
		NCryptography::CCertificateOptions ServerOptions;
		ServerOptions.m_CommonName = "localhost";
		ServerOptions.m_Hostnames = NContainer::fg_CreateVector<NStr::CStr>("localhost");
		ServerOptions.m_KeySetting = KeySetting;
		ServerOptions.f_AddExtension_ExtendedKeyUsage(NCryptography::EExtendedKeyUsage_ServerAuth);
		NCryptography::CCertificate::fs_GenerateClientCertificateRequest(ServerOptions, ServerRequestData, o_Certificates.m_ServerKey);

		NCryptography::CCertificateSignOptions ServerSignOptions;
		ServerSignOptions.m_Days = 1;
		NCryptography::CCertificate::fs_SignClientCertificate(o_Certificates.m_CACertificate, CAKey, ServerRequestData, o_Certificates.m_ServerCertificate, ServerSignOptions);

		NContainer::CByteVector ClientRequestData;
		NCryptography::CCertificateOptions ClientOptions;
		ClientOptions.m_CommonName = "malterlib_user";
		ClientOptions.m_KeySetting = KeySetting;
		ClientOptions.f_AddExtension_ExtendedKeyUsage(NCryptography::EExtendedKeyUsage_ClientAuth);
		NCryptography::CCertificate::fs_GenerateClientCertificateRequest(ClientOptions, ClientRequestData, o_Certificates.m_ClientKey);

		NCryptography::CCertificateSignOptions ClientSignOptions;
		ClientSignOptions.m_Days = 1;
		NCryptography::CCertificate::fs_SignClientCertificate(o_Certificates.m_CACertificate, CAKey, ClientRequestData, o_Certificates.m_ClientCertificate, ClientSignOptions);
	}

	NConcurrency::TCFuture<void> fg_WithPostgresTestServer(CPostgresTestServerScenario _Scenario, FPostgresTestServerCallback _fCallback)
	{
		using namespace NMib::NStr;

		CStr InitDB = NProcess::NPlatform::fg_FindExecutable("initdb");
		CStr PGControl = NProcess::NPlatform::fg_FindExecutable("pg_ctl");

		if (!NFile::CFile::fs_FileExists(InitDB, NFile::EFileAttrib_File) || !NFile::CFile::fs_FileExists(PGControl, NFile::EFileAttrib_File))
		{
			if (fg_GetSys()->f_GetEnvironmentVariable("RunningCI", "") == "true")
				co_return DMibErrorInstance(_Scenario.m_MissingExecutableWarning);

			DMibConErrOut(_Scenario.m_MissingExecutableWarning);

			co_return {};
		}

		CStr TestRoot = NFile::CFile::fs_GetProgramDirectory() / _Scenario.m_DirectoryName;
		CStr DataDir = TestRoot / "data";
		CStr PasswordFile = TestRoot / "password.txt";
		CStr LogFile = TestRoot / "postgres.log";

		fg_TestAddCleanupPath(TestRoot);

		co_await fg_StopPostgresTestServer(PGControl, DataDir);

		if (NFile::CFile::fs_FileExists(TestRoot))
			NFile::CFile::fs_DeleteDirectoryRecursive(TestRoot, true);

		NFile::CFile::fs_CreateDirectory(TestRoot);

		auto CleanupFiles = co_await NConcurrency::fg_AsyncDestroy
			(
				[&]() -> NConcurrency::TCFuture<void>
				{
					if (NFile::CFile::fs_FileExists(TestRoot))
						NFile::CFile::fs_DeleteDirectoryRecursive(TestRoot, true);

					co_return {};
				}
			)
		;

		NFile::CFile::fs_WriteStringToFile(PasswordFile, "malterlib_password", false);
		co_await fg_RunTool(InitDB, fg_PostgresTestParams({"-D", DataDir, "-A", "scram-sha-256", "--username", "malterlib_user", "--pwfile", PasswordFile}));

		CPostgresTestCertificates Certificates;
		if (_Scenario.m_bTLS)
		{
			fg_GeneratePostgresTestCertificates(Certificates);

			NFile::CFile::fs_WriteFile(DataDir / "server.crt", Certificates.m_ServerCertificate);
			NFile::CFile::fs_WriteFileSecure(DataDir / "server.key", Certificates.m_ServerKey);
			NFile::CFile::fs_SetAttributes(DataDir / "server.key", NFile::EFileAttrib_UnixAttributesValid | NFile::EFileAttrib_UserRead | NFile::EFileAttrib_UserWrite);

			if (_Scenario.m_bClientCertificate)
				NFile::CFile::fs_WriteFile(DataDir / "root.crt", Certificates.m_CACertificate);
		}

		CStr Config;
		Config += "listen_addresses = '127.0.0.1'\n";
		Config += "port = {}\n"_f << _Scenario.m_Port;
		Config += "password_encryption = 'scram-sha-256'\n";
		Config += "timezone = 'UTC'\n";

#ifndef DPlatformFamily_Windows
		Config += "unix_socket_directories = '{}'\n"_f << TestRoot;
#endif

		if (_Scenario.m_bTLS)
		{
			Config += "ssl = on\n";
			Config += "ssl_cert_file = 'server.crt'\n";
			Config += "ssl_key_file = 'server.key'\n";

			if (_Scenario.m_bClientCertificate)
				Config += "ssl_ca_file = 'root.crt'\n";
		}
		else
			Config += "ssl = off\n";

		NFile::CFile::fs_WriteStringToFile(DataDir / "postgresql.conf", Config, false);

		CStr HBAConfig;
		if (_Scenario.m_bTLS)
		{
			HBAConfig = "hostssl all all 127.0.0.1/32 scram-sha-256";

			if (_Scenario.m_bClientCertificate)
				HBAConfig += " clientcert=verify-full";

			HBAConfig += "\n";
		}
		else
			HBAConfig = "host all all 127.0.0.1/32 scram-sha-256\n";

		NFile::CFile::fs_WriteStringToFile(DataDir / "pg_hba.conf", HBAConfig, false);

		co_await fg_RunTool(PGControl, fg_PostgresTestParams({"-D", DataDir, "-w", "-l", LogFile, "start"}));

		auto CleanupServer = co_await NConcurrency::fg_AsyncDestroy
			(
				[&]() -> NConcurrency::TCFuture<void>
				{
					CStr PGControlLocal = PGControl;
					CStr DataDirLocal = DataDir;
					CStr TestRootLocal = TestRoot;
					co_await fg_StopPostgresTestServer(fg_Move(PGControlLocal), fg_Move(DataDirLocal));
					if (NFile::CFile::fs_FileExists(TestRootLocal))
						NFile::CFile::fs_DeleteDirectoryRecursive(TestRootLocal, true);

					co_return {};
				}
			)
		;

		CleanupFiles.f_Clear();

		CPostgresConnectionSettings Settings;
		Settings.m_Host = _Scenario.m_bTLS ? "localhost" : "127.0.0.1";
		Settings.m_Port = _Scenario.m_Port;
		Settings.m_Database = "postgres";
		Settings.m_User = "malterlib_user";
		Settings.m_Password = "malterlib_password";
		Settings.m_bRequireTLS = _Scenario.m_bTLS;

		if (_Scenario.m_bTLS)
		{
			Settings.m_TLSSettings.m_CACertificateData = Certificates.m_CACertificate;

			if (_Scenario.m_bClientCertificate)
			{
				Settings.m_TLSSettings.m_PublicCertificateData = Certificates.m_ClientCertificate;
				Settings.m_TLSSettings.m_PrivateKeyData = Certificates.m_ClientKey;
			}
		}

		co_await _fCallback(Settings);

		co_return {};
	}
}
