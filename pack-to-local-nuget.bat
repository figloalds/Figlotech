@echo off

del /f /q fitech.version
git rev-list --count master >> fitech.version
set /p revision= < fitech.version
set /p rev=<rev
set /A rev=rev+1
echo %rev% > rev

set args=-o ..\_nuget -p:PackageVersion=1.0.%revision%.%rev%;TargetFrameworks=netstandard2.0

dotnet pack Figlotech.Core %args%
dotnet pack Figlotech.Core.FileAcessAbstractions.AzureBlobsFileAccessor %args%
dotnet pack Figlotech.BDados %args%
dotnet pack Figlotech.BDados.MySqlDataAccessor %args%
dotnet pack Figlotech.BDados.PostgreSQLDataAccessor %args%
dotnet pack Figlotech.BDados.SQLiteDataAccessor %args%
dotnet pack Figlotech.ExcelUtil %args%


if errorlevel 1 (
	echo BUILD PROCESS FAILED.
) else (
	echo BUILD PROCESS SUCCEDED!
)

copy /d /v /n /y .\_nuget\* ..\NugetLocal