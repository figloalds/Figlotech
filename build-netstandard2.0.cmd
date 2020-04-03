@echo off

set dist=netstandard2.0
set args=-o _dist\%dist% -f %dist% -c Release

dotnet publish Figlotech.Core %args%
dotnet publish Figlotech.BDados %args%
dotnet publish Figlotech.BDados.MySqlDataAccessor %args%
dotnet publish Figlotech.BDados.PostgreSQLDataAccessor %args%
dotnet publish Figlotech.BDados.SQLiteDataAccessor %args%
dotnet publish Figlotech.ExcelUtil %args%

if errorlevel 1 (
	echo BUILD PROCESS FAILED.
) else (
	echo BUILD PROCESS SUCCEDED!
)

pause >nul