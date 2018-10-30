@echo off

set dist=netstandard2.0

dotnet publish Figlotech.Core -o ..\_dist\%dist% -f %dist%
dotnet publish Figlotech.BDados -o ..\_dist\%dist% -f %dist%
dotnet publish Figlotech.BDados.MySqlDataAccessor -o ..\_dist\%dist% -f %dist%
dotnet publish Figlotech.BDados.PostgreSQLDataAccessor -o ..\_dist\%dist% -f %dist%
dotnet publish Figlotech.BDados.SQLiteDataAccessor -o ..\_dist\%dist% -f %dist%
dotnet publish Figlotech.ExcelUtil -o ..\_dist\%dist% -f %dist%

if errorlevel 1 (
	echo BUILD PROCESS FAILED.
) else (
	echo BUILD PROCESS SUCCEDED!
)

pause >nul