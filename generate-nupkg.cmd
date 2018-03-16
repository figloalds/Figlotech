@echo off

dotnet pack Figlotech.Core -o ..\_nupkg
dotnet pack Figlotech.BDados -o ..\_nupkg
dotnet pack Figlotech.BDados.MySqlDataAccessor -o ..\_nupkg
dotnet pack Figlotech.BDados.SQLiteDataAccessor -o ..\_nupkg
dotnet pack Figlotech.ExcelUtil -o ..\_nupkg

if errorlevel 1 (
	echo BUILD PROCESS FAILED.
) else (
	echo BUILD PROCESS SUCCEDED!
)

pause >nul