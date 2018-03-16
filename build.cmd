@echo off

dotnet publish -o ..\_dist -f netstandard2.0

if errorlevel 1 (
	echo BUILD PROCESS FAILED.
) else (
	echo BUILD PROCESS SUCCEDED!
)

pause >nul