@echo off

del /f /q fitech.version
git rev-list --count master >> fitech.version
set /p revision= < fitech.version
set /p rev=<rev
set /A rev=rev+1
echo %rev% > rev

set args=-o .\_nuget -p:PackageVersion=1.0.%revision%.%rev%
set argsb=--api-key %GITHUB_NUGET_PAT% --source "fth-github-admin"

dotnet pack Figlotech.Core %args%
dotnet pack Figlotech.BDados %args%
dotnet pack Figlotech.BDados.MySqlDataAccessor %args%
dotnet pack Figlotech.BDados.PostgreSQLDataAccessor %args%
dotnet pack Figlotech.BDados.SQLiteDataAccessor %args%
dotnet pack Figlotech.Core.FileAcessAbstractions.AzureBlobsFileAccessor %args%
dotnet pack Figlotech.ExcelUtil %args%

dotnet nuget push "./_nuget/Figlotech.Core.1.0.%revision%.%rev%.nupkg" %argsb%
dotnet nuget push "./_nuget/Figlotech.BDados.1.0.%revision%.%rev%.nupkg" %argsb%
dotnet nuget push "./_nuget/Figlotech.BDados.MySqlDataAccessor.1.0.%revision%.%rev%.nupkg" %argsb%
dotnet nuget push "./_nuget/Figlotech.BDados.PostgreSQLDataAccessor.1.0.%revision%.%rev%.nupkg" %argsb%
dotnet nuget push "./_nuget/Figlotech.BDados.SQLiteDataAccessor.1.0.%revision%.%rev%.nupkg" %argsb%
dotnet nuget push "./_nuget/Figlotech.Core.FileAcessAbstractions.AzureBlobsFileAccessor.1.0.%revision%.%rev%.nupkg" %argsb%
dotnet nuget push "./_nuget/Figlotech.ExcelUtil.1.0.%revision%.%rev%.nupkg" %argsb%
