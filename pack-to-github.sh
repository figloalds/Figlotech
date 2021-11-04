#!/bin/bash

rev=$(git rev-list --count master)

args="-o _nuget -p:PackageVersion=1.0.$rev.0;TargetFrameworks=netstandard2.0"
argsb='--api-key %GITHUB_NUGET_PAT% --source "github"'

dotnet pack Figlotech.Core $args
dotnet pack Figlotech.BDados $args
dotnet pack Figlotech.BDados.MySqlDataAccessor $args
dotnet pack Figlotech.BDados.PostgreSQLDataAccessor $args
dotnet pack Figlotech.BDados.SQLiteDataAccessor $args
dotnet pack Figlotech.ExcelUtil $args

dotnet nuget push "./_nuget/Figlotech.Core.1.$rev.0.nupkg" %argsb%
dotnet nuget push "./_nuget/Figlotech.BDados.1.$rev.0.nupkg" %argsb%
dotnet nuget push "./_nuget/Figlotech.BDados.MySqlDataAccessor.1.$rev.0.nupkg" %argsb%
dotnet nuget push "./_nuget/Figlotech.BDados.PostgreSQLDataAccessor.1.$rev.0.nupkg" %argsb%
dotnet nuget push "./_nuget/Figlotech.BDados.SQLiteDataAccessor.1.$rev.0.nupkg" %argsb%
dotnet nuget push "./_nuget/Figlotech.ExcelUtil.1.$rev.0.nupkg" %argsb%
