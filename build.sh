REVISION=$(git rev-list --count HEAD)
ARGS=$(echo -o ..\_nuget -p:PackageVersion=1.0.$REVISION;TargetFrameworks=netstandard2.1)

dotnet pack Figlotech.Core $ARGS
dotnet pack Figlotech.BDados $ARGS
dotnet pack Figlotech.BDados.MySqlDataAccessor $ARGS
dotnet pack Figlotech.BDados.PostgreSQLDataAccessor $ARGS
dotnet pack Figlotech.BDados.SQLiteDataAccessor $ARGS
dotnet pack Figlotech.ExcelUtil $ARGS
