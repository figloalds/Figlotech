pipeline {
    agent any

    stages {
        stage('Prepare') {
            sh 'export REV=$(git rev-list --count $BRANCH_NAME)'
            sh 'export BUILD_ARGS="-o ./_nuget -p:PackageVersion=1.0.$REV.$BUILD_NUMBER"'
            
            withCredentials([string(
                credentialsId: 'github-pat', 
                variable: 'GITHUB_PAT')]) {
                sh 'export PUSH_ARGS="--api-key $GITHUB_PAT --source fth-github-admin'
            }
        }
        stage('Build') {
            steps {
                echo 'Building...'
                sh 'dotnet pack Figlotech.Core $BUILD_ARGS'
                sh 'dotnet pack Figlotech.BDados $BUILD_ARGS'
                sh 'dotnet pack Figlotech.BDados.MySqlDataAccessor $BUILD_ARGS'
                sh 'dotnet pack Figlotech.BDados.PostgreSQLDataAccessor $BUILD_ARGS'
                sh 'dotnet pack Figlotech.BDados.SQLiteDataAccessor $BUILD_ARGS'
                sh 'dotnet pack Figlotech.Core.FileAcessAbstractions.AzureBlobsFileAccessor $BUILD_ARGS'
                sh 'dotnet pack Figlotech.ExcelUtil $BUILD_ARGS'
            }
        }
        stage('Test') {
            steps {
                echo 'No tests yet'
            }
        }
        stage('Deploy') {
            echo 'Deploying...'
            sh 'dotnet nuget push "./_nuget/Figlotech.Core.1.0.$REV.$BUILD_NUMBER.nupkg" $PUSH_ARGS'
            sh 'dotnet nuget push "./_nuget/Figlotech.BDados.1.0.$REV.$BUILD_NUMBER.nupkg" $PUSH_ARGS'
            sh 'dotnet nuget push "./_nuget/Figlotech.BDados.MySqlDataAccessor.1.0.$REV.$BUILD_NUMBER.nupkg" $PUSH_ARGS'
            sh 'dotnet nuget push "./_nuget/Figlotech.BDados.PostgreSQLDataAccessor.1.0.$REV.$BUILD_NUMBER.nupkg" $PUSH_ARGS'
            sh 'dotnet nuget push "./_nuget/Figlotech.BDados.SQLiteDataAccessor.1.0.$REV.$BUILD_NUMBER.nupkg" $PUSH_ARGS'
            sh 'dotnet nuget push "./_nuget/Figlotech.Core.FileAcessAbstractions.AzureBlobsFileAccessor.1.0.$REV.$BUILD_NUMBER.nupkg" $PUSH_ARGS'
            sh 'dotnet nuget push "./_nuget/Figlotech.ExcelUtil.1.0.$REV.$BUILD_NUMBER.nupkg" $PUSH_ARGS'
        }
    }
}