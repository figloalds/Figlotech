pipeline {
  agent any
  environment {
    DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1
    REV=sh(returnStdout: true, script: 'git rev-list --count $BRANCH_NAME').trim()
    BUILD_ARGS="-o ./_nuget -p:PackageVersion=1.0.$REV.$BUILD_NUMBER"
  }
  stages {
    stage('Prepare') {
      steps {
        echo 'Preparing...'
        // sh 'export REV=$(git rev-list --count $BRANCH_NAME)'
        // sh 'export BUILD_ARGS=""'
        // withCredentials(bindings: [string(
        //                   credentialsId: 'github-pat', 
        //                   variable: 'GITHUB_PAT')]) {
        // sh 'export PUSH_ARGS="--api-key $GITHUB_PAT --source fth-github"'
      }   
    }

    stage('Build') {
      steps {
        echo 'Building...'
        withDotNet(sdk: 'net6.0') {
          sh 'dotnet pack Figlotech.Core $BUILD_ARGS'
        }
        withDotNet(sdk: 'net6.0') { 
          sh 'dotnet pack Figlotech.BDados $BUILD_ARGS'
        }
        withDotNet(sdk: 'net6.0') { 
          sh 'dotnet pack Figlotech.BDados.MySqlDataAccessor $BUILD_ARGS'
        }
        withDotNet(sdk: 'net6.0') { 
          sh 'dotnet pack Figlotech.BDados.PostgreSQLDataAccessor $BUILD_ARGS'
        }
        withDotNet(sdk: 'net6.0') { 
          sh 'dotnet pack Figlotech.BDados.SQLiteDataAccessor $BUILD_ARGS'
        }
        withDotNet(sdk: 'net6.0') { 
          sh 'dotnet pack Figlotech.Core.FileAcessAbstractions.AzureBlobsFileAccessor $BUILD_ARGS'
        }
        withDotNet(sdk: 'net6.0') { 
          sh 'dotnet pack Figlotech.ExcelUtil $BUILD_ARGS'
        }
      }
    }

    stage('Test') {
      steps {
        echo 'No tests yet'
      }
    }

    stage('Deploy') {
      steps {
        echo 'Deploying...'
        withCredentials(bindings: [string(
          credentialsId: 'github-pat', 
          variable: 'GITHUB_PAT')]) {
          environment {
            PUSH_ARGS="--api-key $GITHUB_PAT --source fth-github"
          }
          withDotNet(sdk: 'net6.0') {
            sh 'dotnet nuget push "./_nuget/Figlotech.Core.1.0.$REV.$BUILD_NUMBER.nupkg" $PUSH_ARGS'
          }
          withDotNet(sdk: 'net6.0') { 
            sh 'dotnet nuget push "./_nuget/Figlotech.BDados.1.0.$REV.$BUILD_NUMBER.nupkg" $PUSH_ARGS'
          }
          withDotNet(sdk: 'net6.0') { 
            sh 'dotnet nuget push "./_nuget/Figlotech.BDados.MySqlDataAccessor.1.0.$REV.$BUILD_NUMBER.nupkg" $PUSH_ARGS'
          }
          withDotNet(sdk: 'net6.0') { 
            sh 'dotnet nuget push "./_nuget/Figlotech.BDados.PostgreSQLDataAccessor.1.0.$REV.$BUILD_NUMBER.nupkg" $PUSH_ARGS'
          }
          withDotNet(sdk: 'net6.0') { 
            sh 'dotnet nuget push "./_nuget/Figlotech.BDados.SQLiteDataAccessor.1.0.$REV.$BUILD_NUMBER.nupkg" $PUSH_ARGS'
          }
          withDotNet(sdk: 'net6.0') { 
            sh 'dotnet nuget push "./_nuget/Figlotech.Core.FileAcessAbstractions.AzureBlobsFileAccessor.1.0.$REV.$BUILD_NUMBER.nupkg" $PUSH_ARGS'
          }
          withDotNet(sdk: 'net6.0') { 
            sh 'dotnet nuget push "./_nuget/Figlotech.ExcelUtil.1.0.$REV.$BUILD_NUMBER.nupkg" $PUSH_ARGS'
          }
        }
      }
    }
  }
}