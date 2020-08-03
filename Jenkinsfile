pipeline {
  agent { docker { image 'python:3.8' } }
  stages {
      stage('test') {
      steps {
        sh 'python QuandlhiveDb.py'
      }
    }
  }
}