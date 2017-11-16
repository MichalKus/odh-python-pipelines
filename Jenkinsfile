pipeline {
    agent any

    stages {
        stage('Build') {
            steps {
                docker.build 'dev-odh-python-pipelines.img:0.0.1'
            }
        }

        stage('Test') {
            steps {
                echo 'Test'
            }
        }
    }
}