pipeline {
    agent any

    stages {
        stage('Build docker image') {
            steps {
                script {
                    repository_name = 'lgi-docker.jfrog.io'
                    repository_user = 'tempdev'
                    repository_password = 'phaewooH8moob0phuy2E'
                    repository_url = "https://${repository_name}"

                    env.version = new Date().format( 'yyyyMMdd-HHmmss' )
                    dev_prefix = env.BRANCH_NAME == 'master' ? '' : 'dev-'
                    tag = env.BRANCH_NAME == 'master' ? env.version : "${env.BRANCH_NAME}.${env.version}"

                    env.imageName = "${repository_name}/${dev_prefix}odh-python-pipelines:${tag}"

                    escaped_workspace = WORKSPACE.replaceAll("\\\\", "/")
                }

                sh 'rm -rf xunit-reports coverage-reports'

                sh "docker build --network=host --build-arg https_proxy=${env.https_proxy} . -t ${imageName}"

                sh 'mkdir -p xunit-reports coverage-reports'

                sh "docker run --rm -v ${escaped_workspace}/xunit-reports:/odh/python/.xunit-reports -v ${escaped_workspace}/coverage-reports:/odh/python/.coverage-reports ${imageName} bash -c 'cd /odh/python && nosetests --exclude-dir=test/it --with-xunit --xunit-file=.xunit-reports/nosetests-ut.xml --with-coverage --cover-erase --cover-xml --cover-xml-file=.coverage-reports/coverage-ut.xml'"

//                sh "docker run --rm -v ${escaped_workspace}/xunit-reports:/odh/python/.xunit-reports -v ${escaped_workspace}/coverage-reports:/odh/python/.coverage-reports ${imageName} bash -c 'cd /odh/python && nosetests --nologcapture --exclude-dir=test/unit --with-xunit --xunit-file=.xunit-reports/nosetests-it.xml --with-coverage --cover-erase --cover-xml --cover-xml-file=.coverage-reports/coverage-it.xml'"

                sh "docker login -u ${repository_user} -p ${repository_password} ${repository_url}"

                sh "docker push ${imageName}"
            }
            post {
                always {
                    junit "xunit-reports/nosetests-*.xml"
                }
            }

        }

    }

}
