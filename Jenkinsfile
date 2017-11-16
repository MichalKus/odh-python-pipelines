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

                    env.version = new Date().format( 'yyyyMMdd-HHmmSS' )
                    dev_prefix = env.BRANCH_NAME == 'master' ? '' : 'dev-'
                    env.imageName = "${repository_name}/${dev_prefix}odh-python-pipelines:${env.BRANCH_NAME}.${env.version}"
                }

                sh "docker build . -t ${imageName}"

                sh "docker run --rm ${imageName} bash -c 'cd /odh/python && nosetests --exclude-dir=test/it --with-xunit --xunit-file=.xunit-reports/nosetests-ut.xml --with-coverage --cover-erase --cover-xml --cover-xml-file=.coverage-reports/coverage-ut.xml'"

//                sh "docker run --rm ${imageName} bash -c 'cd /odh/python && nosetests --nologcapture --exclude-dir=test/unit --with-xunit --xunit-file=.xunit-reports/nosetests-it.xml --with-coverage --cover-erase --cover-xml --cover-xml-file=.coverage-reports/coverage-it.xml'"

                sh "docker login -u ${repository_user} -p ${repository_password} ${repository_url}"

                sh "docker push ${imageName}"
            }
        }

    }
}
