def projectName = "phq-kafka-python"

pipeline {
  agent {
    kubernetes {
      label "${projectName}"
      yamlFile 'ci/runner.yaml'
    }
  }
  stages {
    stage('Push to PHQ pypi') {
      when {
        branch 'master'
      }
      steps {
        container(name: 'python-ci', shell: '/bin/bash') {
          sh """#!/bin/bash
          python setup.py sdist
          python setup.py sdist upload -r https://pypi.int.phq.io/simple
          """
        }
      }
    }
  }
}