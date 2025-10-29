pipeline {
  agent any
  options {
    disableConcurrentBuilds()
    timestamps()
    ansiColor('xterm')
  }

  environment {
    BASE_DIR        = '/home/ansible'
    IMAGE_REGISTRY  = 'ghcr.io' // default, overridden by env file if present
    GITHUB_CREDS_ID = '63482712-9185-4fca-b8ba-84649d66a380'

    // secret file credential path for env
    ENVFILE         = credentials('env_acumensite')

    // files to move from TEMP to the permanent directory
    DEPLOY_FILES    = 'docker-compose.prod.yml .env .env.local credentials.json'
  }

  stages {
    stage('Load env from credentials') {
      steps {
        script {
          // load all key value pairs from your env file into Jenkins environment
          def props = readProperties file: env.ENVFILE
          props.each { k, v -> env[k] = v }
        }
      }
    }

    stage('Guard only main') {
      when { expression { env.BRANCH_NAME && env.BRANCH_NAME != 'main' } }
      steps {
        echo "Skipping ${env.BRANCH_NAME} (only building main)."
        script { currentBuild.result = 'NOT_BUILT' }
      }
    }

    stage('Prep paths and repo vars') {
      steps {
        script {
          def repoUrl = sh(script: 'git config --get remote.origin.url', returnStdout: true).trim()
          env.REPO_URL = repoUrl

          def norm     = repoUrl.replace(':','/')
          def parts    = norm.tokenize('/')
          def repoPart = parts[-1]
          def owner    = parts[-2]
          def name     = repoPart.endsWith('.git') ? repoPart[0..-5] : repoPart

          // set owner and name if not already provided by env file
          if (!env.IMAGE_OWNER) env.IMAGE_OWNER = owner.toLowerCase()
          if (!env.IMAGE_NAME)  env.IMAGE_NAME  = name.toLowerCase()

          env.BASE_PROJECT_DIR = "${env.BASE_DIR}/${env.IMAGE_NAME}"
          env.TEMP_DIR         = "${env.BASE_PROJECT_DIR}/.tmp_${env.BUILD_NUMBER}"

          sh '''
            bash -lc '
              set -Eeuo pipefail
              mkdir -p "$BASE_PROJECT_DIR" "$TEMP_DIR"
            '
          '''
          echo "Registry=${env.IMAGE_REGISTRY} Owner=${env.IMAGE_OWNER} Repo=${env.IMAGE_NAME} Tag=${env.IMAGE_TAG ?: '(from .env)'}"
        }
      }
    }

    stage('Clone fresh into TEMP main') {
      steps {
        dir("${env.TEMP_DIR}") {
          checkout([
            $class: 'GitSCM',
            branches: [[name: "*/main"]],
            userRemoteConfigs: [[ url: env.REPO_URL, credentialsId: env.GITHUB_CREDS_ID ]]
          ])
        }
      }
    }

    stage('Place env into TEMP_DIR') {
      steps {
        sh '''
          set +x
          # write your credential env file into the compose context as .env
          install -m 600 "$ENVFILE" "$TEMP_DIR/.env"
          set -x
        '''
      }
    }

    stage('Docker login GHCR') {
      steps {
        withCredentials([usernamePassword(credentialsId: env.GITHUB_CREDS_ID,
                                          usernameVariable: 'GH_USER',
                                          passwordVariable: 'GH_PAT')]) {
          sh '''
            bash -lc '
              set -Eeuo pipefail
              : "${JENKINS_HOME:=/var/jenkins_home}"
              export DOCKER_CONFIG="${JENKINS_HOME}/.docker"
              mkdir -p "$DOCKER_CONFIG" && chmod 700 "$DOCKER_CONFIG"
              echo "$GH_PAT" | docker --config "$DOCKER_CONFIG" login ghcr.io -u "$GH_USER" --password-stdin
            '
          '''
        }
      }
    }

    stage('Build image using compose and env placeholders') {
      steps {
        sh '''
          bash -lc '
            set -Eeuo pipefail
            cd "$TEMP_DIR"

            if [[ ! -f docker-compose.prod.yml ]]; then
              echo "ERROR docker-compose.prod.yml not found" >&2
              exit 1
            fi

            docker compose -f docker-compose.prod.yml build
          '
        '''
      }
    }

    stage('Move deploy files to permanent dir') {
      steps {
        sh '''
          bash -lc '
            set -Eeuo pipefail
            cd "$TEMP_DIR"
            for f in ${DEPLOY_FILES}; do
              if [[ -f "$f" ]]; then
                mv -f "$f" "$BASE_PROJECT_DIR/"
              fi
            done
          '
        '''
      }
    }

    stage('Compose Down permanent dir') {
      steps {
        sh '''
          bash -lc '
            set -Eeuo pipefail
            cd "$BASE_PROJECT_DIR" || exit 0
            if [[ -f docker-compose.prod.yml ]]; then
              docker compose -f docker-compose.prod.yml down --remove-orphans || true
            else
              echo "WARN no docker-compose.prod.yml here skipping down"
            fi
          '
        '''
      }
    }

    stage('Compose Up permanent dir') {
      steps {
        sh '''
          bash -lc '
            set -Eeuo pipefail
            cd "$BASE_PROJECT_DIR"

            if [[ ! -f docker-compose.prod.yml ]]; then
              echo "WARN no docker-compose.prod.yml here skipping up"
              exit 0
            fi

            docker compose -f docker-compose.prod.yml up -d
          '
        '''
      }
    }

    stage('Push image using env placeholders') {
      steps {
        sh '''
          bash -lc '
            set -Eeuo pipefail
            : "${IMAGE_REGISTRY:?missing IMAGE_REGISTRY}"
            : "${IMAGE_OWNER:?missing IMAGE_OWNER}"
            : "${IMAGE_NAME:?missing IMAGE_NAME}"
            : "${IMAGE_TAG:?missing IMAGE_TAG}"

            REF="${IMAGE_REGISTRY}/${IMAGE_OWNER}/${IMAGE_NAME}:${IMAGE_TAG}"
            echo "Pushing $REF"
            docker push "$REF"
          '
        '''
      }
    }

    stage('Prune dangling') {
      steps {
        sh '''
          bash -lc '
            set -Eeuo pipefail
            docker image prune -f || true
          '
        '''
      }
    }
  }

  post {
    always {
      sh '''
        bash -lc '
          set -Eeuo pipefail
          rm -rf "$TEMP_DIR" || true
          find "$BASE_PROJECT_DIR" -maxdepth 1 -type d \\( -name ".tmp_*" -o -name ".tmp_*@tmp" \\) \
          ! -path "$TEMP_DIR" -exec rm -rf {} + || true
        '
      '''
    }
    success {
      script {
        def ref = "${env.IMAGE_REGISTRY}/${env.IMAGE_OWNER}/${env.IMAGE_NAME}:${env.IMAGE_TAG}"
        echo "Built deployed pushed ${ref}"
      }
    }
  }
}
