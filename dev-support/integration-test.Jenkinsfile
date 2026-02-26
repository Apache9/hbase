pipeline {
  agent none
  triggers {
    @daily
  }
  options {
    buildDiscarder(logRotator(numToKeepStr: '20'))
    timeout (time: 16, unit: 'HOURS')
    timestamps()
    skipDefaultCheckout()
    disableConcurrentBuilds()
  }
  environment {
    YETUS_RELEASE = '0.15.0'
    HADOOP2_VERSION = "2.10.2"
    HADOOP3_VERSIONS = "3.3.5,3.3.6,3.4.0,3.4.1,3.4.2,3.4.3"
  }
  parameters {
    booleanParam(name: 'DEBUG', defaultValue: false, description: 'Produce a lot more meta-information.')
  }
  stage ('thirdparty installs') {
    agent {
      node {
        label 'hbase'
      }
    }
    stage ('scm-checkout') {
      steps {
        dir('component') {
          checkout scm
        }
      }
    }
    parallel {
      stage ('hadoop 2 cache') {
        steps {
          // directory must be unique for each parallel stage, because jenkins runs them in the same workspace :(
          dir('downloads-hadoop-2') {
            sh '''#!/usr/bin/env bash
              echo "Make sure we have a directory for downloading dependencies: $(pwd)"
            '''
          }
          sh '''#!/usr/bin/env bash
            set -e
            echo "Ensure we have a copy of Hadoop ${HADOOP2_VERSION}"
            "${WORKSPACE}/component/dev-support/jenkins-scripts/cache-apache-project-artifact.sh" \
              --working-dir "${WORKSPACE}/downloads-hadoop-2" \
              --keys 'https://downloads.apache.org/hadoop/common/KEYS' \
              --verify-tar-gz \
              "${WORKSPACE}/hadoop-${HADOOP2_VERSION}-bin.tar.gz" \
              "hadoop/common/hadoop-${HADOOP2_VERSION}/hadoop-${HADOOP2_VERSION}.tar.gz"
            for stale in $(ls -1 "${WORKSPACE}"/hadoop-2*.tar.gz | grep -v ${HADOOP2_VERSION}); do
              echo "Delete stale hadoop 2 cache ${stale}"
              rm -rf $stale
            done
          '''
          stash name: 'hadoop-2', includes: "hadoop-${HADOOP2_VERSION}-bin.tar.gz"
        }
      } // hadoop 2 cache
      stage ('hadoop 3 cache') {
        steps {
          script {
            hadoop3_versions = env.HADOOP3_VERSIONS.split(",");
            env.HADOOP3_VERSIONS_REGEX = "[" + hadoop3_versions.join("|") + "]";
            for (hadoop3_version in hadoop3_versions) {
              env.HADOOP3_VERSION = hadoop3_version;
              echo "env.HADOOP3_VERSION" + env.hadoop3_version;
              stage ('Hadoop 3 cache inner stage') {
                // directory must be unique for each parallel stage, because jenkins runs them in the same workspace :(
                dir("downloads-hadoop-${HADOOP3_VERSION}") {
                  sh '''#!/usr/bin/env bash
                    echo "Make sure we have a directory for downloading dependencies: $(pwd)"
                  '''
                } //dir
                sh '''#!/usr/bin/env bash
                  set -e
                  echo "Ensure we have a copy of Hadoop ${HADOOP3_VERSION}"
                  "${WORKSPACE}/component/dev-support/jenkins-scripts/cache-apache-project-artifact.sh" \
                    --working-dir "${WORKSPACE}/downloads-hadoop-${HADOOP3_VERSION}" \
                    --keys 'https://downloads.apache.org/hadoop/common/KEYS' \
                    --verify-tar-gz \
                    "${WORKSPACE}/hadoop-${HADOOP3_VERSION}-bin.tar.gz" \
                    "hadoop/common/hadoop-${HADOOP3_VERSION}/hadoop-${HADOOP3_VERSION}.tar.gz"
                  for stale in $(ls -1 "${WORKSPACE}"/hadoop-3*.tar.gz | grep -v ${HADOOP3_VERSION}); do
                    echo "Delete stale hadoop 3 cache ${stale}"
                    rm -rf $stale
                  done
                '''
                stash name: "hadoop-${HADOOP3_VERSION}", includes: "hadoop-${HADOOP3_VERSION}-bin.tar.gz"
                script {
                  if (env.HADOOP3_VERSION == env.HADOOP3_DEFAULT_VERSION) {
                    // FIXME: we never unstash this, because we run the packaging tests with the version-specific stashes
                    stash(name: "hadoop-3", includes: "hadoop-${HADOOP3_VERSION}-bin.tar.gz")
                  } // if
                } // script
              } // stage ('Hadoop 3 cache inner stage')
            } // for
          } // script
        } // steps
      } // stage ('hadoop 3 cache')
    } // parallel
  } // stage ('thirdparty installs')
  // This is meant to mimic what a release manager will do to create RCs.
  // See http://hbase.apache.org/book.html#maven.release
  // TODO (HBASE-23870): replace this with invocation of the release tool
  stage ('packaging test') {
    agent {
      node {
        label 'hbase'
      }
    }
    environment {
      BASEDIR = "${env.WORKSPACE}/component"
    }
    steps {
      dir('component') {
        checkout scm
      }
      sh '''#!/bin/bash -e
        echo "Setting up directories"
        rm -rf "output-srctarball" && mkdir "output-srctarball"
        rm -rf "unpacked_src_tarball" && mkdir "unpacked_src_tarball"
        rm -rf ".m2-for-repo" && mkdir ".m2-for-repo"
        rm -rf ".m2-for-src" && mkdir ".m2-for-src"
      '''
      sh '''#!/usr/bin/env bash
        set -e
        rm -rf "output-srctarball/machine" && mkdir "output-srctarball/machine"
        "${BASEDIR}/dev-support/gather_machine_environment.sh" "output-srctarball/machine"
        echo "got the following saved stats in 'output-srctarball/machine'"
        ls -lh "output-srctarball/machine"
      '''
      sh '''#!/bin/bash -e
        echo "Checking the steps for an RM to make a source artifact, then a binary artifact."
        docker build -t hbase-integration-test -f "${BASEDIR}/dev-support/docker/Dockerfile" .
        docker run --rm -v "${WORKSPACE}":/hbase -v /etc/passwd:/etc/passwd:ro -v /etc/group:/etc/group:ro \
          -u `id -u`:`id -g` -e JAVA_HOME="/usr/lib/jvm/java-17" --workdir=/hbase hbase-integration-test \
          "component/dev-support/hbase_nightly_source-artifact.sh" \
          --intermediate-file-dir output-srctarball \
          --unpack-temp-dir unpacked_src_tarball \
          --maven-m2-initial .m2-for-repo \
          --maven-m2-src-build .m2-for-src \
          --clean-source-checkout \
          component
        if [ $? -eq 0 ]; then
          echo '(/) {color:green}+1 source release artifact{color}\n-- See build output for details.' >output-srctarball/commentfile
        else
          echo '(x) {color:red}-1 source release artifact{color}\n-- See build output for details.' >output-srctarball/commentfile
          exit 1
        fi
      '''
      echo "unpacking the hbase bin tarball into 'hbase-install' and the client tarball into 'hbase-client'"
      sh '''#!/bin/bash -e
        if [ 2 -ne $(ls -1 "${WORKSPACE}"/unpacked_src_tarball/hbase-assembly/target/hbase-*-bin.tar.gz | grep -v hadoop3 | wc -l) ]; then
          echo '(x) {color:red}-1 testing binary artifact{color}\n-- source tarball did not produce the expected binaries.' >>output-srctarball/commentfile
          exit 1
        fi
        if [[ "${BRANCH_NAME}" == *"branch-2"* ]]; then
          if [ 2 -eq $(ls -1 "${WORKSPACE}"/unpacked_src_tarball/hbase-assembly/target/hbase-*-hadoop3-*-bin.tar.gz | wc -l) ]; then
             echo '(x) {color:red}-1 testing binary artifact{color}\n-- source tarball did not produce the expected hadoop3 binaries.' >>output-srctarball/commentfile
          fi
        fi
      '''
      stash name: 'hbase-install', includes: "${env.WORKSPACE}/unpacked_src_tarball/hbase-assembly/target/hbase-*-bin.tar.gz"
    }
  }
  stage ('integration test') {
    agent none
    environment {
      BASEDIR = "${env.WORKSPACE}/component"
      BRANCH = "${env.BRANCH_NAME}"
    }
    parallel {
      stage('hadoop 2 integration test') {
        agent {
          node {
            label 'hbase'
          }
        }
        environment {
          OUTPUT_DIR = "output-integration-hadoop-${env.HADOOP2_VERSION}"
        }
        sh '''#!/bin/bash -e
          echo "Setting up directories"
          rm -rf "${OUTPUT_DIR}"
          echo "(x) {color:red}-1 client integration test{color}\n-- Something went wrong with this stage, [check relevant console output|${BUILD_URL}/console]." >${OUTPUT_DIR}/commentfile
          rm -rf "hbase-install"
          rm -rf "hbase-client"
          rm -rf "hbase-hadoop3-install"
          rm -rf "hbase-hadoop3-client"
          # remove old hadoop tarballs in workspace
          rm -rf hadoop-2*.tar.gz
        '''
        unstash 'hadoop-2'
        unstash 'hbase-install'
        sh '''#!/bin/bash -xe
          if [[ "${BRANCH_NAME}" == *"branch-2"* ]]; then
            echo "Attempting to run an instance on top of Hadoop 2."
            hadoop_artifact=$(ls -1 "${WORKSPACE}"/hadoop-2*.tar.gz | head -n 1)
            tar --strip-components=1 -xzf "${hadoop_artifact}" -C "hadoop-2"
            install_artifact=$(ls -1 "${WORKSPACE}"/unpacked_src_tarball/hbase-assembly/target/hbase-*-bin.tar.gz | grep -v client-bin | grep -v hadoop3)
            tar --strip-component=1 -xzf "${install_artifact}" -C "hbase-install"
            client_artifact=$(ls -1 "${WORKSPACE}"/unpacked_src_tarball/hbase-assembly/target/hbase-*-client-bin.tar.gz | grep -v hadoop3)
            tar --strip-component=1 -xzf "${client_artifact}" -C "hbase-client"
            docker build -t hbase-integration-test -f "${BASEDIR}/dev-support/docker/Dockerfile" .
            docker run --rm -v "${WORKSPACE}":/hbase -v /etc/passwd:/etc/passwd:ro -v /etc/group:/etc/group:ro \
              -u `id -u`:`id -g` -e JAVA_HOME="/usr/lib/jvm/java-8" --workdir=/hbase hbase-integration-test \
              component/dev-support/hbase_nightly_pseudo-distributed-test.sh \
              --single-process \
              --working-dir ${OUTPUT_DIR}/hadoop-2 \
              --hbase-client-install "hbase-client" \
              hbase-install \
              hadoop-2/bin/hadoop \
              hadoop-2/share/hadoop/yarn/timelineservice \
              hadoop-2/share/hadoop/yarn/test/hadoop-yarn-server-tests-*-tests.jar \
              hadoop-2/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-*-tests.jar \
              hadoop-2/bin/mapred \
              >${OUTPUT_DIR}/hadoop-2.log 2>&1
            if [ $? -ne 0 ]; then
              echo "(x) {color:red}-1 client integration test{color}\n--Failed when running client tests on top of Hadoop 2. [see log for details|${BUILD_URL}/artifact/${OUTPUT_DIR}/hadoop-2.log]. (note that this means we didn't run on Hadoop 3)" >${OUTPUT_DIR}/commentfile
              exit 2
            fi
            echo "(/) {color:green}+1 client integration test for HBase 2 {color}" >${OUTPUT_DIR}/commentfile
          else
            echo "Skipping to run against Hadoop 2 for branch ${BRANCH_NAME}"
          fi
        '''
      }
      stage('hadoop 3 matrix') {
        axes {
          axis {
            name 'HADOOP_VERSION'
            values getHadoopVersions(env.HADOOP3_VERSIONS)
          }
        }
        agent {
          node {
            label 'hbase'
          }
        }
        stage('hadoop 3 integration test') {
          environment {
            OUTPUT_DIR = "output-integration-hadoop-${env.HADOOP_VERSION}"
          }
          dir('component') {
            checkout scm
          }
          sh '''#!/bin/bash -e
            echo "Setting up directories"
            rm -rf "${OUTPUT_DIR}"
            echo "(x) {color:red}-1 client integration test{color}\n-- Something went wrong with this stage, [check relevant console output|${BUILD_URL}/console]." >${OUTPUT_DIR}/commentfile
            rm -rf "hbase-install"
            rm -rf "hbase-client"
            rm -rf "hbase-hadoop3-install"
            rm -rf "hbase-hadoop3-client"
            # remove old hadoop tarballs in workspace
            rm -rf hadoop-3*.tar.gz
          '''
          unstash "hadoop-" + ${HADOOP_VERSION}
          unstash 'hbase-install'
          sh '''#!/bin/bash -e
            echo "Attempting to use run an instance on top of Hadoop ${HADOOP_VERSION}."
            # Clean up any previous tested Hadoop3 files before unpacking the current one
            rm -rf hadoop-3/*
            # Create working dir
            rm -rf "${OUTPUT_DIR}/non-shaded" && mkdir "${OUTPUT_DIR}/non-shaded"
            rm -rf "${OUTPUT_DIR}/shaded" && mkdir "${OUTPUT_DIR}/shaded"
            artifact=$(ls -1 "${WORKSPACE}"/hadoop-${HADOOP3_VERSION}-bin.tar.gz | head -n 1)
            tar --strip-components=1 -xzf "${artifact}" -C "hadoop-3"
            # we need to patch some files otherwise minicluster will fail to start, see MAPREDUCE-7471
            ${BASEDIR}/dev-support/patch-hadoop3.sh hadoop-3
            hbase_install_dir="hbase-install"
            hbase_client_dir="hbase-client"
            if [ -d "hbase-hadoop3-install" ]; then
              echo "run hadoop3 client integration test against hbase hadoop3 binaries"
              hbase_install_dir="hbase-hadoop3-install"
              hbase_client_dir="hbase-hadoop3-client"
            fi
            docker build -t hbase-integration-test -f "${BASEDIR}/dev-support/docker/Dockerfile" .
            docker run --rm -v "${WORKSPACE}":/hbase -v /etc/passwd:/etc/passwd:ro -v /etc/group:/etc/group:ro \
              -u `id -u`:`id -g` -e JAVA_HOME="/usr/lib/jvm/java-17" \
              -e HADOOP_OPTS="--add-opens java.base/java.lang=ALL-UNNAMED" \
              --workdir=/hbase hbase-integration-test \
              component/dev-support/hbase_nightly_pseudo-distributed-test.sh \
              --single-process \
              --working-dir ${OUTPUT_DIR}/non-shaded \
              --hbase-client-install ${hbase_client_dir} \
              ${hbase_install_dir} \
              hadoop-3/bin/hadoop \
              hadoop-3/share/hadoop/yarn/timelineservice \
              hadoop-3/share/hadoop/yarn/test/hadoop-yarn-server-tests-*-tests.jar \
              hadoop-3/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-*-tests.jar \
              hadoop-3/bin/mapred \
              >${OUTPUT_DIR}/hadoop.log 2>&1
            if [ $? -ne 0 ]; then
              echo "(x) {color:red}-1 client integration test{color}\n--Failed when running client tests on top of Hadoop ${HADOOP_VERSION}. [see log for details|${BUILD_URL}/artifact/${OUTPUT_DIR}/hadoop.log]. (note that this means we didn't check the Hadoop ${HADOOP_VERSION} shaded client)" > ${OUTPUT_DIR}/commentfile
              exit 2
            fi
            echo "(/) {color:green}+1 client integration test for ${HADOOP_VERSION} {color}" >> ${OUTPUT_DIR}/commentfile
            echo "Attempting to run an instance on top of Hadoop ${HADOOP_VERSION}, relying on the Hadoop client artifacts for the example client program."
            docker run --rm -v "${WORKSPACE}":/hbase -v /etc/passwd:/etc/passwd:ro -v /etc/group:/etc/group:ro \
              -u `id -u`:`id -g` -e JAVA_HOME="/usr/lib/jvm/java-17" \
              -e HADOOP_OPTS="--add-opens java.base/java.lang=ALL-UNNAMED" \
              --workdir=/hbase hbase-integration-test \
              component/dev-support/hbase_nightly_pseudo-distributed-test.sh \
              --single-process \
              --hadoop-client-classpath hadoop-3/share/hadoop/client/hadoop-client-api-*.jar:hadoop-3/share/hadoop/client/hadoop-client-runtime-*.jar \
              --working-dir ${OUTPUT_DIR}/shade \
              --hbase-client-install ${hbase_client_dir} \
              ${hbase_install_dir} \
              hadoop-3/bin/hadoop \
              hadoop-3/share/hadoop/yarn/timelineservice \
              hadoop-3/share/hadoop/yarn/test/hadoop-yarn-server-tests-*-tests.jar \
              hadoop-3/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-*-tests.jar \
              hadoop-3/bin/mapred \
              >${OUTPUT_DIR}/hadoop-shaded.log 2>&1
            if [ $? -ne 0 ]; then
              echo "(x) {color:red}-1 client integration test{color}\n--Failed when running client tests on top of Hadoop ${HADOOP_VERSION} using Hadoop's shaded client. [see log for details|${BUILD_URL}/artifact/${OUTPUT_DIR}/hadoop-shaded.log]." >> ${OUTPUT_DIR}/commentfile
              exit 2
            fi
            echo "(/) {color:green}+1 client integration test for ${HADOOP_VERSION} with shaded hadoop client {color}" >> ${OUTPUT_DIR}/commentfile
          '''
        }
      }
    }
  } // stage integration test
  post {
    always {
      scripts {
        sshPublisher(publishers: [
          sshPublisherDesc(configName: 'Nightlies',
            transfers: [
              sshTransfer(remoteDirectory: "hbase/${JOB_NAME}/${BUILD_NUMBER}",
                sourceFiles: "output-srctarball/hbase-src.tar.gz"
              )
            ]
          )
        ])
        // remove the big src tarball, store the nightlies url in hbase-src.html
        sh '''#!/bin/bash -e
          SRC_TAR="${WORKSPACE}/output-srctarball/hbase-src.tar.gz"
          if [ -f "${SRC_TAR}" ]; then
            echo "Remove ${SRC_TAR} for saving space"
            rm -rf "${SRC_TAR}"
            python3 ${BASEDIR}/dev-support/gen_redirect_html.py "${ASF_NIGHTLIES_BASE}/output-srctarball" > "${WORKSPACE}/output-srctarball/hbase-src.html"
          else
            echo "No hbase-src.tar.gz, skipping"
          fi
        '''
        archiveArtifacts artifacts: 'output-srctarball/*'
        archiveArtifacts artifacts: 'output-srctarball/**/*'
        archiveArtifacts artifacts: 'output-integration-*/*'
        archiveArtifacts artifacts: 'output-integration-*/**/*'
        def results = []
        results.add('output-srctarball/commentfile')
        results.add("output-integration-hadoop-${env.HADOOP_VERSION}/commentfile")
        for (hadoop3_version in getHadoopVersions($env.HADOOP3_VERSIONS) {
          results.add("output-integration-hadoop-${hadoop3_version}/commentfile")
        }
        echo env.BRANCH_NAME
        echo env.BUILD_URL
        echo currentBuild.result
        echo currentBuild.durationString
        def comment = "Results for branch ${env.BRANCH_NAME}\n"
        comment += "\t[build ${currentBuild.displayName} on builds.a.o|${env.BUILD_URL}]: "
        if (currentBuild.result == null || currentBuild.result == "SUCCESS") {
          comment += "(/) *{color:green}+1 overall{color}*\n"
        } else {
          comment += "(x) *{color:red}-1 overall{color}*\n"
          // Ideally get the committer our of the change and @ mention them in the per-jira comment
        }
        comment += "----\ndetails (if available):\n\n"
        echo ""
        echo "[DEBUG] trying to aggregate step-wise results"
        comment += results.collect { fileExists(file: it) ? readFile(file: it) : "" }.join("\n\n")
        echo "[INFO] Comment:"
        echo comment
        echo ""
        echo "[DEBUG] checking to see if feature branch"
        def jiras = getJirasToComment(env.BRANCH_NAME, [])
        if (jiras.isEmpty()) {
          echo "[DEBUG] non-feature branch, checking change messages for jira keys."
          echo "[INFO] There are ${currentBuild.changeSets.size()} change sets."
          jiras = getJirasToCommentFromChangesets(currentBuild)
        }
        jiras.each { currentIssue ->
          jiraComment issueKey: currentIssue, body: comment
        }
      } // scripts
    } // always
  } // post
}

import org.jenkinsci.plugins.workflow.support.steps.build.RunWrapper
@NonCPS
List<String> getJirasToCommentFromChangesets(RunWrapper thisBuild) {
  def seenJiras = []
  thisBuild.changeSets.each { cs ->
    cs.getItems().each { change ->
      CharSequence msg = change.msg
      echo "change: ${change}"
      echo "     ${msg}"
      echo "     ${change.commitId}"
      echo "     ${change.author}"
      echo ""
      seenJiras = getJirasToComment(msg, seenJiras)
    }
  }
  return seenJiras
}
@NonCPS
List<String> getJirasToComment(CharSequence source, List<String> seen) {
  source.eachMatch("HBASE-[0-9]+") { currentIssue ->
    echo "[DEBUG] found jira key: ${currentIssue}"
    if (currentIssue in seen) {
      echo "[DEBUG] already commented on ${currentIssue}."
    } else {
      echo "[INFO] commenting on ${currentIssue}."
      seen << currentIssue
    }
  }
  return seen
}
@NonCPS
def getHadoopVersions(versionString) {
  return (versionString ?: "")
          .split(',')
          .collect { it.trim() }
          .findAll { it } as String[]
}
