pipeline {
    agent none

    // Adds timestamps to console logs
    options {
        timestamps()
    }

    stages {
        stage('Run benchmarks in production') {
            // This stage runs all the available benchmarks on 
	    // both Fidis and Deneb.
	    //
	    // In the end it appends the results to an SQLite DB 
	    // and archives the raw data.
            //

            when {
                branch 'features/jenkins_pipeline'
            }

            parallel {
                stage('fidis') {
                    agent {
                        label 'fidis-benchmark'
                    }
                    steps {
                        sh  'scripts/run_benchmarks.sh'
                    }
                }

                stage('deneb') {
                    agent {
                        label 'deneb-benchmark'
                    }
                    steps {
                        sh  'scripts/run_benchmarks.sh'
                    }
                }
            }
        }
    }
    stage('Publish benchmark results') {
        // This stage is here to process the benchmark DB and publish 
        // the results in some form somewhere

        when {
            branch 'features/jenkins_pipeline'
        }

	agent {
	    label 'fidis-benchmark'
        }
          
        steps {
            echo 'Results ready.'
        }
    }
}